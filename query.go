package thunderdb

import (
	"fmt"
	"iter"
	"maps"
	"slices"
)

type queryNode interface {
	AddParent(parent queryNode)
	Find(ranges map[int]*BytesRange, cols map[int]bool, mainIndex int) (iter.Seq2[*Row, error], error)
	ColumnSpecs() []ColumnSpec
	ComputedColumnSpecs() []ComputedColumnSpec
	metadata() *Metadata
	propagateToParents(row *Row, child queryNode) error
}

type baseQueryNode struct {
	parents     []queryNode
	metadataObj Metadata
}

func (n *baseQueryNode) AddParent(parent queryNode) {
	n.parents = append(n.parents, parent)
}

func (n *baseQueryNode) ColumnSpecs() []ColumnSpec {
	return n.metadata().ColumnSpecs
}

func (n *baseQueryNode) ComputedColumnSpecs() []ComputedColumnSpec {
	return n.metadata().ComputedColumnSpecs
}

func (n *baseQueryNode) metadata() *Metadata {
	return &n.metadataObj
}

type headQueryNode struct {
	backing  *storage
	children []queryNode
	baseQueryNode
}

func initHeadQueryNode(result *headQueryNode, backing *storage, columns []ColumnSpec, computedColumns []ComputedColumnSpec, children []queryNode) {
	result.backing = backing
	result.baseQueryNode = baseQueryNode{
		metadataObj: Metadata{
			ColumnSpecs:         columns,
			ComputedColumnSpecs: computedColumns,
		},
	}
	for _, child := range children {
		child.AddParent(result)
	}
}

func (n *headQueryNode) Find(ranges map[int]*BytesRange, cols map[int]bool, mainIndex int) (iter.Seq2[*Row, error], error) {
	return n.backing.find(ranges, cols, mainIndex)
}

func (n *headQueryNode) propagateToParents(row *Row, child queryNode) error {
	values := make(map[int]any)
	for k, v := range row.Iter() {
		if v.err != nil {
			return v.err
		}
		values[k] = v.value
	}
	if err := n.backing.Insert(values); err != nil {
		if terr, ok := err.(*ThunderError); ok && terr.Code == ErrCodeUniqueConstraint {
			return nil
		}
		return err
	}
	for _, parent := range n.parents {
		if err := parent.propagateToParents(row, n); err != nil {
			return err
		}
	}
	return nil
}

type joinedQueryNode struct {
	conditions []JoinOn
	left       queryNode
	right      queryNode
	baseQueryNode
}

func initJoinedQueryNode(result *joinedQueryNode, left, right queryNode, conditions []JoinOn) {
	result.left = left
	result.right = right
	result.conditions = conditions
	result.baseQueryNode = baseQueryNode{
		metadataObj: Metadata{
			ColumnSpecs:         append(slices.Clone(left.ColumnSpecs()), right.ColumnSpecs()...),
			ComputedColumnSpecs: append(slices.Clone(left.ComputedColumnSpecs()), right.ComputedColumnSpecs()...),
		},
	}
	left.AddParent(result)
	right.AddParent(result)
}

func (n *joinedQueryNode) Find(ranges map[int]*BytesRange, cols map[int]bool, mainIndex int) (iter.Seq2[*Row, error], error) {
	leftRanges, rightRanges := n.splitRanges(ranges)
	leftCols := make(map[int]bool)
	rightCols := make(map[int]bool)
	for k := range cols {
		if k < len(n.left.ColumnSpecs()) {
			leftCols[k] = true
		} else if k >= len(n.left.ColumnSpecs()) && k < len(n.metadata().ColumnSpecs) {
			rightCols[k-len(n.left.ColumnSpecs())] = true
		} else {
			return nil, ErrFieldNotFound(fmt.Sprintf("column %d", k))
		}
	}
	if mainIndex < len(n.left.ColumnSpecs()) || (mainIndex >= len(n.metadata().ColumnSpecs) && mainIndex < len(n.metadata().ColumnSpecs)+len(n.left.ComputedColumnSpecs())) {
		if mainIndex >= len(n.metadata().ColumnSpecs) {
			mainIndex = mainIndex - len(n.metadata().ColumnSpecs) + len(n.left.ColumnSpecs())
		}
		leftSeq, err := n.left.Find(leftRanges, leftCols, mainIndex)
		if err != nil {
			return nil, err
		}
		rightMainIndex := n.right.metadata().bestIndex(rightRanges)
		return func(yield func(*Row, error) bool) {
			for leftRow, err := range leftSeq {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				rowRightRanges, err := n.ComputeRangeForRight(leftRow)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				mergedRightRanges := MergeRangesMap(rightRanges, rowRightRanges)
				rightSeq, err := n.right.Find(mergedRightRanges, rightCols, rightMainIndex)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				for rightRow, err := range rightSeq {
					if err != nil {
						if !yield(nil, err) {
							return
						}
						continue
					}
					joinedRow, err := n.joinRows(leftRow, rightRow)
					if !yield(joinedRow, err) {
						return
					}
				}
			}
		}, nil
	}
	if mainIndex >= len(n.metadata().ColumnSpecs) {
		mainIndex = mainIndex - len(n.metadata().ColumnSpecs) + len(n.right.ColumnSpecs()) - len(n.left.ComputedColumnSpecs())
	} else {
		mainIndex = mainIndex - len(n.left.ColumnSpecs())
	}
	rightSeq, err := n.right.Find(rightRanges, rightCols, mainIndex)
	if err != nil {
		return nil, err
	}
	leftMainIndex := n.left.metadata().bestIndex(leftRanges)
	return func(yield func(*Row, error) bool) {
		for rightRow, err := range rightSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			rowLeftRanges, err := n.ComputeRangeForLeft(rightRow)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			mergedLeftRanges := MergeRangesMap(leftRanges, rowLeftRanges)
			leftSeq, err := n.left.Find(mergedLeftRanges, leftCols, leftMainIndex)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			for leftRow, err := range leftSeq {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				joinedRow, err := n.joinRows(leftRow, rightRow)
				if !yield(joinedRow, err) {
					return
				}
			}
		}
	}, nil
}

func (n *joinedQueryNode) joinRows(leftRow, rightRow *Row) (*Row, error) {
	newRow := &Row{
		values: make(map[int][]byte),
		maUn:   leftRow.maUn,
	}
	maps.Copy(newRow.values, leftRow.values)
	for k, v := range rightRow.values {
		newRow.values[k+len(n.left.ColumnSpecs())] = v
	}
	return newRow, nil
}

func (n *joinedQueryNode) ComputeRangeForRight(row *Row) (map[int]*BytesRange, error) {
	result := make(map[int]*BytesRange)
	vals := make(map[int]any)
	for k, v := range row.Iter() {
		if v.err != nil {
			return nil, v.err
		}
		vals[k] = v.value
	}
	for _, cond := range n.conditions {
		leftVal, ok := vals[cond.leftField]
		if !ok {
			return nil, ErrFieldNotFound(fmt.Sprintf("column %d", cond.leftField))
		}
		key, err := ToKey(leftVal)
		if err != nil {
			return nil, err
		}
		var curRange *BytesRange
		switch cond.operator {
		case EQ:
			curRange = NewBytesRange(key, key, true, true, nil)
		case LT:
			curRange = NewBytesRange(nil, key, false, false, nil)
		case LTE:
			curRange = NewBytesRange(nil, key, false, true, nil)
		case GT:
			curRange = NewBytesRange(key, nil, false, false, nil)
		case GTE:
			curRange = NewBytesRange(key, nil, true, false, nil)
		case NEQ:
			curRange = NewBytesRange(nil, nil, false, false, [][]byte{key})
		default:
			return nil, ErrUnsupportedOperator(cond.operator)
		}
		if existingRange, ok := result[cond.rightField]; ok {
			result[cond.rightField] = existingRange.Merge(curRange)
		} else {
			result[cond.rightField] = curRange
		}
	}
	return result, nil
}

func (n *joinedQueryNode) ComputeRangeForLeft(row *Row) (map[int]*BytesRange, error) {
	result := make(map[int]*BytesRange)
	vals := make(map[int]any)
	for k, v := range row.Iter() {
		if v.err != nil {
			return nil, v.err
		}
		vals[k] = v.value
	}
	for _, cond := range n.conditions {
		rightVal, ok := vals[cond.rightField]
		if !ok {
			return nil, ErrFieldNotFound(fmt.Sprintf("column %d", cond.rightField))
		}
		key, err := ToKey(rightVal)
		if err != nil {
			return nil, err
		}
		var curRange *BytesRange
		switch cond.operator {
		case EQ:
			curRange = NewBytesRange(key, key, true, true, nil)
		case LT:
			curRange = NewBytesRange(nil, key, false, false, nil)
		case LTE:
			curRange = NewBytesRange(nil, key, false, true, nil)
		case GT:
			curRange = NewBytesRange(key, nil, false, false, nil)
		case GTE:
			curRange = NewBytesRange(key, nil, true, false, nil)
		case NEQ:
			curRange = NewBytesRange(nil, nil, false, false, [][]byte{key})
		default:
			return nil, ErrUnsupportedOperator(cond.operator)
		}
		if existingRange, ok := result[cond.leftField]; ok {
			result[cond.leftField] = existingRange.Merge(curRange)
		} else {
			result[cond.leftField] = curRange
		}
	}
	return result, nil
}

func (n *joinedQueryNode) splitRanges(ranges map[int]*BytesRange) (map[int]*BytesRange, map[int]*BytesRange) {
	leftRanges := make(map[int]*BytesRange)
	rightRanges := make(map[int]*BytesRange)
	for field, r := range ranges {
		if field < len(n.left.ColumnSpecs()) {
			leftRanges[field] = r
		} else if field < len(n.left.ColumnSpecs())+len(n.right.ColumnSpecs()) {
			rightRanges[field-len(n.left.ColumnSpecs())] = r
		} else if field < len(n.left.ColumnSpecs())+len(n.right.ColumnSpecs())+len(n.left.ComputedColumnSpecs()) {
			leftRanges[field-len(n.left.ColumnSpecs())-len(n.right.ColumnSpecs())] = r
		} else {
			rightRanges[field-len(n.left.ColumnSpecs())-len(n.right.ColumnSpecs())-len(n.left.ComputedColumnSpecs())] = r
		}
	}
	return leftRanges, rightRanges
}

func (n *joinedQueryNode) propagateToParents(row *Row, child queryNode) error {
	if child == n.left {
		rightRanges, err := n.ComputeRangeForRight(row)
		if err != nil {
			return err
		}
		bestIndexRight := n.right.metadata().bestIndex(rightRanges)
		cols := make(map[int]bool)
		for k := range n.right.ColumnSpecs() {
			cols[k] = true
		}
		rightSeq, err := n.right.Find(rightRanges, cols, bestIndexRight)
		if err != nil {
			return err
		}
		for rightRow, err := range rightSeq {
			if err != nil {
				return err
			}
			joinedRow, err := n.joinRows(row, rightRow)
			if err != nil {
				return err
			}
			for parent := range n.left.(*backedQueryNode).propagatedParents {
				if err := parent.propagateToParents(joinedRow, n); err != nil {
					return err
				}
			}
		}
	} else if child == n.right {
		leftRanges, err := n.ComputeRangeForLeft(row)
		if err != nil {
			return err
		}
		bestIndexLeft := n.left.metadata().bestIndex(leftRanges)
		cols := make(map[int]bool)
		for k := range n.left.ColumnSpecs() {
			cols[k] = true
		}
		leftSeq, err := n.left.Find(leftRanges, cols, bestIndexLeft)
		if err != nil {
			return err
		}
		for leftRow, err := range leftSeq {
			if err != nil {
				return err
			}
			joinedRow, err := n.joinRows(row, leftRow)
			if err != nil {
				return err
			}
			for parent := range n.left.(*backedQueryNode).propagatedParents {
				if err := parent.propagateToParents(joinedRow, n); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type projectedQueryNode struct {
	computedColumns []int
	columns         []int
	child           queryNode
	baseQueryNode
}

func initProjectedQueryNode(result *projectedQueryNode, child queryNode, columns []int, computedColumns []int) {
	result.child = child
	result.columns = columns
	result.computedColumns = computedColumns
	result.baseQueryNode = baseQueryNode{
		metadataObj: Metadata{
			ColumnSpecs:         make([]ColumnSpec, len(columns)),
			ComputedColumnSpecs: make([]ComputedColumnSpec, len(computedColumns)),
		},
	}
	child.AddParent(result)
}

func (n *projectedQueryNode) Find(ranges map[int]*BytesRange, cols map[int]bool, mainIndex int) (iter.Seq2[*Row, error], error) {
	childCols := make(map[int]bool)
	for k := range cols {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(fmt.Sprintf("computed column %d", k))
		}
		childCols[n.columns[k]] = true
	}
	childRanges := make(map[int]*BytesRange)
	for field, r := range ranges {
		if field < len(n.columns) {
			childRanges[n.columns[field]] = r
		} else if field < len(n.columns)+len(n.computedColumns) {
			childRanges[n.computedColumns[field-len(n.columns)]+len(n.child.ColumnSpecs())] = r
		} else {
			return nil, ErrFieldNotFound(fmt.Sprintf("column %d", field))
		}
	}
	var childIndex int = -1
	if mainIndex == -1 {
		// no index found, use full table scan
	} else if mainIndex < len(n.columns) {
		childIndex = n.columns[mainIndex]
	} else if mainIndex < len(n.columns)+len(n.computedColumns) {
		childIndex = n.computedColumns[mainIndex-len(n.columns)] + len(n.child.ColumnSpecs())
	} else {
		return nil, ErrFieldNotFound(fmt.Sprintf("column %d", mainIndex))
	}
	childSeq, err := n.child.Find(childRanges, childCols, childIndex)
	if err != nil {
		return nil, err
	}
	return func(yield func(*Row, error) bool) {
		for childRow, err := range childSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			newRow := &Row{
				values: make(map[int][]byte),
				maUn:   childRow.maUn,
			}
			for _, col := range n.columns {
				if val, ok := childRow.values[col]; ok {
					newRow.values[col] = val
				}
			}
			for _, col := range n.computedColumns {
				if val, ok := childRow.values[col+len(n.child.ColumnSpecs())]; ok {
					newRow.values[col+len(n.columns)] = val
				}
			}
			if !yield(newRow, nil) {
				return
			}
		}
	}, nil
}

func (n *projectedQueryNode) propagateToParents(row *Row, child queryNode) error {
	parentRow := &Row{
		values: make(map[int][]byte),
		maUn:   row.maUn,
	}
	for k, col := range n.columns {
		if val, ok := row.values[col]; ok {
			parentRow.values[k] = val
		} else {
			return ErrFieldNotFound(fmt.Sprintf("column %d", col))
		}
	}
	for _, parent := range n.parents {
		if err := parent.propagateToParents(parentRow, n); err != nil {
			return err
		}
	}
	return nil
}

type backedQueryNode struct {
	ranges            map[int]*BytesRange
	backing           *storage
	propagatedParents map[queryNode]bool
	baseQueryNode
	explored bool
}

func initBackedQueryNode(result *backedQueryNode, backing *storage, ranges map[int]*BytesRange) {
	result.ranges = ranges
	result.backing = backing
	result.propagatedParents = make(map[queryNode]bool)
	result.baseQueryNode = baseQueryNode{
		metadataObj: backing.metadata,
	}
}

func (n *backedQueryNode) Find(ranges map[int]*BytesRange, cols map[int]bool, mainIndex int) (iter.Seq2[*Row, error], error) {
	if !n.explored {
		return func(func(*Row, error) bool) {
		}, nil
	}
	mergedRanges := MergeRangesMap(n.ranges, ranges)
	return n.backing.find(mergedRanges, cols, mainIndex)
}

func (n *backedQueryNode) propagateToParents(*Row, queryNode) error {
	if n.explored {
		return nil
	}
	n.explored = true
	bestIndex := n.backing.metadata.bestIndex(n.ranges)
	cols := make(map[int]bool)
	for k := range n.ColumnSpecs() {
		cols[k] = true
	}
	rows, err := n.backing.find(n.ranges, cols, bestIndex)
	if err != nil {
		return err
	}
	for row, err := range rows {
		if err != nil {
			return err
		}
		for parent := range n.propagatedParents {
			if err := parent.propagateToParents(row, n); err != nil {
				return err
			}
		}
	}
	return nil
}
