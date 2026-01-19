package thunderdb

import (
	"fmt"
	"iter"
	"maps"
	// "github.com/davecgh/go-spew/spew"
)

type queryNode interface {
	AddParent(parent queryNode)
	Find(mainIndex uint64, indexRange *BytesRange, equals map[int]*Value, ranges map[int]*BytesRange, exclusion map[int][]*Value, cols map[int]bool) (iter.Seq2[*Row, error], error)
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

func (n *baseQueryNode) metadata() *Metadata {
	return &n.metadataObj
}

type headQueryNode struct {
	backing  *storage
	children []queryNode
	baseQueryNode
}

func initHeadQueryNode(result *headQueryNode, backing *storage, children []queryNode) {
	result.backing = backing
	result.baseQueryNode.metadataObj = Metadata{
		ColumnsCount: backing.metadata.ColumnsCount,
		Indexes:      backing.metadata.Indexes,
	}
	for _, child := range children {
		child.AddParent(result)
	}
}

func (n *headQueryNode) Find(
	mainIndex uint64,
	indexRange *BytesRange,
	equals map[int]*Value,
	ranges map[int]*BytesRange,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	return n.backing.find(mainIndex, indexRange, equals, ranges, exclusion, cols)
}

func (n *headQueryNode) propagateToParents(row *Row, child queryNode) error {
	/*
		fmt.Printf("DEBUG: headQueryNode.propagateToParents %p. Parents: %d. Row: %v\n", n, len(n.parents), spew.Sdump(row.values))
		for i, p := range n.parents {
			fmt.Printf("DEBUG: Head Parent %d: %T %p\n", i, p, p)
		}
	*/
	values := make(map[int]any)
	for k, val := range row.Iter() {
		v, err := val.GetValue()
		if err != nil {
			return err
		}
		values[k] = v
	}
	if err := n.backing.Insert(values); err != nil {
		if terr, ok := err.(*ThunderError); ok && terr.Code == ErrCodeUniqueConstraint {
			return nil
		}
		return err
	}

	// fmt.Printf("HeadNode %p stored: %v\n", n, values)

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

func initJoinedQueryNode(result *joinedQueryNode, left, right queryNode, conditions []JoinOn) error {
	result.left = left
	result.right = right
	result.conditions = conditions
	if err := initJoinedMetadata(&result.baseQueryNode.metadataObj, left.metadata(), right.metadata()); err != nil {
		return err
	}
	left.AddParent(result)
	right.AddParent(result)
	return nil
}

func (n *joinedQueryNode) Find(
	mainIndex uint64,
	indexRange *BytesRange,
	equals map[int]*Value,
	ranges map[int]*BytesRange,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	leftRanges, rightRanges := splitRanges(n.left.metadata(), n.right.metadata(), ranges)
	leftCols, rightCols := splitCols(n.left.metadata(), n.right.metadata(), cols)
	leftEquals, rightEquals := splitEquals(n.left.metadata(), n.right.metadata(), equals)
	leftExclusion, rightExclusion := splitExclusion(n.left.metadata(), n.right.metadata(), exclusion)
	if indexRange != nil && mainIndex < (uint64(1)<<n.left.metadata().ColumnsCount) {
		leftSeq, err := n.left.Find(mainIndex, indexRange, leftEquals, leftRanges, leftExclusion, leftCols)
		if err != nil {
			return nil, err
		}
		return func(yield func(*Row, error) bool) {
			for leftRow, err := range leftSeq {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				rowRightEquals, rowRightRanges, rowRightNotEquals, err := n.ComputeContraintsForRight(leftRow)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				mergedRightRanges := MergeRangesMap(rightRanges, rowRightRanges)
				mergedRightEquals, ok, err := mergeEquals(rightEquals, rowRightEquals)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				if !ok {
					continue
				}
				mergedRightExclusion := mergeNotEquals(leftExclusion, rowRightNotEquals)
				rightMainIndex, rightMainRanges, err := n.right.metadata().bestIndex(mergedRightEquals, mergedRightRanges)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				rightSeq, err := n.right.Find(rightMainIndex, rightMainRanges, mergedRightEquals, mergedRightRanges, mergedRightExclusion, rightCols)
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
	if indexRange != nil && mainIndex >= uint64(1)<<n.left.metadata().ColumnsCount {
		mainIndex = mainIndex >> uint64(n.left.metadata().ColumnsCount)
	}
	rightSeq, err := n.right.Find(mainIndex, indexRange, rightEquals, rightRanges, rightExclusion, rightCols)
	if err != nil {
		return nil, err
	}
	return func(yield func(*Row, error) bool) {
		for rightRow, err := range rightSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			rowLeftEquals, rowLeftRanges, rowLeftExclusions, err := n.ComputeContraintsForLeft(rightRow)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			mergedLeftEquals, ok, err := mergeEquals(leftEquals, rowLeftEquals)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !ok {
				continue
			}
			mergedLeftExclusions := mergeNotEquals(leftExclusion, rowLeftExclusions)
			mergedLeftRanges := MergeRangesMap(leftRanges, rowLeftRanges)
			leftMainIndex, leftMainRanges, err := n.left.metadata().bestIndex(mergedLeftEquals, mergedLeftRanges)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			leftSeq, err := n.left.Find(leftMainIndex, leftMainRanges, mergedLeftEquals, mergedLeftRanges, mergedLeftExclusions, leftCols)
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
		newRow.values[k+n.left.metadata().ColumnsCount] = v
	}
	return newRow, nil
}

func (n *joinedQueryNode) ComputeContraintsForRight(row *Row) (map[int]*Value, map[int]*BytesRange, map[int][]*Value, error) {
	resultRanges := make(map[int]*BytesRange)
	resultEquals := make(map[int]*Value)
	resultExclusion := make(map[int][]*Value)
	vals := make(map[int]any)
	for k, val := range row.Iter() {
		v, err := val.GetValue()
		if err != nil {
			return nil, nil, nil, err
		}
		vals[k] = v
	}
	for _, cond := range n.conditions {
		leftVal, ok := vals[cond.leftField]
		if !ok {
			return nil, nil, nil, ErrFieldNotFound(fmt.Sprintf("column %d", cond.leftField))
		}
		key, err := ToKey(leftVal)
		if err != nil {
			return nil, nil, nil, err
		}
		var curRange *BytesRange
		var curValue *Value
		switch cond.operator {
		case EQ:
			curValue = ValueOfRaw(key, &orderedMaUn)
		case LT:
			curRange = NewBytesRange(nil, key, false, false)
		case LTE:
			curRange = NewBytesRange(nil, key, false, true)
		case GT:
			curRange = NewBytesRange(key, nil, false, false)
		case GTE:
			curRange = NewBytesRange(key, nil, true, false)
		case NEQ:
			curValue = ValueOfRaw(key, &orderedMaUn)
		default:
			return nil, nil, nil, ErrUnsupportedOperator(cond.operator)
		}
		if curRange != nil {
			if existingRange, ok := resultRanges[cond.rightField]; ok {
				resultRanges[cond.rightField] = existingRange.Merge(curRange)
			} else {
				resultRanges[cond.rightField] = curRange
			}
		}
		if curValue != nil {
			if cond.operator == EQ {
				resultEquals[cond.rightField] = curValue
			} else if cond.operator == NEQ {
				resultExclusion[cond.rightField] = append(resultExclusion[cond.rightField], curValue)
			}
		}
	}
	return resultEquals, resultRanges, resultExclusion, nil
}

func (n *joinedQueryNode) ComputeContraintsForLeft(row *Row) (map[int]*Value, map[int]*BytesRange, map[int][]*Value, error) {
	resultRanges := make(map[int]*BytesRange)
	resultEquals := make(map[int]*Value)
	resultExclusion := make(map[int][]*Value)
	vals := make(map[int]any)
	for k, val := range row.Iter() {
		v, err := val.GetValue()
		if err != nil {
			return nil, nil, nil, err
		}
		vals[k] = v
	}
	for _, cond := range n.conditions {
		rightVal, ok := vals[cond.rightField+n.left.metadata().ColumnsCount]
		if !ok {
			return nil, nil, nil, ErrFieldNotFound(fmt.Sprintf("column %d", cond.rightField))
		}
		key, err := ToKey(rightVal)
		if err != nil {
			return nil, nil, nil, err
		}
		var curRange *BytesRange
		var curValue *Value
		switch cond.operator {
		case EQ:
			curValue = ValueOfRaw(key, &orderedMaUn)
		case LT:
			curRange = NewBytesRange(nil, key, false, false)
		case LTE:
			curRange = NewBytesRange(nil, key, false, true)
		case GT:
			curRange = NewBytesRange(key, nil, false, false)
		case GTE:
			curRange = NewBytesRange(key, nil, true, false)
		case NEQ:
			curValue = ValueOfRaw(key, &orderedMaUn)
		default:
			return nil, nil, nil, ErrUnsupportedOperator(cond.operator)
		}
		if curRange != nil {
			if existingRange, ok := resultRanges[cond.leftField]; ok {
				resultRanges[cond.leftField] = existingRange.Merge(curRange)
			} else {
				resultRanges[cond.leftField] = curRange
			}
		}
		if curValue != nil {
			if cond.operator == EQ {
				resultEquals[cond.leftField] = curValue
			} else if cond.operator == NEQ {
				resultExclusion[cond.leftField] = append(resultExclusion[cond.leftField], curValue)
			}
		}
	}
	return resultEquals, resultRanges, resultExclusion, nil
}

func (n *joinedQueryNode) propagateToParents(row *Row, child queryNode) error {
	// fmt.Printf("DEBUG: JoinedNode %p propagate. Child: %p (Left: %p, Right: %p)\n", n, child, n.left, n.right)
	switch child {
	case n.left:
		rightEquals, rightRanges, rightExclusions, err := n.ComputeContraintsForRight(row)
		if err != nil {
			return err
		}
		// fmt.Printf("DEBUG: Join Left->Right. Row: %v. Ranges: %v\n", spew.Sdump(row.values), rightRanges)
		bestIndexRight, bestRangesRight, err := n.right.metadata().bestIndex(rightEquals, rightRanges)
		if err != nil {
			return err
		}
		cols := make(map[int]bool)
		for k := range n.right.metadata().ColumnsCount {
			cols[k] = true
		}
		rightSeq, err := n.right.Find(bestIndexRight, bestRangesRight, rightEquals, rightRanges, rightExclusions, cols)
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
			// Propagate to JOIN's parents, not left's parents
			for _, parent := range n.parents {
				if err := parent.propagateToParents(joinedRow, n); err != nil {
					return err
				}
			}
		}
	case n.right:
		leftEquals, leftRanges, leftExclusions, err := n.ComputeContraintsForLeft(row)
		if err != nil {
			return err
		}
		// fmt.Printf("DEBUG: Join Right->Left. Row: %v. Ranges: %v\n", spew.Sdump(row.values), leftRanges)
		bestIndexLeft, bestRangesLeft, err := n.left.metadata().bestIndex(leftEquals, leftRanges)
		cols := make(map[int]bool)
		for k := range n.left.metadata().ColumnsCount {
			cols[k] = true
		}
		leftSeq, err := n.left.Find(bestIndexLeft, bestRangesLeft, leftEquals, leftRanges, leftExclusions, cols)
		if err != nil {
			return err
		}
		count := 0
		for leftRow, err := range leftSeq {
			count++
			if err != nil {
				return err
			}
			// fmt.Printf("DEBUG: Join Match Found! LeftRow: %v\n", spew.Sdump(leftRow.values))
			joinedRow, err := n.joinRows(leftRow, row)
			if err != nil {
				return err
			}
			// Propagate to JOIN's parents, not left's parents
			for _, parent := range n.parents {
				if err := parent.propagateToParents(joinedRow, n); err != nil {
					return err
				}
			}
		}
		// fmt.Printf("DEBUG: Join Right->Left Found %d matches\n", count)
	default:
		panic("unknown child in joinedQueryNode.propagateToParents")
	}
	return nil
}

type projectedQueryNode struct {
	columns []int
	child   queryNode
	baseQueryNode
}

func initProjectedQueryNode(result *projectedQueryNode, child queryNode, columns []int) {
	result.child = child
	result.columns = columns
	childToResultColumnMap := make(map[int][]int)
	for i, col := range columns {
		childToResultColumnMap[col] = append(childToResultColumnMap[col], i)
	}
	indexes := make(map[uint64]bool)
	for idx, isUnique := range child.metadata().Indexes {
		var projectedIdx uint64 = 0
		refCols := ReferenceColumns(idx)
		for _, col := range refCols {
			childCols := childToResultColumnMap[col]
			for _, childCol := range childCols {
				projectedIdx |= 1 << uint64(childCol)
			}
		}
		indexes[projectedIdx] = isUnique
	}
	result.baseQueryNode.metadataObj = Metadata{
		ColumnsCount: len(columns),
		Indexes:      indexes,
	}
	child.AddParent(result)
}

func (n *projectedQueryNode) Find(
	mainIndex uint64,
	indexRange *BytesRange,
	equals map[int]*Value,
	ranges map[int]*BytesRange,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	childCols := make(map[int]bool)
	for k := range cols {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(fmt.Sprintf("computed column %d", k))
		}
		childCols[n.columns[k]] = true
	}
	childRanges := make(map[int]*BytesRange)
	for field, r := range ranges {
		if field < n.metadataObj.ColumnsCount {
			childRanges[n.columns[field]] = r
		} else {
			return nil, ErrFieldNotFound(fmt.Sprintf("column %d", field))
		}
	}
	childIndex := uint64(0)
	refCols := ReferenceColumns(mainIndex)
	for _, col := range refCols {
		if col >= len(n.columns) {
			return nil, ErrFieldNotFound(fmt.Sprintf("computed column %d", col))
		}
		childIndex |= 1 << uint64(n.columns[col])
	}
	childEquals := make(map[int]*Value)
	for k, v := range equals {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(fmt.Sprintf("computed column %d", k))
		}
		childEquals[n.columns[k]] = v
	}
	childExclusion := make(map[int][]*Value)
	for k, vals := range exclusion {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(fmt.Sprintf("computed column %d", k))
		}
		childVals := make([]*Value, len(vals))
		for i, val := range vals {
			childVals[i] = val
		}
		childExclusion[n.columns[k]] = childVals
	}
	childSeq, err := n.child.Find(childIndex, indexRange, childEquals, childRanges, childExclusion, childCols)
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
			for col := range cols {
				if col >= len(n.columns) {
					if !yield(nil, ErrFieldNotFound(fmt.Sprintf("column %d", col))) {
						return
					}
					continue
				}
				newRow.values[col] = childRow.values[n.columns[col]]
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
	ranges    map[int]*BytesRange
	equals    map[int]*Value
	exclusion map[int][]*Value
	backing   *storage
	baseQueryNode
	explored bool
}

func initBackedQueryNode(result *backedQueryNode, backing *storage, equals map[int]*Value, ranges map[int]*BytesRange, exclusion map[int][]*Value) {
	result.equals = equals
	result.exclusion = exclusion
	result.ranges = ranges
	result.backing = backing
	result.baseQueryNode.metadataObj = backing.metadata
}

func (n *backedQueryNode) Find(
	mainIndex uint64,
	indexRange *BytesRange,
	equals map[int]*Value,
	ranges map[int]*BytesRange,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	if !n.explored {
		return func(func(*Row, error) bool) {
		}, nil
	}
	return n.backing.find(mainIndex, indexRange, equals, ranges, exclusion, cols)
}

func (n *backedQueryNode) propagateToParents(*Row, queryNode) error {
	// fmt.Printf("DEBUG: backedQueryNode.propagateToParents %p explored=%v\n", n, n.explored)
	if n.explored {
		return nil
	}
	n.explored = true
	bestIndex, bestRanges, err := n.backing.metadata.bestIndex(n.equals, n.ranges)
	if err != nil {
		return err
	}
	cols := make(map[int]bool)
	for k := range n.metadata().ColumnsCount {
		cols[k] = true
	}
	rows, err := n.backing.find(bestIndex, bestRanges, n.equals, n.ranges, n.exclusion, cols)
	if err != nil {
		return err
	}
	for row, err := range rows {
		if err != nil {
			return err
		}
		for _, parent := range n.parents {
			if err := parent.propagateToParents(row, n); err != nil {
				return err
			}
		}
	}
	return nil
}
