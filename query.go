package thunderdb

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	// "github.com/davecgh/go-spew/spew"
)

type queryNode interface {
	AddParent(parent queryNode)
	Find(mainIndex uint64, indexRange *Range, equals map[int]*Value, ranges map[int]*Range, exclusion map[int][]*Value, cols map[int]bool) (iter.Seq2[*Row, error], error)
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
	indexRange *Range,
	equals map[int]*Value,
	ranges map[int]*Range,
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
		if errors.Is(err, ErrCodeUniqueConstraint) {
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
	indexRange *Range,
	equals map[int]*Value,
	ranges map[int]*Range,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	// println("DEBUG: Join Find. MainIndex:", mainIndex, "IndexRange:", indexRange, "Equals:", equals, "Ranges:", ranges, "Exclusion:", exclusion, "Cols:", cols)
	leftRanges, rightRanges, err := splitRanges(n.left.metadata(), n.right.metadata(), ranges)
	if err != nil {
		return nil, err
	}
	leftCols, rightCols, err := splitCols(n.left.metadata(), n.right.metadata(), cols)
	if err != nil {
		return nil, err
	}
	leftEquals, rightEquals, err := splitEquals(n.left.metadata(), n.right.metadata(), equals)
	if err != nil {
		return nil, err
	}
	leftExclusion, rightExclusion, err := splitExclusion(n.left.metadata(), n.right.metadata(), exclusion)
	if err != nil {
		return nil, err
	}
	if indexRange != nil && mainIndex < (uint64(1)<<n.left.metadata().ColumnsCount) {
		leftSeq, err := n.left.Find(mainIndex, indexRange, leftEquals, leftRanges, leftExclusion, leftCols)
		if err != nil {
			return nil, err
		}
		return func(yield func(*Row, error) bool) {
			mergedRightEquals := make(map[int]*Value)
			mergedRightExclusion := make(map[int][]*Value)
			mergedRightRanges := make(map[int]*Range)

			// Scratch buffers for constraint computation
			rowRightEquals := make(map[int]*Value)
			rowRightRanges := make(map[int]*Range)
			rowRightExclusion := make(map[int][]*Value)
			equalsBytes := make(map[int][]byte)
			exclusionBytes := make(map[int]map[string]bool)
			rowBytes := make(map[int][]byte)

			for leftRow, err := range leftSeq {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				possible, err := n.ComputeContraintsForRight(
					leftRow,
					rowRightEquals,
					rowRightRanges,
					rowRightExclusion,
					equalsBytes,
					exclusionBytes,
					rowBytes,
				)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				if !possible {
					// Constraints conflicted, skip this left row
					continue
				}

				ok, err := mergeEquals(&mergedRightEquals, rightEquals, rowRightEquals)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				if !ok {
					// Constraints conflicted (e.g. x=1 AND x=2), skip this combination
					continue
				}

				if err := MergeRangesMap(&mergedRightRanges, rightRanges, rowRightRanges); err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}

				// mergedRightEquals is already done above

				mergeNotEquals(&mergedRightExclusion, leftExclusion, rowRightExclusion)
				rightMainIndex, rightMainRanges, err := n.right.metadata().bestIndex(mergedRightEquals, mergedRightRanges)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				// println("DEBUG: Join Find Right. MainIndex:", rightMainIndex, "MainRanges:", rightMainRanges)
				rightSeq, err := n.right.Find(rightMainIndex, rightMainRanges, mergedRightEquals, mergedRightRanges, mergedRightExclusion, rightCols)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}

				// Reusable row for joining
				joinedRow := &Row{
					values: make(map[int][]byte),
				}

				for rightRow, err := range rightSeq {
					if err != nil {
						if !yield(nil, err) {
							return
						}
						continue
					}
					n.joinInto(joinedRow, leftRow, rightRow)
					if !yield(joinedRow, nil) {
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
		mergedLeftEquals := make(map[int]*Value)
		mergedLeftExclusion := make(map[int][]*Value)
		mergedLeftRanges := make(map[int]*Range)

		// Scratch buffers for constraint computation
		rowLeftEquals := make(map[int]*Value)
		rowLeftRanges := make(map[int]*Range)
		rowLeftExclusions := make(map[int][]*Value)
		equalsBytes := make(map[int][]byte)
		exclusionBytes := make(map[int]map[string]bool)
		rowBytes := make(map[int][]byte)

		// Reusable row for joining
		joinedRow := &Row{
			values: make(map[int][]byte),
		}

		for rightRow, err := range rightSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			possible, err := n.ComputeContraintsForLeft(
				rightRow,
				rowLeftEquals,
				rowLeftRanges,
				rowLeftExclusions,
				equalsBytes,
				exclusionBytes,
				rowBytes,
			)

			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !possible {
				// Constraints conflicted, skip this right row
				continue
			}
			ok, err := mergeEquals(&mergedLeftEquals, leftEquals, rowLeftEquals)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !ok {
				continue
			}
			mergeNotEquals(&mergedLeftExclusion, leftExclusion, rowLeftExclusions)

			if err := MergeRangesMap(&mergedLeftRanges, leftRanges, rowLeftRanges); err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			leftMainIndex, leftMainRanges, err := n.left.metadata().bestIndex(mergedLeftEquals, mergedLeftRanges)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			leftSeq, err := n.left.Find(leftMainIndex, leftMainRanges, mergedLeftEquals, mergedLeftRanges, mergedLeftExclusion, leftCols)
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
				n.joinInto(joinedRow, leftRow, rightRow)
				if !yield(joinedRow, nil) {
					return
				}
			}
		}
	}, nil
}

func (n *joinedQueryNode) joinInto(dest *Row, leftRow, rightRow *Row) {
	clear(dest.values)
	dest.maUn = leftRow.maUn
	maps.Copy(dest.values, leftRow.values)
	offset := n.left.metadata().ColumnsCount
	for k, v := range rightRow.values {
		dest.values[k+offset] = v
	}
}

func (n *joinedQueryNode) ComputeContraintsForRight(
	row *Row,
	resultEquals map[int]*Value,
	resultRanges map[int]*Range,
	resultExclusion map[int][]*Value,
	equalsBytes map[int][]byte,
	exclusionBytes map[int]map[string]bool,
	rowBytes map[int][]byte,
) (bool, error) {
	clear(resultRanges)
	clear(resultEquals)
	// We do NOT want to clear exclusion map fully as we append to slices inside.
	// But actually, we should clear the map keys, and reuse the slices if possible?
	// For now, to be safe and match previous logic:
	clear(resultExclusion)
	clear(equalsBytes)
	clear(exclusionBytes)
	clear(rowBytes)

	for _, cond := range n.conditions {
		// When computing constraints for the right side, we use values from the left side (row).
		// The row comes from the left child, so its indices are 0 to left.ColumnsCount-1.
		// cond.LeftField refers to a column in the left table, so we use it directly.
		var key []byte
		if rowByte, ok := rowBytes[cond.LeftField]; ok {
			key = rowByte
		} else {
			var leftVal any
			err := row.Get(cond.LeftField, &leftVal)
			if err != nil {
				return false, err
			}
			key, err = ToKey(leftVal)
			if err != nil {
				return false, err
			}
			rowBytes[cond.LeftField] = key
		}
		var curRange *Range
		switch cond.Operator {
		case EQ:
			if excs, err := exclusionBytes[cond.RightField]; err {
				if excs[string(key)] {
					return false, nil
				}
			}
			if rng, ok := resultRanges[cond.RightField]; ok {
				// Avoid allocating ValueOfRaw every time
				valCheck := &Value{marshaler: orderedMaUn, raw: key}
				contains, err := rng.Contains(valCheck)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				delete(resultRanges, cond.RightField)
			}
			if existing, exists := equalsBytes[cond.RightField]; exists {
				if !bytes.Equal(existing, key) {
					return false, nil
				}
			} else {
				equalsBytes[cond.RightField] = key
				// Use existing Value struct if possible, or allocate one
				if val, exists := resultEquals[cond.RightField]; exists {
					val.SetRaw(key, orderedMaUn)
				} else {
					resultEquals[cond.RightField] = ValueOfRaw(key, orderedMaUn)
				}
			}
		case LT:
			var err error
			curRange, err = NewRangeFromBytes(nil, key, false, false)
			if err != nil {
				return false, err
			}

		case LTE:
			var err error
			curRange, err = NewRangeFromBytes(nil, key, false, true)
			if err != nil {
				return false, err
			}
		case GT:
			var err error
			curRange, err = NewRangeFromBytes(key, nil, false, false)
			if err != nil {
				return false, err
			}
		case GTE:
			var err error
			curRange, err = NewRangeFromBytes(key, nil, true, false)
			if err != nil {
				return false, err
			}
		case NEQ:
			// curValue := ValueOfRaw(key, orderedMaUn) // Allocation
			if eqb, ok := equalsBytes[cond.RightField]; ok {
				if bytes.Equal(eqb, key) {
					return false, nil
				}
			}
			if excs, exists := exclusionBytes[cond.RightField]; !exists {
				exclusionBytes[cond.RightField] = make(map[string]bool)
				// We need a Value here for resultExclusion.
				// Since resultExclusion is a list, we append.
				// For now, we accept allocation here as NEQ might be less frequent or we can pool Values later.
				curValue := ValueOfRaw(key, orderedMaUn)
				resultExclusion[cond.RightField] = append(resultExclusion[cond.RightField], curValue)
				exclusionBytes[cond.RightField][string(key)] = true
			} else if !excs[string(key)] {
				curValue := ValueOfRaw(key, orderedMaUn)
				resultExclusion[cond.RightField] = append(resultExclusion[cond.RightField], curValue)
				excs[string(key)] = true
			}
		default:
			return false, ErrUnsupportedOperator(cond.Operator)
		}
		if curRange != nil {
			if existingEquals, exists := resultEquals[cond.RightField]; exists {
				contains, err := curRange.Contains(existingEquals)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				continue
			}
			if existingRange, ok := resultRanges[cond.RightField]; ok {
				var err error
				resultRanges[cond.RightField], err = existingRange.Merge(curRange)
				if err != nil {
					return false, err
				}
			} else {
				resultRanges[cond.RightField] = curRange
			}
		}
	}
	return true, nil
}

func (n *joinedQueryNode) ComputeContraintsForLeft(
	row *Row,
	resultEquals map[int]*Value,
	resultRanges map[int]*Range,
	resultExclusion map[int][]*Value,
	equalsBytes map[int][]byte,
	exclusionBytes map[int]map[string]bool,
	rowBytes map[int][]byte,
) (bool, error) {
	clear(resultRanges)
	clear(resultEquals)
	// Same as Right side
	clear(resultExclusion)
	clear(equalsBytes)
	clear(exclusionBytes)
	clear(rowBytes)
	for _, cond := range n.conditions {
		// When computing constraints for the left side, we use values from the right side (row).
		// The row comes from the right child, so its indices are left.ColumnsCount to left.ColumnsCount+right.ColumnsCount-1.
		// cond.RightField refers to a column in the right table, so we need to adjust it.
		var key []byte
		if rowByte, ok := rowBytes[cond.RightField]; ok {
			key = rowByte
		} else {
			var rightVal any
			err := row.Get(cond.RightField, &rightVal)
			if err != nil {
				return false, err
			}
			key, err = ToKey(rightVal)
			if err != nil {
				return false, err
			}
			rowBytes[cond.RightField] = key
		}
		var curRange *Range
		switch cond.Operator {
		case EQ:
			if excs, err := exclusionBytes[cond.LeftField]; err {
				if excs[string(key)] {
					return false, nil
				}
			}
			if rng, ok := resultRanges[cond.LeftField]; ok {
				valCheck := &Value{marshaler: orderedMaUn, raw: key}
				contains, err := rng.Contains(valCheck)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				delete(resultRanges, cond.LeftField)
			}
			if existing, exists := equalsBytes[cond.LeftField]; exists {
				if !bytes.Equal(existing, key) {
					return false, nil
				}
			} else {
				equalsBytes[cond.LeftField] = key
				if val, exists := resultEquals[cond.LeftField]; exists {
					val.SetRaw(key, orderedMaUn)
				} else {
					resultEquals[cond.LeftField] = ValueOfRaw(key, orderedMaUn)
				}
			}
		case LT:
			var err error
			curRange, err = NewRangeFromBytes(nil, key, false, false)
			if err != nil {
				return false, err
			}
		case LTE:
			var err error
			curRange, err = NewRangeFromBytes(nil, key, false, true)
			if err != nil {
				return false, err
			}
		case GT:
			var err error
			curRange, err = NewRangeFromBytes(key, nil, false, false)
			if err != nil {
				return false, err
			}
		case GTE:
			var err error
			curRange, err = NewRangeFromBytes(key, nil, true, false)
			if err != nil {
				return false, err
			}
		case NEQ:
			// curValue := ValueOfRaw(key, orderedMaUn)
			if eqb, ok := equalsBytes[cond.LeftField]; ok {
				if bytes.Equal(eqb, key) {
					return false, nil
				}
			}
			if excs, exists := exclusionBytes[cond.LeftField]; !exists {
				exclusionBytes[cond.LeftField] = make(map[string]bool)
				curValue := ValueOfRaw(key, orderedMaUn)
				resultExclusion[cond.LeftField] = append(resultExclusion[cond.LeftField], curValue)
				exclusionBytes[cond.LeftField][string(key)] = true
			} else if !excs[string(key)] {
				curValue := ValueOfRaw(key, orderedMaUn)
				resultExclusion[cond.LeftField] = append(resultExclusion[cond.LeftField], curValue)
				excs[string(key)] = true
			}
		default:
			return false, ErrUnsupportedOperator(cond.Operator)
		}
		if curRange != nil {
			if existingEquals, exists := resultEquals[cond.LeftField]; exists {
				contains, err := curRange.Contains(existingEquals)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				continue
			}
			if existingRange, ok := resultRanges[cond.LeftField]; ok {
				var err error
				resultRanges[cond.LeftField], err = existingRange.Merge(curRange)
				if err != nil {
					return false, err
				}
			} else {
				resultRanges[cond.LeftField] = curRange
			}
		}
	}
	return true, nil
}

func (n *joinedQueryNode) propagateToParents(row *Row, child queryNode) error {
	// fmt.Printf("DEBUG: JoinedNode %p propagate. Child: %p (Left: %p, Right: %p)\n", n, child, n.left, n.right)
	switch child {
	case n.left:
		// Pre-allocate buffers for reuse (or just once here since this is likely not the tightest loop in some cases,
		// but propagateToParents is called for each row insert/update).
		// Note: Ideally, these should be pooled or reused if this is hot.
		// For now, we adapt to the new API.
		rowRightEquals := make(map[int]*Value)
		rowRightRanges := make(map[int]*Range)
		rowRightExclusions := make(map[int][]*Value)
		equalsBytes := make(map[int][]byte)
		exclusionBytes := make(map[int]map[string]bool)
		rowBytes := make(map[int][]byte)

		possible, err := n.ComputeContraintsForRight(
			row,
			rowRightEquals,
			rowRightRanges,
			rowRightExclusions,
			equalsBytes,
			exclusionBytes,
			rowBytes,
		)
		if err != nil {
			return err
		}
		if !possible {
			// Constraints conflicted, no matches possible
			return nil
		}
		// fmt.Printf("DEBUG: Join Left->Right. Row: %v. Ranges: %v\n", spew.Sdump(row.values), rightRanges)
		bestIndexRight, bestRangesRight, err := n.right.metadata().bestIndex(rowRightEquals, rowRightRanges)
		if err != nil {
			return err
		}
		cols := make(map[int]bool)
		for k := range n.right.metadata().ColumnsCount {
			cols[k] = true
		}
		rightSeq, err := n.right.Find(bestIndexRight, bestRangesRight, rowRightEquals, rowRightRanges, rowRightExclusions, cols)
		if err != nil {
			return err
		}

		joinedRow := &Row{
			values: make(map[int][]byte),
			maUn:   row.maUn,
		}

		for rightRow, err := range rightSeq {
			if err != nil {
				return err
			}
			n.joinInto(joinedRow, row, rightRow)
			// Propagate to JOIN's parents, not left's parents
			for _, parent := range n.parents {
				if err := parent.propagateToParents(joinedRow, n); err != nil {
					return err
				}
			}
		}
	case n.right:
		rowLeftEquals := make(map[int]*Value)
		rowLeftRanges := make(map[int]*Range)
		rowLeftExclusions := make(map[int][]*Value)
		equalsBytes := make(map[int][]byte)
		exclusionBytes := make(map[int]map[string]bool)
		rowBytes := make(map[int][]byte)

		possible, err := n.ComputeContraintsForLeft(
			row,
			rowLeftEquals,
			rowLeftRanges,
			rowLeftExclusions,
			equalsBytes,
			exclusionBytes,
			rowBytes,
		)
		if err != nil {
			return err
		}
		if !possible {
			// Constraints conflicted, no matches possible
			return nil
		}
		// fmt.Printf("DEBUG: Join Right->Left. Row: %v. Ranges: %v\n", spew.Sdump(row.values), leftRanges)
		bestIndexLeft, bestRangesLeft, err := n.left.metadata().bestIndex(rowLeftEquals, rowLeftRanges)
		cols := make(map[int]bool)
		for k := range n.left.metadata().ColumnsCount {
			cols[k] = true
		}
		leftSeq, err := n.left.Find(bestIndexLeft, bestRangesLeft, rowLeftEquals, rowLeftRanges, rowLeftExclusions, cols)
		if err != nil {
			return err
		}

		joinedRow := &Row{
			values: make(map[int][]byte),
			maUn:   row.maUn,
		}

		count := 0
		for leftRow, err := range leftSeq {
			count++
			if err != nil {
				return err
			}
			// fmt.Printf("DEBUG: Join Match Found! LeftRow: %v\n", spew.Sdump(leftRow.values))
			n.joinInto(joinedRow, leftRow, row)
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
		for col := range refCols {
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
	indexRange *Range,
	equals map[int]*Value,
	ranges map[int]*Range,
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
	childRanges := make(map[int]*Range)
	for field, r := range ranges {
		if field < n.metadataObj.ColumnsCount {
			childRanges[n.columns[field]] = r
		} else {
			return nil, ErrFieldNotFound(fmt.Sprintf("column %d", field))
		}
	}
	childIndex := uint64(0)
	refCols := ReferenceColumns(mainIndex)
	for col := range refCols {
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
		childVals := slices.Clone(vals)
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
	ranges    map[int]*Range
	equals    map[int]*Value
	exclusion map[int][]*Value
	backing   *storage
	baseQueryNode
	explored bool
}

func initBackedQueryNode(result *backedQueryNode, backing *storage, equals map[int]*Value, ranges map[int]*Range, exclusion map[int][]*Value) {
	result.equals = equals
	result.exclusion = exclusion
	result.ranges = ranges
	result.backing = backing
	result.baseQueryNode.metadataObj = backing.metadata
}

func (n *backedQueryNode) Find(
	mainIndex uint64,
	indexRange *Range,
	equals map[int]*Value,
	ranges map[int]*Range,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	if !n.explored {
		return func(func(*Row, error) bool) {
		}, nil
	}
	mergedEquals := make(map[int]*Value)
	mergedRanges := make(map[int]*Range)
	mergedExclusion := make(map[int][]*Value)
	possible, err := mergeEquals(&mergedEquals, n.equals, equals)
	if err != nil {
		return nil, err
	}
	if !possible {
		return func(func(*Row, error) bool) {
		}, nil
	}
	if err := MergeRangesMap(&mergedRanges, n.ranges, ranges); err != nil {
		return nil, err
	}
	mergeNotEquals(&mergedExclusion, n.exclusion, exclusion)
	if mainIndex == 0 {
		bestIndex, bestRanges, err := n.backing.metadata.bestIndex(mergedEquals, mergedRanges)
		if err != nil {
			return nil, err
		}
		// println("DEBUG: BackedNode Find. BestIndex:", bestIndex, "BestRanges:", bestRanges)
		return n.backing.find(bestIndex, bestRanges, mergedEquals, mergedRanges, mergedExclusion, cols)
	}
	// println("DEBUG: BackedNode Find. Given Index:", mainIndex, "IndexRange:", indexRange)
	return n.backing.find(mainIndex, indexRange, mergedEquals, mergedRanges, mergedExclusion, cols)
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
