package thunderdb

import (
	"bytes"
	"errors"
	"iter"
	"maps"
	"slices"
)

type queryNode interface {
	AddParent(parent queryNode)
	Find(mainIndex uint64, indexRange *interval, equals map[int]*Value, ranges map[int]*interval, exclusion map[int][]*Value, cols map[int]bool) (iter.Seq2[*Row, error], error)
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

type closureFilterNode struct {
	equals    map[int]*Value
	ranges    map[int]*interval
	exclusion map[int][]*Value
	child     *closureNode
	baseQueryNode
}

func initClosureFilterNode(result *closureFilterNode, child *closureNode, equals map[int]*Value, ranges map[int]*interval, exclusion map[int][]*Value) {
	result.child = child
	result.equals = equals
	result.ranges = ranges
	result.exclusion = exclusion
	child.AddParent(result)
	result.baseQueryNode.metadataObj = Metadata{
		ColumnsCount: child.metadataObj.ColumnsCount,
		Indexes:      child.metadataObj.Indexes,
	}
}

func (n *closureFilterNode) Find(
	mainIndex uint64,
	indexRange *interval,
	equals map[int]*Value,
	ranges map[int]*interval,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	mergedEquals := make(map[int]*Value)
	mergedRanges := make(map[int]*interval)
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
	return n.child.Find(mainIndex, indexRange, mergedEquals, mergedRanges, mergedExclusion, cols)
}

func (n *closureFilterNode) propagateToParents(row *Row, child queryNode) error {
	// check if row matches the filter
	vals := maps.Collect(row.Iter())
	matches, err := inRanges(vals, n.equals, n.ranges, n.exclusion)
	if err != nil {
		return err
	}
	if !matches {
		return nil
	}
	for _, parent := range n.parents {
		if err := parent.propagateToParents(row, n); err != nil {
			return err
		}
	}
	return nil
}

type closureNode struct {
	backing *storage
	baseQueryNode
}

func initClosureNode(result *closureNode, backing *storage) {
	result.backing = backing
	result.baseQueryNode.metadataObj = Metadata{
		ColumnsCount: backing.metadata.ColumnsCount,
		Indexes:      backing.metadata.Indexes,
	}
}

func (n *closureNode) Find(
	mainIndex uint64,
	indexRange *interval,
	equals map[int]*Value,
	ranges map[int]*interval,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	return n.backing.find(mainIndex, indexRange, equals, ranges, exclusion, cols)
}

func (n *closureNode) propagateToParents(row *Row, child queryNode) error {
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
	for _, parent := range n.parents {
		if err := parent.propagateToParents(row, n); err != nil {
			return err
		}
	}
	return nil
}

type joinedQueryNode struct {
	conditions            []JoinCondition
	left                  queryNode
	right                 queryNode
	preferredScanLeft     bool
	mustUsePreferScanLeft bool
	// Merge join decision made at initialization time
	canUseMergeJoin       bool
	mergeJoinLeftIndex    uint64
	mergeJoinRightIndex   uint64
	mergeJoinConditions   []JoinCondition // EQ conditions for merge join
	mergeJoinNonEQFilters []JoinCondition // Non-EQ conditions to apply as post-filters
	baseQueryNode
}

// joinSide encapsulates one side of a join operation (outer or inner)
type joinSide struct {
	node      queryNode
	equals    map[int]*Value
	ranges    map[int]*interval
	exclusion map[int][]*Value
	cols      map[int]bool
	isLeft    bool // true if this is the left side of the join
}

// nestedLoopJoin encapsulates the state and logic for nested loop join execution
type nestedLoopJoin struct {
	parent     *joinedQueryNode
	outer      *joinSide // Table to scan
	inner      *joinSide // Table to lookup
	mainIndex  uint64
	indexRange *interval
}

func initJoinedQueryNode(result *joinedQueryNode, left, right queryNode, conditions []JoinCondition) error {
	result.left = left
	result.right = right
	result.conditions = conditions
	if err := initJoinedMetadata(&result.baseQueryNode.metadataObj, left.metadata(), right.metadata()); err != nil {
		return err
	}
	left.AddParent(result)
	right.AddParent(result)
	// Decide on preferred scan side based on conditions
	result.preferredScanLeft, result.mustUsePreferScanLeft = result.chooseNestedLoopDirection()
	// Determine if merge join is possible based on join conditions and available indexes
	result.canUseMergeJoin, result.mergeJoinLeftIndex, result.mergeJoinRightIndex, result.mergeJoinConditions, result.mergeJoinNonEQFilters = result.determineMergeJoinCapability()
	return nil
}

func (n *joinedQueryNode) Find(
	mainIndex uint64,
	indexRange *interval,
	equals map[int]*Value,
	ranges map[int]*interval,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	// Split constraints by left and right tables
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

	// Try merge join first if it's been determined to be possible
	if seq, usedMergeJoin, err := n.tryMergeJoin(leftEquals, rightEquals, leftRanges, rightRanges, leftExclusion, rightExclusion, leftCols, rightCols); err != nil {
		return nil, err
	} else if usedMergeJoin {
		return seq, nil
	}

	// Fall back to nested loop join
	// Determine which side to scan based on preferences and index hints
	scanLeft := n.shouldScanLeft(mainIndex)

	if scanLeft {
		outer := &joinSide{n.left, leftEquals, leftRanges, leftExclusion, leftCols, true}
		inner := &joinSide{n.right, rightEquals, rightRanges, rightExclusion, rightCols, false}
		return n.executeNestedLoop(outer, inner, mainIndex, indexRange)
	} else {
		outer := &joinSide{n.right, rightEquals, rightRanges, rightExclusion, rightCols, false}
		inner := &joinSide{n.left, leftEquals, leftRanges, leftExclusion, leftCols, true}
		return n.executeNestedLoop(outer, inner, mainIndex, indexRange)
	}
}

// shouldScanLeft determines which table to use as the outer loop
func (n *joinedQueryNode) shouldScanLeft(mainIndex uint64) bool {
	// If we have a strong preference, use it
	if n.mustUsePreferScanLeft {
		return n.preferredScanLeft
	}
	// If mainIndex suggests a specific side, honor it
	if mainIndex < (uint64(1) << n.left.metadata().ColumnsCount) {
		return true
	}
	// Otherwise use our preferred direction
	return n.preferredScanLeft
}

// executeNestedLoop performs a nested loop join with the given outer and inner sides
// This unified function works for both left-outer and right-outer configurations
func (n *joinedQueryNode) executeNestedLoop(
	outer, inner *joinSide,
	mainIndex uint64,
	indexRange *interval,
) (iter.Seq2[*Row, error], error) {
	// Determine the main index for the outer table
	var outerMainIndex uint64
	var outerMainRanges *interval
	var err error

	// Check if we have an index hint for the outer side
	if indexRange != nil && n.isIndexForSide(mainIndex, outer.isLeft) {
		outerMainIndex = n.extractIndexForSide(mainIndex, outer.isLeft)
		outerMainRanges = indexRange
	} else {
		// No hint provided, use bestIndex
		outerMainIndex, outerMainRanges, err = outer.node.metadata().bestIndex(outer.equals, outer.ranges)
		if err != nil {
			return nil, err
		}
	}

	// Start scanning the outer table
	outerSeq, err := outer.node.Find(outerMainIndex, outerMainRanges, outer.equals, outer.ranges, outer.exclusion, outer.cols)
	if err != nil {
		return nil, err
	}

	// Pre-calculate capacity for joined row map
	joinedRowCap := n.left.metadata().ColumnsCount + n.right.metadata().ColumnsCount

	return func(yield func(*Row, error) bool) {
		// Buffers for merging constraints
		mergedInnerEquals := make(map[int]*Value)
		mergedInnerExclusion := make(map[int][]*Value)
		mergedInnerRanges := make(map[int]*interval)

		// Scratch buffers for constraint computation
		rowInnerEquals := make(map[int]*Value)
		rowInnerRanges := make(map[int]*interval)
		rowInnerExclusion := make(map[int][]*Value)
		equalsBytes := make(map[int][]byte)
		exclusionBytes := make(map[int]map[string]bool)
		rowBytes := make(map[int][]byte)

		// Reusable row for joining
		joinedRow := &Row{
			values: make(map[int][]byte, joinedRowCap),
		}

		for outerRow, err := range outerSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			// Compute constraints for inner table based on outer row
			possible, err := n.computeConstraints(
				outerRow,
				outer.isLeft, // direction: true if outer is left
				rowInnerEquals,
				rowInnerRanges,
				rowInnerExclusion,
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
				// Constraints conflicted, skip this outer row
				continue
			}

			// Merge row-derived constraints with existing constraints
			ok, err := mergeEquals(&mergedInnerEquals, inner.equals, rowInnerEquals)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !ok {
				// Constraints conflicted (e.g. x=1 AND x=2), skip
				continue
			}

			if err := MergeRangesMap(&mergedInnerRanges, inner.ranges, rowInnerRanges); err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			mergeNotEquals(&mergedInnerExclusion, inner.exclusion, rowInnerExclusion)

			// Find best index for inner table with merged constraints
			innerMainIndex, innerMainRanges, err := inner.node.metadata().bestIndex(mergedInnerEquals, mergedInnerRanges)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			// Lookup matching rows in inner table
			innerSeq, err := inner.node.Find(innerMainIndex, innerMainRanges, mergedInnerEquals, mergedInnerRanges, mergedInnerExclusion, inner.cols)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}

			// Join outer row with each matching inner row
			for innerRow, err := range innerSeq {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}

				// Join rows based on which side is which
				if outer.isLeft {
					n.joinInto(joinedRow, outerRow, innerRow)
				} else {
					n.joinInto(joinedRow, innerRow, outerRow)
				}

				if !yield(joinedRow, nil) {
					return
				}
			}
		}
	}, nil
}

// isIndexForSide checks if the given mainIndex refers to the specified side
func (n *joinedQueryNode) isIndexForSide(mainIndex uint64, isLeft bool) bool {
	leftMaxIndex := uint64(1) << n.left.metadata().ColumnsCount
	if isLeft {
		return mainIndex < leftMaxIndex
	}
	return mainIndex >= leftMaxIndex
}

// extractIndexForSide extracts the actual index bits for the specified side
func (n *joinedQueryNode) extractIndexForSide(mainIndex uint64, isLeft bool) uint64 {
	if isLeft {
		return mainIndex
	}
	return mainIndex >> uint64(n.left.metadata().ColumnsCount)
}

func (n *joinedQueryNode) joinInto(dest *Row, leftRow, rightRow *Row) {
	clear(dest.values)
	maps.Copy(dest.values, leftRow.values)
	offset := n.left.metadata().ColumnsCount
	for k, v := range rightRow.values {
		dest.values[k+offset] = v
	}
}

// computeConstraints is a unified function that computes constraints for the target side based on values from the source row
// If outerIsLeft is true: source=left row, target=right; otherwise source=right row, target=left
func (n *joinedQueryNode) computeConstraints(
	sourceRow *Row,
	outerIsLeft bool,
	resultEquals map[int]*Value,
	resultRanges map[int]*interval,
	resultExclusion map[int][]*Value,
	equalsBytes map[int][]byte,
	exclusionBytes map[int]map[string]bool,
	rowBytes map[int][]byte,
) (bool, error) {
	clear(resultRanges)
	clear(resultEquals)
	clear(resultExclusion)
	clear(equalsBytes)
	clear(exclusionBytes)
	clear(rowBytes)

	for _, cond := range n.conditions {
		// Determine which fields to read from and write to
		var sourceField, targetField int
		if outerIsLeft {
			sourceField = cond.Left
			targetField = cond.Right
		} else {
			sourceField = cond.Right
			targetField = cond.Left
		}

		// Get value from source row
		var key []byte
		var sourceVal any
		if rowByte, ok := rowBytes[sourceField]; ok {
			key = rowByte
		} else {
			err := sourceRow.Get(sourceField, &sourceVal)
			if err != nil {
				return false, err
			}
			key, err = ToKey(ValueOfLiteral(sourceVal))
			if err != nil {
				return false, err
			}
			rowBytes[sourceField] = key
		}

		var curRange *interval
		operator := cond.Operator

		// For right-to-left computation, need to invert comparison operators
		if !outerIsLeft {
			switch operator {
			case LT:
				operator = GT
			case LTE:
				operator = GTE
			case GT:
				operator = LT
			case GTE:
				operator = LTE
			}
		}

		switch operator {
		case EQ:
			if excs, err := exclusionBytes[targetField]; err {
				if excs[string(key)] {
					return false, nil
				}
			}
			if rng, ok := resultRanges[targetField]; ok {
				valCheck := &Value{raw: key}
				contains, err := rng.Contains(valCheck)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				delete(resultRanges, targetField)
			}
			if existing, exists := equalsBytes[targetField]; exists {
				if !bytes.Equal(existing, key) {
					return false, nil
				}
			} else {
				equalsBytes[targetField] = key
				if val, exists := resultEquals[targetField]; exists {
					val.SetRaw(key)
				} else {
					if sourceVal == nil {
						sourceRow.Get(sourceField, &sourceVal)
					}
					resultEquals[targetField] = ValueOfLiteral(sourceVal)
				}
			}
		case LT:
			// Source < Target means Target > Source, so Target is in range (Source, nil)
			var err error
			curRange, err = newIntervalFromBytes(key, nil, false, false)
			if err != nil {
				return false, err
			}
		case LTE:
			// Source <= Target means Target >= Source, so Target is in range [Source, nil)
			var err error
			curRange, err = newIntervalFromBytes(key, nil, true, false)
			if err != nil {
				return false, err
			}
		case GT:
			// Source > Target means Target < Source, so Target is in range (nil, Source)
			var err error
			curRange, err = newIntervalFromBytes(nil, key, false, false)
			if err != nil {
				return false, err
			}
		case GTE:
			// Source >= Target means Target <= Source, so Target is in range (nil, Source]
			var err error
			curRange, err = newIntervalFromBytes(nil, key, false, true)
			if err != nil {
				return false, err
			}
		case NEQ:
			if eqb, ok := equalsBytes[targetField]; ok {
				if bytes.Equal(eqb, key) {
					return false, nil
				}
			}
			if excs, exists := exclusionBytes[targetField]; !exists {
				exclusionBytes[targetField] = make(map[string]bool)
				curValue := ValueOfRaw(key)
				resultExclusion[targetField] = append(resultExclusion[targetField], curValue)
				exclusionBytes[targetField][string(key)] = true
			} else if !excs[string(key)] {
				curValue := ValueOfRaw(key)
				resultExclusion[targetField] = append(resultExclusion[targetField], curValue)
				excs[string(key)] = true
			}
		default:
			return false, ErrUnsupportedOperator(cond.Operator)
		}

		if curRange != nil {
			if existingEquals, exists := resultEquals[targetField]; exists {
				contains, err := curRange.Contains(existingEquals)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				continue
			}
			if existingRange, ok := resultRanges[targetField]; ok {
				var err error
				resultRanges[targetField], err = existingRange.Merge(curRange)
				if err != nil {
					return false, err
				}
			} else {
				resultRanges[targetField] = curRange
			}
		}
	}
	return true, nil
}

func (n *joinedQueryNode) ComputeContraintsForRight(
	row *Row,
	resultEquals map[int]*Value,
	resultRanges map[int]*interval,
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
		var leftVal any
		if rowByte, ok := rowBytes[cond.Left]; ok {
			key = rowByte
		} else {
			err := row.Get(cond.Left, &leftVal)
			if err != nil {
				return false, err
			}
			key, err = ToKey(ValueOfLiteral(leftVal))
			if err != nil {
				return false, err
			}
			rowBytes[cond.Left] = key
		}
		var curRange *interval
		switch cond.Operator {
		case EQ:
			if excs, err := exclusionBytes[cond.Right]; err {
				if excs[string(key)] {
					return false, nil
				}
			}
			if rng, ok := resultRanges[cond.Right]; ok {
				// Avoid allocating ValueOfRaw every time
				valCheck := &Value{raw: key}
				contains, err := rng.Contains(valCheck)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				delete(resultRanges, cond.Right)
			}
			if existing, exists := equalsBytes[cond.Right]; exists {
				if !bytes.Equal(existing, key) {
					return false, nil
				}
			} else {
				equalsBytes[cond.Right] = key
				// Use existing Value struct if possible, or allocate one
				if val, exists := resultEquals[cond.Right]; exists {
					val.SetRaw(key)
				} else {
					// Use ValueOfLiteral instead of ValueOfRaw to avoid double-wrapping
					// when bestIndex calls GetValue() + ToKey()
					if leftVal == nil {
						// Need to get the value if we didn't already
						row.Get(cond.Left, &leftVal)
					}
					resultEquals[cond.Right] = ValueOfLiteral(leftVal)
				}
			}
		case LT:
			// Left < Right means Right > Left, so Right is in range (Left, nil)
			var err error
			curRange, err = newIntervalFromBytes(key, nil, false, false)
			if err != nil {
				return false, err
			}

		case LTE:
			// Left <= Right means Right >= Left, so Right is in range [Left, nil)
			var err error
			curRange, err = newIntervalFromBytes(key, nil, true, false)
			if err != nil {
				return false, err
			}
		case GT:
			// Left > Right means Right < Left, so Right is in range (nil, Left)
			var err error
			curRange, err = newIntervalFromBytes(nil, key, false, false)
			if err != nil {
				return false, err
			}
		case GTE:
			// Left >= Right means Right <= Left, so Right is in range (nil, Left]
			var err error
			curRange, err = newIntervalFromBytes(nil, key, false, true)
			if err != nil {
				return false, err
			}
		case NEQ:
			// curValue := ValueOfRaw(key, orderedMaUn) // Allocation
			if eqb, ok := equalsBytes[cond.Right]; ok {
				if bytes.Equal(eqb, key) {
					return false, nil
				}
			}
			if excs, exists := exclusionBytes[cond.Right]; !exists {
				exclusionBytes[cond.Right] = make(map[string]bool)
				// We need a Value here for resultExclusion.
				// Since resultExclusion is a list, we append.
				// For now, we accept allocation here as NEQ might be less frequent or we can pool Values later.
				curValue := ValueOfRaw(key)
				resultExclusion[cond.Right] = append(resultExclusion[cond.Right], curValue)
				exclusionBytes[cond.Right][string(key)] = true
			} else if !excs[string(key)] {
				curValue := ValueOfRaw(key)
				resultExclusion[cond.Right] = append(resultExclusion[cond.Right], curValue)
				excs[string(key)] = true
			}
		default:
			return false, ErrUnsupportedOperator(cond.Operator)
		}
		if curRange != nil {
			if existingEquals, exists := resultEquals[cond.Right]; exists {
				contains, err := curRange.Contains(existingEquals)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				continue
			}
			if existingRange, ok := resultRanges[cond.Right]; ok {
				var err error
				resultRanges[cond.Right], err = existingRange.Merge(curRange)
				if err != nil {
					return false, err
				}
			} else {
				resultRanges[cond.Right] = curRange
			}
		}
	}
	return true, nil
}

func (n *joinedQueryNode) ComputeContraintsForLeft(
	row *Row,
	resultEquals map[int]*Value,
	resultRanges map[int]*interval,
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
		var rightVal any
		if rowByte, ok := rowBytes[cond.Right]; ok {
			key = rowByte
		} else {
			err := row.Get(cond.Right, &rightVal)
			if err != nil {
				return false, err
			}
			key, err = ToKey(ValueOfLiteral(rightVal))
			if err != nil {
				return false, err
			}
			rowBytes[cond.Right] = key
		}
		var curRange *interval
		switch cond.Operator {
		case EQ:
			if excs, err := exclusionBytes[cond.Left]; err {
				if excs[string(key)] {
					return false, nil
				}
			}
			if rng, ok := resultRanges[cond.Left]; ok {
				valCheck := &Value{raw: key}
				contains, err := rng.Contains(valCheck)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				delete(resultRanges, cond.Left)
			}
			if existing, exists := equalsBytes[cond.Left]; exists {
				if !bytes.Equal(existing, key) {
					return false, nil
				}
			} else {
				equalsBytes[cond.Left] = key
				if val, exists := resultEquals[cond.Left]; exists {
					val.SetRaw(key)
				} else {
					// Use ValueOfLiteral instead of ValueOfRaw to avoid double-wrapping
					// when bestIndex calls GetValue() + ToKey()
					if rightVal == nil {
						// Need to get the value if we didn't already
						row.Get(cond.Right, &rightVal)
					}
					resultEquals[cond.Left] = ValueOfLiteral(rightVal)
				}
			}
		case LT:
			var err error
			curRange, err = newIntervalFromBytes(nil, key, false, false)
			if err != nil {
				return false, err
			}
		case LTE:
			var err error
			curRange, err = newIntervalFromBytes(nil, key, false, true)
			if err != nil {
				return false, err
			}
		case GT:
			var err error
			curRange, err = newIntervalFromBytes(key, nil, false, false)
			if err != nil {
				return false, err
			}
		case GTE:
			var err error
			curRange, err = newIntervalFromBytes(key, nil, true, false)
			if err != nil {
				return false, err
			}
		case NEQ:
			// curValue := ValueOfRaw(key, orderedMaUn)
			if eqb, ok := equalsBytes[cond.Left]; ok {
				if bytes.Equal(eqb, key) {
					return false, nil
				}
			}
			if excs, exists := exclusionBytes[cond.Left]; !exists {
				exclusionBytes[cond.Left] = make(map[string]bool)
				curValue := ValueOfRaw(key)
				resultExclusion[cond.Left] = append(resultExclusion[cond.Left], curValue)
				exclusionBytes[cond.Left][string(key)] = true
			} else if !excs[string(key)] {
				curValue := ValueOfRaw(key)
				resultExclusion[cond.Left] = append(resultExclusion[cond.Left], curValue)
				excs[string(key)] = true
			}
		default:
			return false, ErrUnsupportedOperator(cond.Operator)
		}
		if curRange != nil {
			if existingEquals, exists := resultEquals[cond.Left]; exists {
				contains, err := curRange.Contains(existingEquals)
				if err != nil {
					return false, err
				}
				if !contains {
					return false, nil
				}
				continue
			}
			if existingRange, ok := resultRanges[cond.Left]; ok {
				var err error
				resultRanges[cond.Left], err = existingRange.Merge(curRange)
				if err != nil {
					return false, err
				}
			} else {
				resultRanges[cond.Left] = curRange
			}
		}
	}
	return true, nil
}

// determineMergeJoinCapability checks at initialization time if merge join is possible.
// This decision is based solely on:
// 1. Join conditions (must have at least one EQ condition)
// 2. Available indexes on both tables (both must have indexes on EQ join columns)
// Non-EQ conditions are separated and applied as post-filters after merge join.
// Returns: canUseMergeJoin, leftIndex, rightIndex, eqConditions, nonEQConditions
func (n *joinedQueryNode) determineMergeJoinCapability() (bool, uint64, uint64, []JoinCondition, []JoinCondition) {
	if len(n.conditions) == 0 {
		return false, 0, 0, nil, nil
	}

	// Separate EQ and non-EQ join conditions
	var eqConditions []JoinCondition
	var nonEQConditions []JoinCondition
	for _, cond := range n.conditions {
		if cond.Operator == EQ {
			eqConditions = append(eqConditions, cond)
		} else {
			// Non-EQ operators will be applied as post-filters
			nonEQConditions = append(nonEQConditions, cond)
		}
	}

	if len(eqConditions) == 0 {
		// No EQ conditions means we can't use merge join at all
		return false, 0, 0, nil, nil
	}

	// Build join column bitmasks for EQ conditions only
	leftJoinCols := uint64(0)
	rightJoinCols := uint64(0)
	for _, cond := range eqConditions {
		leftJoinCols |= 1 << uint64(cond.Left)
		rightJoinCols |= 1 << uint64(cond.Right)
	}

	// Check if both sides have indexes that cover the join columns
	// We check if there's an index where join columns form a prefix
	leftIndex, leftFound := n.findJoinPrefixIndex(n.left.metadata(), leftJoinCols)
	rightIndex, rightFound := n.findJoinPrefixIndex(n.right.metadata(), rightJoinCols)

	if !leftFound || !rightFound {
		return false, 0, 0, nil, nil
	}

	return true, leftIndex, rightIndex, eqConditions, nonEQConditions
}

// findJoinPrefixIndex finds an index where the join columns form a prefix.
// This is used at initialization time, so it doesn't consider query-time filtering conditions.
func (n *joinedQueryNode) findJoinPrefixIndex(meta *Metadata, joinBits uint64) (uint64, bool) {
	// First, try exact match
	if _, exists := meta.Indexes[joinBits]; exists {
		return joinBits, true
	}

	// Try to find a composite index where join columns form a prefix
	for indexBits := range meta.Indexes {
		// Check if this index contains all join columns
		if indexBits&joinBits != joinBits {
			continue
		}

		// Check if join columns form a prefix of this index
		refCols := ReferenceColumns(indexBits)
		allJoinColsAtPrefix := true
		seenNonJoinCol := false

		for col := range refCols {
			isJoinCol := (joinBits & (1 << uint64(col))) != 0

			if !isJoinCol {
				// This is not a join column, mark that we've seen a non-join column
				seenNonJoinCol = true
			} else if seenNonJoinCol {
				// This is a join column, but we've already seen a non-join column
				// So join columns don't form a prefix
				allJoinColsAtPrefix = false
				break
			}
		}

		if allJoinColsAtPrefix {
			return indexBits, true
		}
	}

	return 0, false
}

// tryMergeJoin attempts to use a merge join optimization.
// The decision of whether merge join is possible was made at initialization time.
// This method just checks if it's still beneficial given the query-time filtering conditions.
func (n *joinedQueryNode) tryMergeJoin(
	leftEquals, rightEquals map[int]*Value,
	leftRanges, rightRanges map[int]*interval,
	leftExclusion, rightExclusion map[int][]*Value,
	leftCols, rightCols map[int]bool,
) (iter.Seq2[*Row, error], bool, error) {
	// If merge join was determined to be impossible at init time, don't try
	if !n.canUseMergeJoin {
		return nil, false, nil
	}

	// Execute merge join using the pre-computed indexes
	seq, err := n.executeMergeJoin(
		leftEquals, rightEquals,
		leftRanges, rightRanges,
		leftExclusion, rightExclusion,
		leftCols, rightCols,
		n.mergeJoinConditions,
	)
	if err != nil {
		return nil, false, err
	}

	return seq, true, nil
}

// executeMergeJoin performs the actual merge join operation using pre-computed indexes
func (n *joinedQueryNode) executeMergeJoin(
	leftEquals, rightEquals map[int]*Value,
	leftRanges, rightRanges map[int]*interval,
	leftExclusion, rightExclusion map[int][]*Value,
	leftCols, rightCols map[int]bool,
	eqConditions []JoinCondition,
) (iter.Seq2[*Row, error], error) {
	// Use the pre-computed merge join indexes
	// For merge join, we scan both indexes in sorted order (no specific range constraint)

	// Get sequences from both sides using the merge join indexes
	leftSeq, err := n.left.Find(n.mergeJoinLeftIndex, nil, leftEquals, leftRanges, leftExclusion, leftCols)
	if err != nil {
		return nil, err
	}

	rightSeq, err := n.right.Find(n.mergeJoinRightIndex, nil, rightEquals, rightRanges, rightExclusion, rightCols)
	if err != nil {
		return nil, err
	}

	// Pre-calculate capacity for joined row map
	joinedRowCap := n.left.metadata().ColumnsCount + n.right.metadata().ColumnsCount

	return func(yield func(*Row, error) bool) {
		// Buffer for storing rows from left and right sides that match current key
		var leftBuffer []*Row
		var rightBuffer []*Row
		var currentLeftKey []byte
		var currentRightKey []byte

		leftNext, leftStop := iter.Pull2(leftSeq)
		rightNext, rightStop := iter.Pull2(rightSeq)
		defer leftStop()
		defer rightStop()

		// Pull initial rows
		leftRow, leftErr, leftOk := leftNext()
		rightRow, rightErr, rightOk := rightNext()

		for leftOk && leftErr == nil && rightOk && rightErr == nil {
			// Extract join key from left row
			currentLeftKey, err = n.extractJoinKey(leftRow, eqConditions, true)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				leftRow, leftErr, leftOk = leftNext()
				continue
			}

			// Extract join key from right row
			currentRightKey, err = n.extractJoinKey(rightRow, eqConditions, false)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				rightRow, rightErr, rightOk = rightNext()
				continue
			}

			// Compare keys
			cmp := bytes.Compare(currentLeftKey, currentRightKey)

			if cmp < 0 {
				// Left key < right key, advance left
				leftRow, leftErr, leftOk = leftNext()
			} else if cmp > 0 {
				// Left key > right key, advance right
				rightRow, rightErr, rightOk = rightNext()
			} else {
				// Keys match! Collect all left rows with this key
				leftBuffer = leftBuffer[:0]
				leftBuffer = append(leftBuffer, &Row{values: maps.Clone(leftRow.values)})
				matchKey := slices.Clone(currentLeftKey)

				// Collect all left rows with same key
				for {
					nextLeftRow, nextLeftErr, nextLeftOk := leftNext()
					if !nextLeftOk || nextLeftErr != nil {
						leftRow, leftErr, leftOk = nextLeftRow, nextLeftErr, nextLeftOk
						break
					}

					nextLeftKey, err := n.extractJoinKey(nextLeftRow, eqConditions, true)
					if err != nil {
						if !yield(nil, err) {
							return
						}
						leftRow, leftErr, leftOk = nextLeftRow, nextLeftErr, nextLeftOk
						break
					}

					if bytes.Equal(matchKey, nextLeftKey) {
						leftBuffer = append(leftBuffer, &Row{values: maps.Clone(nextLeftRow.values)})
					} else {
						// Different key, save for next iteration
						leftRow, leftErr, leftOk = nextLeftRow, nextLeftErr, nextLeftOk
						break
					}
				}

				// Collect all right rows with this key
				rightBuffer = rightBuffer[:0]
				rightBuffer = append(rightBuffer, &Row{values: maps.Clone(rightRow.values)})

				for {
					nextRightRow, nextRightErr, nextRightOk := rightNext()
					if !nextRightOk || nextRightErr != nil {
						rightRow, rightErr, rightOk = nextRightRow, nextRightErr, nextRightOk
						break
					}

					nextRightKey, err := n.extractJoinKey(nextRightRow, eqConditions, false)
					if err != nil {
						if !yield(nil, err) {
							return
						}
						rightRow, rightErr, rightOk = nextRightRow, nextRightErr, nextRightOk
						break
					}

					if bytes.Equal(matchKey, nextRightKey) {
						rightBuffer = append(rightBuffer, &Row{values: maps.Clone(nextRightRow.values)})
					} else {
						// Different key, save for next iteration
						rightRow, rightErr, rightOk = nextRightRow, nextRightErr, nextRightOk
						break
					}
				}

				// Emit all combinations of buffered left and right rows
				joinedRow := &Row{values: make(map[int][]byte, joinedRowCap)}
				for _, bufferedLeft := range leftBuffer {
					for _, bufferedRight := range rightBuffer {
						n.joinInto(joinedRow, bufferedLeft, bufferedRight)

						// Apply non-EQ filters if present
						if len(n.mergeJoinNonEQFilters) > 0 {
							passes, err := n.checkNonEQConditions(joinedRow, n.mergeJoinNonEQFilters)
							if err != nil {
								if !yield(nil, err) {
									return
								}
								continue
							}
							if !passes {
								// Row doesn't satisfy non-EQ conditions, skip it
								continue
							}
						}

						if !yield(joinedRow, nil) {
							return
						}
					}
				}
			}
		}

		// Handle any errors from the final pulls
		if leftErr != nil {
			if !yield(nil, leftErr) {
				return
			}
		}
		if rightErr != nil {
			if !yield(nil, rightErr) {
				return
			}
		}
	}, nil
}

// extractJoinKey creates a composite key from the join columns in a row
func (n *joinedQueryNode) extractJoinKey(row *Row, conditions []JoinCondition, isLeft bool) ([]byte, error) {
	values := make([]*Value, len(conditions))
	for i, cond := range conditions {
		var col int
		if isLeft {
			col = cond.Left
		} else {
			col = cond.Right
		}

		if valBytes, ok := row.values[col]; ok {
			values[i] = ValueOfRaw(valBytes)
		} else {
			return nil, ErrFieldNotFound(col)
		}
	}

	return ToKey(values...)
}

// checkNonEQConditions evaluates non-EQ join conditions on a joined row.
// The row contains values from both left and right tables.
// Returns true if all non-EQ conditions are satisfied, false otherwise.
func (n *joinedQueryNode) checkNonEQConditions(row *Row, nonEQConditions []JoinCondition) (bool, error) {
	leftColCount := n.left.metadata().ColumnsCount

	for _, cond := range nonEQConditions {
		// Get left value bytes
		leftBytes, ok := row.values[cond.Left]
		if !ok {
			return false, ErrFieldNotFound(cond.Left)
		}

		// Get right value bytes (offset by left column count)
		rightCol := leftColCount + cond.Right
		rightBytes, ok := row.values[rightCol]
		if !ok {
			return false, ErrFieldNotFound(rightCol)
		}

		// Compare byte representations
		cmp := bytes.Compare(leftBytes, rightBytes)

		satisfied := false
		switch cond.Operator {
		case GT:
			satisfied = cmp > 0
		case GTE:
			satisfied = cmp >= 0
		case LT:
			satisfied = cmp < 0
		case LTE:
			satisfied = cmp <= 0
		case NEQ:
			satisfied = cmp != 0
		default:
			return false, ErrUnsupportedOperator(cond.Operator)
		}

		if !satisfied {
			return false, nil
		}
	}

	return true, nil
}

// chooseNestedLoopDirection determines which table to scan (outer loop) for nested loop join.
// Returns true if we should scan the left table, false if we should scan the right table.
// Strategy: Scan the table WITHOUT an index on join columns (outer), and use the indexed
// table for lookups (inner), to avoid full table scans on the inner loop.
func (n *joinedQueryNode) chooseNestedLoopDirection() (isLeft, enforced bool) {
	// Extract join columns from conditions
	if len(n.conditions) == 0 {
		return true, false
	}

	leftJoinCols := uint64(0)
	rightJoinCols := uint64(0)
	for _, cond := range n.conditions {
		leftJoinCols |= 1 << uint64(cond.Left)
		rightJoinCols |= 1 << uint64(cond.Right)
	}

	if leftJoinCols == 0 || rightJoinCols == 0 {
		return true, false
	}

	// Check if each side has an index on join columns
	// Use the same logic as merge join determination
	_, leftHasJoinIndex := n.findJoinPrefixIndex(n.left.metadata(), leftJoinCols)
	_, rightHasJoinIndex := n.findJoinPrefixIndex(n.right.metadata(), rightJoinCols)

	// Decision logic:
	// - If only right has index on join cols: scan left, lookup right (return true)
	// - If only left has index on join cols: scan right, lookup left (return false)
	// - If both have indexes: default to scanning left (merge join should have been used)
	// - If neither has indexes: default to scanning left (both are bad)

	if leftHasJoinIndex && !rightHasJoinIndex {
		return false, true
	} else if !leftHasJoinIndex && rightHasJoinIndex {
		return true, true
	} else {
		// Both have or both don't have join indexes
		return true, false
	}
}

func (n *joinedQueryNode) propagateToParents(row *Row, child queryNode) error {
	// Determine which side the row came from
	childIsLeft := child == n.left
	if !childIsLeft && child != n.right {
		panic("unknown child in joinedQueryNode.propagateToParents")
	}

	// Get the opposite side to query
	otherSide := n.right
	if !childIsLeft {
		otherSide = n.left
	}

	// Pre-allocate constraint buffers
	equals := make(map[int]*Value)
	ranges := make(map[int]*interval)
	exclusions := make(map[int][]*Value)
	equalsBytes := make(map[int][]byte)
	exclusionBytes := make(map[int]map[string]bool)
	rowBytes := make(map[int][]byte)

	// Compute constraints for the opposite side
	possible, err := n.computeConstraints(row, childIsLeft, equals, ranges, exclusions, equalsBytes, exclusionBytes, rowBytes)
	if err != nil {
		return err
	}
	if !possible {
		return nil
	}

	// Find best index and query the other side
	bestIndex, bestRanges, err := otherSide.metadata().bestIndex(equals, ranges)
	if err != nil {
		return err
	}

	cols := make(map[int]bool)
	for k := range otherSide.metadata().ColumnsCount {
		cols[k] = true
	}

	matchSeq, err := otherSide.Find(bestIndex, bestRanges, equals, ranges, exclusions, cols)
	if err != nil {
		return err
	}

	// Join matching rows and propagate to parents
	joinedRow := &Row{values: make(map[int][]byte)}
	for matchRow, err := range matchSeq {
		if err != nil {
			return err
		}

		// Order arguments correctly: joinInto expects (dest, leftRow, rightRow)
		if childIsLeft {
			n.joinInto(joinedRow, row, matchRow)
		} else {
			n.joinInto(joinedRow, matchRow, row)
		}

		for _, parent := range n.parents {
			if err := parent.propagateToParents(joinedRow, n); err != nil {
				return err
			}
		}
	}

	return nil
}

type projectedQueryNode struct {
	columns []int
	child   queryNode
	baseQueryNode
}

func initProjectedQueryNode(result *projectedQueryNode, child queryNode, columns []int) error {
	// Validate that all projected columns exist in the child
	childMeta := child.metadata()
	for _, col := range columns {
		if col < 0 || col >= childMeta.ColumnsCount {
			return ErrFieldNotFound(col)
		}
	}
	result.child = child
	result.columns = columns
	childToResultColumnMap := make(map[int][]int)
	for i, col := range columns {
		childToResultColumnMap[col] = append(childToResultColumnMap[col], i)
	}
	indexes := make(map[uint64]bool)
	for idx, isUnique := range childMeta.Indexes {
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
	return nil
}

func (n *projectedQueryNode) Find(
	mainIndex uint64,
	indexRange *interval,
	equals map[int]*Value,
	ranges map[int]*interval,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	childCols := make(map[int]bool)
	for k := range cols {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(k)
		}
		childCols[n.columns[k]] = true
	}
	childRanges := make(map[int]*interval)
	for field, r := range ranges {
		if field < n.metadataObj.ColumnsCount {
			childRanges[n.columns[field]] = r
		} else {
			return nil, ErrFieldNotFound(field)
		}
	}
	childIndex := uint64(0)
	refCols := ReferenceColumns(mainIndex)
	for col := range refCols {
		if col >= len(n.columns) {
			return nil, ErrFieldNotFound(col)
		}
		childIndex |= 1 << uint64(n.columns[col])
	}
	childEquals := make(map[int]*Value)
	for k, v := range equals {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(k)
		}
		childEquals[n.columns[k]] = v
	}
	childExclusion := make(map[int][]*Value)
	for k, vals := range exclusion {
		if k >= len(n.columns) {
			return nil, ErrFieldNotFound(k)
		}
		childVals := slices.Clone(vals)
		childExclusion[n.columns[k]] = childVals
	}
	childSeq, err := n.child.Find(childIndex, indexRange, childEquals, childRanges, childExclusion, childCols)
	if err != nil {
		return nil, err
	}
	return func(yield func(*Row, error) bool) {
		// Reusable row for projection - moved outside loop
		newRow := &Row{
			values: make(map[int][]byte, len(cols)),
		}
		for childRow, err := range childSeq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			clear(newRow.values)
			for col := range cols {
				if col >= len(n.columns) {
					if !yield(nil, ErrFieldNotFound(col)) {
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
	}
	for k, col := range n.columns {
		if val, ok := row.values[col]; ok {
			parentRow.values[k] = val
		} else {
			return ErrFieldNotFound(col)
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
	ranges    map[int]*interval
	equals    map[int]*Value
	exclusion map[int][]*Value
	backing   *storage
	baseQueryNode
	explored bool
}

func initBackedQueryNode(result *backedQueryNode, backing *storage, equals map[int]*Value, ranges map[int]*interval, exclusion map[int][]*Value) {
	result.equals = equals
	result.exclusion = exclusion
	result.ranges = ranges
	result.backing = backing
	result.baseQueryNode.metadataObj = backing.metadata
}

func (n *backedQueryNode) Find(
	mainIndex uint64,
	indexRange *interval,
	equals map[int]*Value,
	ranges map[int]*interval,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	if !n.explored {
		return func(func(*Row, error) bool) {
		}, nil
	}
	mergedEquals := make(map[int]*Value)
	mergedRanges := make(map[int]*interval)
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
		return n.backing.find(bestIndex, bestRanges, mergedEquals, mergedRanges, mergedExclusion, cols)
	}
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
