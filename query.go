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
