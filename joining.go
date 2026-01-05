package thunderdb

import (
	"bytes"
	"iter"
	"maps"
	"slices"
)

type Joining struct {
	bodies          []linkedSelector
	columns         []string
	firstOccurences map[string]int
	parentsList     []*queryParent
	recursive       bool
	joinPlans       map[int][]int
}

func newJoining(bodies []linkedSelector) Selector {
	columnsSet := make(map[string]struct{})
	firstOccurences := make(map[string]int)
	recursive := false
	result := &Joining{}
	for bodyIdx, body := range bodies {
		bodyFields := body.Fields()
		for _, col := range body.Columns() {
			columnsSet[col] = struct{}{}
			if _, exists := firstOccurences[col]; !exists {
				firstOccurences[col] = bodyIdx
			} else {
				// Prioritize indexed or unique fields
				currentSpec := bodyFields[col]
				if currentSpec.Indexed || currentSpec.Unique {
					// Check if the current first occurrence has indexed/unique
					currentBodySpec := bodies[firstOccurences[col]].Fields()[col]
					if !currentBodySpec.Indexed && !currentBodySpec.Unique {
						// Replace with indexed/unique version
						firstOccurences[col] = bodyIdx
					}
				}
			}
		}
		if body.IsRecursive() {
			recursive = true
		}
		body.addParent(&queryParent{
			parent: result,
			index:  bodyIdx,
		})
	}
	columns := maps.Keys(columnsSet)
	result.columns = slices.Collect(columns)
	result.bodies = bodies
	result.firstOccurences = firstOccurences
	result.recursive = recursive
	result.joinPlans = make(map[int][]int)

	// Precompute join plans for each body acting as a seed
	for seedIdx := range bodies {
		resolvedColumns := make(map[string]bool)
		for _, col := range bodies[seedIdx].Columns() {
			resolvedColumns[col] = true
		}
		remainingIndices := make([]int, 0, len(bodies)-1)
		for i := range bodies {
			if i != seedIdx {
				remainingIndices = append(remainingIndices, i)
			}
		}
		joinOrder := make([]int, 0, len(remainingIndices))

		for len(remainingIndices) > 0 {
			bestIdx := -1
			bestScore := -1

			for i, idx := range remainingIndices {
				score := 0
				for _, col := range bodies[idx].Columns() {
					if resolvedColumns[col] {
						score++
					}
				}
				// Prefer bodies that share columns with already resolved set
				if score > bestScore {
					bestScore = score
					bestIdx = i
				}
			}

			if bestIdx == -1 {
				bestIdx = 0
			}

			// Move best candidate to order
			selectedBodyIdx := remainingIndices[bestIdx]
			joinOrder = append(joinOrder, selectedBodyIdx)

			// Add its columns to resolved set
			for _, col := range bodies[selectedBodyIdx].Columns() {
				resolvedColumns[col] = true
			}

			// Remove from remaining
			remainingIndices = slices.Delete(remainingIndices, bestIdx, bestIdx+1)
		}
		result.joinPlans[seedIdx] = joinOrder
	}

	return result
}

func (jr *Joining) Columns() []string {
	return jr.columns
}

func (jr *Joining) Fields() map[string]ColumnSpec {
	result := make(map[string]ColumnSpec)

	// Iterate through all bodies and collect their field specs
	for _, body := range jr.bodies {
		bodyFields := body.Fields()
		for fieldName, spec := range bodyFields {
			// If field is not yet in result, add it
			if _, exists := result[fieldName]; !exists {
				result[fieldName] = spec
				continue
			}

			// If field already exists, prioritize indexed or unique fields
			existing := result[fieldName]
			// Prefer the spec with indexed or unique flag set
			if (spec.Indexed || spec.Unique) && !existing.Indexed && !existing.Unique {
				result[fieldName] = spec
			}
			// If both or neither are indexed/unique, keep existing
		}
	}

	return result
}

func (jr *Joining) Project(mapping map[string]string) Selector {
	return newProjection(jr, mapping)
}

func (jr *Joining) IsRecursive() bool {
	return jr.recursive
}

func (jr *Joining) addParent(parent *queryParent) {
	jr.parentsList = append(jr.parentsList, parent)
}

func (jr *Joining) parents() []*queryParent {
	return jr.parentsList
}

func (jr *Joining) Select(ranges map[string]*BytesRange) (iter.Seq2[Row, error], error) {
	seedIdx := jr.bestBodyIndex(ranges)
	body := jr.bodies[seedIdx]
	columns := body.Columns()

	neededRanges := make(map[string]*BytesRange)
	for name, kr := range ranges {
		if slices.Contains(columns, name) {
			neededRanges[name] = kr
		}
	}

	// Build execution order
	joinOrder := jr.joinPlans[seedIdx]

	seq, err := body.Select(neededRanges)
	if err != nil {
		return nil, err
	}
	return func(yield func(Row, error) bool) {
		for item, err := range seq {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			joinedBases := make([]Row, len(jr.bodies))
			joinedBases[seedIdx] = item
			joinedItem := newJoinedRow(joinedBases, jr.firstOccurences)
			nextSeq, err := jr.join(joinedItem, ranges, joinOrder, 0)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			nextSeq(yield)
		}
	}, nil
}

func (jr *Joining) Join(bodies ...Selector) Selector {
	linkedBodies := make([]linkedSelector, len(bodies))
	for i, body := range bodies {
		linkedBodies[i] = body.(linkedSelector)
	}
	return newJoining(append(linkedBodies, jr.bodies...))
}

func (jr *Joining) bestBodyIndex(ranges map[string]*BytesRange) int {
	if len(ranges) == 0 {
		return 0
	}
	shortest := slices.MinFunc(slices.Collect(maps.Keys(ranges)), func(aKey, bKey string) int {
		a := ranges[aKey]
		b := ranges[bKey]
		return bytes.Compare(a.distance, b.distance)
	})
	return jr.firstOccurences[shortest]
}

func (jr *Joining) join(values *joinedRow, ranges map[string]*BytesRange, order []int, step int) (iter.Seq2[Row, error], error) {
	if step >= len(order) {
		return func(yield func(Row, error) bool) {
			yield(values, nil)
		}, nil
	}
	bodyIdx := order[step]

	body := jr.bodies[bodyIdx]
	columns := body.Columns()
	neededRanges := make(map[string]*BytesRange)
	for _, col := range columns {
		if val, err := values.Get(col); err == nil {
			key, err := ToKey(val)
			if err != nil {
				return nil, err
			}
			kr := NewBytesRange(key, key, true, true, nil)
			neededRanges[col] = kr
		}
	}
	// Add external ranges that apply to this selectable
	for name, kr := range ranges {
		if slices.Contains(columns, name) {
			if r, exists := neededRanges[name]; !exists {
				neededRanges[name] = kr
			} else {
				changed := false
				if r.start == nil || (kr.start != nil && bytes.Compare(kr.start, r.start) > 0) {
					r.start = kr.start
					r.includeStart = kr.includeStart
					changed = true
				}
				if r.end == nil || (kr.end != nil && bytes.Compare(kr.end, r.end) < 0) {
					r.end = kr.end
					r.includeEnd = kr.includeEnd
					changed = true
				}
				if changed {
					r.distance = r.computeDistance()
				}
			}
		}
	}

	iterEntries, err := body.Select(neededRanges)
	if err != nil {
		return nil, err
	}
	return func(yield func(Row, error) bool) {
		for en, err := range iterEntries {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			combinedBases := slices.Clone(values.bases)
			combinedBases[bodyIdx] = en
			combined := newJoinedRow(combinedBases, jr.firstOccurences)
			nextSeq, err := jr.join(combined, ranges, order, step+1)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			nextSeq(yield)
		}
	}, nil
}

type joinedRow struct {
	bases           []Row
	firstOccurences map[string]int
}

func newJoinedRow(bases []Row, firstOccurences map[string]int) *joinedRow {
	return &joinedRow{
		bases:           bases,
		firstOccurences: firstOccurences,
	}
}

func (jr *joinedRow) Get(field string) (any, error) {
	bodyIdx, ok := jr.firstOccurences[field]
	if !ok {
		return nil, ErrFieldNotFound(field)
	}
	if jr.bases[bodyIdx] != nil {
		return jr.bases[bodyIdx].Get(field)
	}
	// Fallback: If the canonical body is nil (e.g. during intermediate join steps),
	// check other bodies that might contain this field.
	for i, base := range jr.bases {
		if i == bodyIdx || base == nil {
			continue
		}
		if val, err := base.Get(field); err == nil {
			return val, nil
		}
	}
	return nil, ErrFieldNotFound(field)
}

func (jr *joinedRow) ToMap() (map[string]any, error) {
	result := make(map[string]any)
	for name, bodyIdx := range jr.firstOccurences {
		val, err := jr.bases[bodyIdx].Get(name)
		if err != nil {
			return nil, err
		}
		result[name] = val
	}
	return result, nil
}
