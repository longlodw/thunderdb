package thunderdb

import (
	"fmt"
	"iter"
	"slices"
	"sort"
	"strings"
)

type Recursion struct {
	branches      []linkedSelector
	columns       []string
	parentsList   []*queryParent
	explored      map[string]bool
	backing       *Persistent
	interestedSet map[linkedSelector]bool
	callCount     int
	maxDepth      int
}

func newRecursive(tx *Tx, relation string, columnSpecs map[string]ColumnSpec) (*Recursion, error) {
	fields := make(map[string]ColumnSpec)
	columns := make([]string, 0, len(columnSpecs))
	for colName, colSpec := range columnSpecs {
		fields[colName] = colSpec
		columns = append(columns, colName)
	}
	fields[queryAllUniqueCol] = ColumnSpec{
		Unique:        true,
		ReferenceCols: columns,
	}
	backing, err := newPersistent(tx, relation, fields, true)
	if err != nil {
		return nil, err
	}
	interestedSet := make(map[linkedSelector]bool)
	result := &Recursion{
		branches:      make([]linkedSelector, 0),
		columns:       columns,
		explored:      make(map[string]bool),
		backing:       backing,
		interestedSet: interestedSet,
		maxDepth:      50, // Default depth limit
	}
	interestedSet[result] = true
	return result, nil
}

func (r *Recursion) Columns() []string {
	return r.columns
}

func (r *Recursion) IsRecursive() bool {
	return true
}

func (r *Recursion) SetMaxDepth(depth int) {
	r.maxDepth = depth
}

func (r *Recursion) addParent(parent *queryParent) {
	r.parentsList = append(r.parentsList, parent)
}

func (r *Recursion) parents() []*queryParent {
	return r.parentsList
}

func (r *Recursion) Project(mapping map[string]string) Selector {
	return newProjection(r, mapping)
}

func (r *Recursion) Join(bodies ...Selector) Selector {
	linkedBodies := make([]linkedSelector, len(bodies)+1)
	linkedBodies[0] = r
	for i, body := range bodies {
		linkedBodies[i+1] = body.(linkedSelector)
	}
	return newJoining(linkedBodies)
}

func (r *Recursion) AddBranch(branch Selector) error {
	if len(branch.Columns()) != len(r.columns) {
		return ErrFieldCountMismatch(len(r.columns), len(branch.Columns()))
	}
	for _, col := range r.columns {
		if !slices.Contains(branch.Columns(), col) {
			return ErrFieldNotFound(col)
		}
	}
	if ls, ok := branch.(linkedSelector); !ok {
		return ErrUnsupportedSelector()
	} else {
		r.branches = append(r.branches, ls)
		ls.addParent(&queryParent{
			parent: r,
		})
	}
	stack := []linkedSelector{branch.(linkedSelector)}
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if r.interestedSet[top] {
			continue
		}
		r.interestedSet[top] = true
		switch v := top.(type) {
		case *Projection:
			stack = append(stack, v.base)
		case *Joining:
			for _, body := range v.bodies {
				stack = append(stack, body)
			}
		case *Recursion:
			for _, branch := range v.branches {
				stack = append(stack, branch)
			}
		}
	}
	return nil
}

func (r *Recursion) Select(ranges map[string]*BytesRange, refRanges map[string][]*RefRange) (iter.Seq2[Row, error], error) {
	if err := r.explore(ranges, refRanges); err != nil {
		return nil, err
	}
	return r.backing.Select(ranges, refRanges)
}

func (r *Recursion) explore(ranges map[string]*BytesRange, refRanges map[string][]*RefRange) error {
	if r.callCount > r.maxDepth {
		return ErrRecursionDepthExceeded(r.maxDepth)
	}
	r.callCount++
	defer func() {
		r.callCount--
		if r.callCount == 0 {
			r.explored = make(map[string]bool)
		}
	}()
	hashKey := hashOperators(ranges, refRanges)
	// fmt.Printf("Explore: %s (callCount=%d)\n", hashKey, r.callCount)
	if r.explored[hashKey] {
		return nil
	}
	r.explored[hashKey] = true
	for _, branch := range r.branches {
		seq, err := branch.Select(ranges, refRanges)
		if err != nil {
			return err
		}
		for row, err := range seq {
			if err != nil {
				return err
			}
			valMap, err := row.ToMap()
			if err != nil {
				return err
			}
			if err := r.backing.Insert(valMap); err != nil {
				if thunderErr, ok := err.(*ThunderError); ok && thunderErr.Code == ErrCodeUniqueConstraint {
					continue
				}
				return err
			}
			if err := r.propagateUp(row, branch, ranges, refRanges); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Recursion) propagateUp(row Row, selector linkedSelector, ranges map[string]*BytesRange, refRanges map[string][]*RefRange) error {
	stack := make([]upStackItem, 0)
	stack = append(stack, upStackItem{
		selector: selector,
		value:    row,
	})
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, parent := range top.selector.parents() {
			if !r.interestedSet[parent.parent] {
				continue
			}
			idx := parent.index
			switch p := parent.parent.(type) {
			case *Joining:
				// Handle joining parent
				joinedBases := make([]Row, len(p.bodies))
				joinedBases[idx] = top.value
				joinedValues := newJoinedRow(joinedBases, p.firstOccurences)

				// Optimization: Check if the seed row satisfies the global ranges.
				// If the seed itself violates the constraints, we can skip the entire join.

				// Calculate join order for propagateUp
				// Since we are propagating UP, we already have the value at 'idx'.
				// We need to join with all OTHER bodies to complete the row.
				// This is essentially the same as Select logic: treat 'idx' as the seed.
				joinOrder := p.joinPlans[idx]

				// Use global ranges, may results in bugs where correct rows are filtered out
				entries, err := p.join(joinedValues, ranges, refRanges, joinOrder, 0)
				if err != nil {
					return err
				}
				for e, err := range entries {
					if err != nil {
						return err
					}
					stack = append(stack, upStackItem{
						selector: p,
						value:    e,
					})
				}
			case *Projection:
				// Handle projection parent
				projValue := newProjectedRow(top.value, p.toBase)
				stack = append(stack, upStackItem{
					selector: p,
					value:    projValue,
				})
			case *Recursion:
				// Handle recursive parent
				if r == p {
					matched, err := r.checkBeforeInsert(top.value, ranges, refRanges)
					if err != nil {
						return err
					}
					if !matched {
						continue
					}
				}
				stack = append(stack, upStackItem{
					selector: p,
					value:    top.value,
				})
			default:
				return ErrUnsupportedSelector()
			}
		}
	}
	return nil
}

func (r *Recursion) checkBeforeInsert(row Row, ranges map[string]*BytesRange, refRanges map[string][]*RefRange) (bool, error) {
	matched, err := r.backing.matchBytesRanges(row, ranges, "")
	if err != nil {
		return false, err
	}
	if !matched {
		return false, nil
	}
	matched, err = r.backing.matchRefRanges(row, refRanges)
	if err != nil {
		return false, err
	}
	if !matched {
		return false, nil
	}
	mapRow, err := row.ToMap()
	if err != nil {
		return false, err
	}
	if err := r.backing.Insert(mapRow); err != nil {
		if thunderErr, ok := err.(*ThunderError); ok && thunderErr.Code == ErrCodeUniqueConstraint {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func hashOperators(ranges map[string]*BytesRange, refRange map[string][]*RefRange) string {
	var opStrings []string
	for name, rangeObj := range ranges {
		excludes := make([]string, len(rangeObj.excludes))
		for i, ex := range rangeObj.excludes {
			excludes[i] = string(ex)
		}
		slices.Sort(excludes)
		opStrings = append(opStrings, fmt.Sprintf("%s:%v %v %v %v %v", name, string(rangeObj.start), string(rangeObj.end), rangeObj.includeStart, rangeObj.includeEnd, strings.Join(excludes, ",")))
	}
	for name, rangeObj := range refRange {
		refs := make([]string, len(rangeObj))
		for i, rr := range rangeObj {
			excludeStrs := make([]string, len(rr.excludes))
			for j, ex := range rr.excludes {
				excludeStrs[j] = strings.Join(ex, ",")
			}
			refs[i] = fmt.Sprintf("%v %v %v %v %v", strings.Join(rr.start, ","), strings.Join(rr.end, ","), rr.includeStart, rr.includeEnd, strings.Join(excludeStrs, "/"))
		}
		slices.Sort(refs)
		opStrings = append(opStrings, fmt.Sprintf("%s:%v", name, refs))
	}
	sort.Strings(opStrings)
	result := strings.Join(opStrings, "|")
	return result
}

type upStackItem struct {
	selector linkedSelector
	value    Row
}

const queryAllUniqueCol = "__all_unique"
