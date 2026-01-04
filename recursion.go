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
	r.callCount++
	defer func() {
		r.callCount--
		if r.callCount == 0 {
			r.explored = make(map[string]bool)
		}
	}()
	hashKey := hashOperators(ranges, refRanges)
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
			if err := r.propagateUp(row, r, ranges, refRanges); err != nil {
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

				entries, err := p.join(joinedValues, nil, nil, 0, idx)
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
				if p != r {
					stack = append(stack, upStackItem{
						selector: p,
						value:    top.value,
					})
					continue
				}
				matched, err := r.backing.matchBytesRanges(top.value, ranges, "")
				if err != nil {
					return err
				}
				if !matched {
					continue
				}
				matched, err = r.backing.matchRefRanges(top.value, refRanges)
				if err != nil {
					return err
				}
				if !matched {
					continue
				}
				valMap, err := top.value.ToMap()
				if err != nil {
					return err
				}
				if err := r.backing.Insert(valMap); err != nil {
					if thunderErr, ok := err.(*ThunderError); ok && thunderErr.Code == ErrCodeUniqueConstraint {
						continue
					}
					return err
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

func hashOperators(ranges map[string]*BytesRange, refRange map[string][]*RefRange) string {
	var opStrings []string
	for name, rangeObj := range ranges {
		opStrings = append(opStrings, fmt.Sprintf("%s:%v", name, rangeObj))
	}
	for name, rangeObj := range refRange {
		refs := make([]string, len(rangeObj))
		for i, rr := range rangeObj {
			refs[i] = fmt.Sprintf("%v", rr)
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
