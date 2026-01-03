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

func (r *Recursion) Select(ranges map[string]*BytesRange, refRanges map[string]*RefRange) (iter.Seq2[Row, error], error) {
	if err := r.explore(ranges, refRanges); err != nil {
		return nil, err
	}
	return r.backing.Select(ranges, refRanges)
}

func (r *Recursion) explore(ranges map[string]*BytesRange, refRanges map[string]*RefRange) error {
	stack := []any{&downStack{
		ranges:    ranges,
		refRanges: refRanges,
		selector:  r,
	}}
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch v := top.(type) {
		case *downStack:
			// Handle down stack logic
			if !v.selector.IsRecursive() {
				entries, err := v.selector.Select(v.ranges, v.refRanges)
				if err != nil {
					return err
				}
				for e, err := range entries {
					if err != nil {
						return err
					}
					stack = append(stack, &upStack{
						selector:  v.selector,
						value:     e,
						ranges:    v.ranges,
						refRanges: v.refRanges,
					})
				}
				continue
			}
			switch sel := v.selector.(type) {
			case *Recursion:
				key := hashOperators(v.ranges, v.refRanges)
				if sel.explored[key] {
					continue
				}
				sel.explored[key] = true
				for _, branch := range sel.branches {
					stack = append(stack, &downStack{
						ranges:    v.ranges,
						selector:  branch,
						refRanges: v.refRanges,
					})
				}
			case *Joining:
				bestIdx := sel.bestBodyIndex(v.ranges)
				stack = append(stack, &downStack{
					ranges:    v.ranges,
					selector:  sel.bodies[bestIdx],
					refRanges: v.refRanges,
				})
			case *Projection:
				baseRanges := make(map[string]*BytesRange)
				for projField, kr := range v.ranges {
					baseField, ok := sel.toBase[projField]
					if !ok {
						return ErrFieldNotFound(projField)
					}
					baseRanges[baseField] = kr
				}
				baseRangesRef := make(map[string]*RefRange)
				for projField, rr := range v.refRanges {
					baseField, ok := sel.toBase[projField]
					if !ok {
						return ErrFieldNotFound(projField)
					}
					baseRangesRef[baseField] = rr
				}
				stack = append(stack, &downStack{
					ranges:    baseRanges,
					selector:  sel.base,
					refRanges: baseRangesRef,
				})
			default:
				return ErrUnsupportedSelector()
			}
		case *upStack:
			// Handle up stack logic
			for _, parent := range v.selector.parents() {
				if !r.interestedSet[parent.parent] {
					continue
				}
				idx := parent.index
				switch p := parent.parent.(type) {
				case *Joining:
					// Handle joining parent
					joinedBases := make([]Row, len(p.bodies))
					joinedBases[idx] = v.value
					joinedValues := newJoinedRow(joinedBases, p.firstOccurences)
					entries, err := p.join(joinedValues, v.ranges, v.refRanges, 0, idx)
					if err != nil {
						return err
					}
					for e, err := range entries {
						if err != nil {
							return err
						}
						stack = append(stack, &upStack{
							selector:  p,
							value:     e,
							ranges:    v.ranges,
							refRanges: v.refRanges,
						})
					}
				case *Projection:
					// Handle projection parent
					projValue := newProjectedRow(v.value, p.toBase)
					stack = append(stack, &upStack{
						selector:  p,
						value:     projValue,
						ranges:    v.ranges,
						refRanges: v.refRanges,
					})
				case *Recursion:
					// Handle recursive parent
					valMap, err := v.value.ToMap()
					if err != nil {
						return err
					}
					if err := p.backing.Insert(valMap); err != nil {
						if thunderErr, ok := err.(*ThunderError); ok && thunderErr.Code == ErrCodeUniqueConstraint {
							continue
						}
						return err
					}
					stack = append(stack, &upStack{
						selector:  p,
						value:     v.value,
						ranges:    v.ranges,
						refRanges: v.refRanges,
					})
				default:
					return ErrUnsupportedSelector()
				}
			}
		}
	}
	return nil
}

func hashOperators(ranges map[string]*BytesRange, refRange map[string]*RefRange) string {
	var opStrings []string
	for name, rangeObj := range ranges {
		opStrings = append(opStrings, fmt.Sprintf("%s:%v", name, rangeObj))
	}
	for name, rangeObj := range refRange {
		opStrings = append(opStrings, fmt.Sprintf("%s:%v", name, rangeObj))
	}
	sort.Strings(opStrings)
	result := strings.Join(opStrings, "|")
	return result
}

type downStack struct {
	ranges    map[string]*BytesRange
	selector  linkedSelector
	refRanges map[string]*RefRange
}

type upStack struct {
	selector  linkedSelector
	value     Row
	ranges    map[string]*BytesRange
	refRanges map[string]*RefRange
}

const queryAllUniqueCol = "__all_unique"
