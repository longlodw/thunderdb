package thunder

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
	}
	stack := []linkedSelector{branch.(linkedSelector)}
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
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

func (r *Recursion) Select(ranges map[string]*keyRange) (iter.Seq2[map[string]any, error], error) {
	if err := r.explore(ranges); err != nil {
		return nil, err
	}
	return r.backing.Select(ranges)
}

func (r *Recursion) explore(ranges map[string]*keyRange) error {
	stack := []any{&downStack{
		ranges:   ranges,
		selector: r,
	}}
	for len(stack) > 0 {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch v := top.(type) {
		case *downStack:
			// Handle down stack logic
			if !v.selector.IsRecursive() {
				entries, err := v.selector.Select(v.ranges)
				if err != nil {
					return err
				}
				for e, err := range entries {
					if err != nil {
						return err
					}
					stack = append(stack, &upStack{
						selector: v.selector,
						value:    e,
						ranges:   v.ranges,
					})
				}
				continue
			}
			switch sel := v.selector.(type) {
			case *Recursion:
				key := hashOperators(v.ranges)
				if sel.explored[key] {
					continue
				}
				sel.explored[key] = true
				for _, branch := range sel.branches {
					stack = append(stack, &downStack{
						ranges:   v.ranges,
						selector: branch,
					})
				}
			case *Joining:
				bestIdx := sel.bestBodyIndex(v.ranges)
				stack = append(stack, &downStack{
					ranges:   v.ranges,
					selector: sel.bodies[bestIdx],
				})
			case *Projection:
				baseRanges := make(map[string]*keyRange)
				for projField, kr := range v.ranges {
					baseField, ok := sel.toBase[projField]
					if !ok {
						return ErrFieldNotFound(projField)
					}
					baseRanges[baseField] = kr
				}
				stack = append(stack, &downStack{
					ranges:   baseRanges,
					selector: sel.base,
				})
			default:
				return ErrUnsupportedSelector()
			}
		case *upStack:
			// Handle up stack logic
			for _, parent := range v.selector.parents() {
				idx := parent.index
				switch p := parent.parent.(type) {
				case *Joining:
					// Handle joining parent
					entries, err := p.join(v.value, ranges, 0, idx)
					if err != nil {
						return err
					}
					for e, err := range entries {
						if err != nil {
							return err
						}
						stack = append(stack, &upStack{
							selector: p,
							value:    e,
							ranges:   v.ranges,
						})
					}
				case *Projection:
					// Handle projection parent
					projValue := make(map[string]any)
					for projField, baseField := range p.toBase {
						vv, ok := v.value[baseField]
						if !ok {
							return ErrFieldNotFound(baseField)
						}
						projValue[projField] = vv
					}
					stack = append(stack, &upStack{
						selector: p,
						value:    projValue,
						ranges:   v.ranges,
					})
				case *Recursion:
					// Handle recursive parent
					if err := p.backing.Insert(v.value); err != nil {
						if thunderErr, ok := err.(*ThunderError); ok && thunderErr.Code == ErrCodeUniqueConstraint {
							continue
						}
						return err
					}
					stack = append(stack, &upStack{
						selector: p,
						value:    v.value,
						ranges:   v.ranges,
					})
				default:
					return ErrUnsupportedSelector()
				}
			}
		}
	}
	return nil
}

func hashOperators(ranges map[string]*keyRange) string {
	var opStrings []string
	for name, rangeObj := range ranges {
		opStrings = append(opStrings, fmt.Sprintf("%s:%v", name, rangeObj))
	}
	sort.Strings(opStrings)
	result := strings.Join(opStrings, "|")
	return result
}

type downStack struct {
	ranges   map[string]*keyRange
	selector linkedSelector
}

type upStack struct {
	selector linkedSelector
	value    map[string]any
	ranges   map[string]*keyRange
}

const queryAllUniqueCol = "__all_unique"
