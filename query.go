package thunder

import (
	"fmt"
	"iter"
	"maps"
	"slices"
	"sort"
	"strings"
)

type Query struct {
	tx       *Tx
	bodies   []*queryBody
	backing  *Persistent
	explored map[string]bool
	parent   []*queryParent
	columns  []string
	name     string
}

func newQuery(tx *Tx, name string, columns []string, recursive bool) (*Query, error) {
	var backing *Persistent
	var err error
	if recursive {
		fields := make(map[string]ColumnSpec)
		for _, col := range columns {
			fields[col] = ColumnSpec{}
		}
		fields[queryAllUniqueCol] = ColumnSpec{
			Unique:        true,
			ReferenceCols: columns,
		}
		backing, err = tx.CreatePersistent(
			fmt.Sprintf("query_backing_%s_%d", name, tx.ID()),
			fields,
		)
		if err != nil {
			return nil, err
		}
	}
	return &Query{
		tx:       tx,
		bodies:   make([]*queryBody, 0),
		backing:  backing,
		explored: make(map[string]bool),
		parent:   make([]*queryParent, 0),
		columns:  columns,
		name:     name,
	}, nil
}

const queryAllUniqueCol = "__all_unique"

type QueryProjection struct {
	Projection
	parent []*queryParent
}

type queryParent struct {
	body  any
	index int
}

type queryBody struct {
	body      []Selector
	head      *Query
	headIndex int
}

func (q *Query) AddBody(body ...Selector) error {
	columnsSet := make(map[string]struct{})
	for _, b := range body {
		for _, col := range b.Columns() {
			columnsSet[col] = struct{}{}
		}
	}
	// Check if all query columns are present in the body
	for _, col := range q.columns {
		if _, ok := columnsSet[col]; !ok {
			return ErrBodyMissingColumn(col)
		}
	}
	newBody := &queryBody{
		body:      body,
		head:      q,
		headIndex: len(q.bodies),
	}
	q.bodies = append(q.bodies, newBody)

	// Register this body as a parent for any Query or QueryProjection children
	for i, sel := range body {
		switch s := sel.(type) {
		case *Query:
			s.parent = append(s.parent, &queryParent{
				body:  newBody,
				index: i,
			})
		case *QueryProjection:
			s.parent = append(s.parent, &queryParent{
				body:  newBody,
				index: i,
			})
		}
	}

	return nil
}

func (q *Query) Name() string {
	return q.name
}

func (q *Query) Columns() []string {
	return slices.Clone(q.columns)
}

// Project creates a new Projection that wraps this Query.
// The projection will act as a parent to this Query, receiving updates when this Query produces data.
func (q *Query) Project(mapping map[string]string) (Selector, error) {
	pp, err := newProjection(q, mapping)
	if err != nil {
		return nil, err
	}
	proj := &QueryProjection{
		Projection: *pp,
		parent:     make([]*queryParent, 0),
	}
	q.parent = append(q.parent, &queryParent{
		body:  proj,
		index: 0,
	})
	return proj, nil
}

func (q *Query) Select(ops ...Op) (iter.Seq2[map[string]any, error], error) {
	if q.backing != nil {
		err := q.explore(ops...)
		if err != nil {
			return nil, err
		}
		return q.backing.Select(ops...)
	}
	return func(yield func(map[string]any, error) bool) {
		for _, body := range q.bodies {
			iterEntries, err := body.join(0, map[string]any{}, -1, ops)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			iterEntries(yield)
		}
	}, nil
}

func (q *Query) explore(ops ...Op) error {
	stack := []any{downStackItem{part: q, ops: ops, requireUp: q.backing != nil}}
	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch v := item.(type) {
		case downStackItem:
			switch part := v.part.(type) {
			case *Query:
				key := hashOperators(v.ops...)
				// no need to explore again or there is no backing (meaning the query is not recursive)
				if part.backing == nil || part.explored[key] {
					continue
				}
				part.explored[key] = true
				for i := range part.bodies {
					stack = append(stack, downStackItem{part: part.bodies[i], ops: v.ops, requireUp: v.requireUp})
				}
			case *queryBody:
				// only first part is needed, because joins are handled in upStackItem
				if len(part.body) == 0 {
					continue
				}
				partBody := part.body[0]
				matchedOps := make([]Op, 0, len(v.ops))
				for _, op := range v.ops {
					if slices.Contains(partBody.Columns(), op.Field) {
						matchedOps = append(matchedOps, op)
					}
				}
				switch node := partBody.(type) {
				case *Query:
					stack = append(stack, downStackItem{part: node, ops: matchedOps, requireUp: node.backing != nil || v.requireUp})
				case *QueryProjection:
					stack = append(stack, downStackItem{part: node.base, ops: matchedOps, requireUp: v.requireUp})
				case *Persistent, *Projection:
					if !v.requireUp {
						continue
					}
					entries, err := node.Select(matchedOps...)
					if err != nil {
						return err
					}
					for e, err := range entries {
						if err != nil {
							return err
						}
						stack = append(stack, upStackItem{part: part, value: e, index: 0, ops: v.ops})
					}
				default:
					return ErrUnsupportedSelector(node)
				}
			}
		case upStackItem:
			switch part := v.part.(type) {
			case *queryBody:
				iterJoined, err := part.join(0, v.value, v.index, v.ops)
				if err != nil {
					return err
				}
				for joinedEntry, err := range iterJoined {
					if err != nil {
						return err
					}
					if part.head != nil {
						// Propagate up to the head Query
						// Filter columns to match head columns
						filteredEntry := make(map[string]any)
						for _, col := range part.head.columns {
							if val, ok := joinedEntry[col]; ok {
								filteredEntry[col] = val
							}
						}
						stack = append(stack, upStackItem{part: part.head, value: filteredEntry, index: part.headIndex, ops: v.ops})
					}
				}
			case *Query:
				if part.backing != nil {
					if err := part.backing.Insert(v.value); err != nil {
						if err.Error() == ErrUniqueConstraint(queryAllUniqueCol).Error() {
							// Ignore unique constraint violations
							continue
						}
						return err
					}
				}
				for _, parent := range part.parent {
					stack = append(stack, upStackItem{part: parent.body, value: v.value, index: parent.index, ops: v.ops})
				}
			case *QueryProjection:
				proj := part
				mappedValue := make(map[string]any)
				for fromKey, toKey := range proj.fromBase {
					if val, ok := v.value[fromKey]; ok {
						mappedValue[toKey] = val
					}
				}
				for _, parent := range proj.parent {
					stack = append(stack, upStackItem{part: parent.body, value: mappedValue, index: parent.index, ops: v.ops})
				}
			}
		}
	}
	return nil
}

type downStackItem struct {
	part      any
	ops       []Op
	requireUp bool
}

type upStackItem struct {
	part  any
	value map[string]any
	index int
	ops   []Op
}

func (qb *queryBody) join(curIdx int, e map[string]any, skipIdx int, ops []Op) (iter.Seq2[map[string]any, error], error) {
	if curIdx >= len(qb.body) {
		return func(yield func(map[string]any, error) bool) {
			yield(e, nil)
		}, nil
	}
	if curIdx == skipIdx {
		return qb.join(curIdx+1, e, skipIdx, ops)
	}
	selectable := qb.body[curIdx]
	columns := selectable.Columns()
	joinOps := make([]Op, 0, len(columns)+len(ops))
	for _, col := range columns {
		if val, ok := e[col]; ok {
			joinOps = append(joinOps, Op{
				Field: col,
				Type:  OpEq,
				Value: val,
			})
		}
	}
	// Add external ops that apply to this selectable
	for _, op := range ops {
		if slices.Contains(columns, op.Field) {
			joinOps = append(joinOps, op)
		}
	}

	iterEntries, err := selectable.Select(joinOps...)
	if err != nil {
		return nil, err
	}
	return func(yield func(map[string]any, error) bool) {
		for en, err := range iterEntries {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			merged := maps.Clone(e)
			maps.Copy(merged, en)
			iterJoined, err := qb.join(curIdx+1, merged, skipIdx, ops)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			iterJoined(yield)
		}
	}, nil
}

func hashOperators(ops ...Op) string {
	var opStrings []string
	for _, op := range ops {
		opStrings = append(opStrings, fmt.Sprintf("%s:%v:%d", op.Field, op.Value, op.Type))
	}
	sort.Strings(opStrings)
	result := strings.Join(opStrings, "|")
	return result
}
