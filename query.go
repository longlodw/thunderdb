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
	bodies   []queryBody
	backing  *Persistent
	explored map[string]bool
	parent   []*queryParent
	columns  []string
}

func newQuery(tx *Tx, columns []string) *Query {
	return &Query{
		tx:       tx,
		bodies:   make([]queryBody, 0),
		explored: make(map[string]bool),
		columns:  columns,
	}
}

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
	if len(columnsSet) != len(q.columns) {
		return fmt.Errorf("body columns do not match query columns")
	}
	for _, col := range q.columns {
		if _, ok := columnsSet[col]; !ok {
			return fmt.Errorf("body missing required column %s", col)
		}
	}
	q.bodies = append(q.bodies, queryBody{
		body:      body,
		head:      q,
		headIndex: len(q.bodies),
	})
	return nil
}

func (q *Query) Columns() []string {
	return slices.Clone(q.columns)
}

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
	if q.backing == nil {
		tempRelation := fmt.Sprintf("tmp_query_%d", q.tx.ID())
		indexes := make(map[string][]string)
		p, err := q.tx.CreatePersistent(tempRelation, q.columns, indexes)
		if err != nil {
			return nil, err
		}
		q.backing = p
	}

	err := q.explore(ops...)
	if err != nil {
		return nil, err
	}
	return q.backing.Select(ops...)
}

func (q *Query) explore(ops ...Op) error {
	stack := []any{downStackItem{part: q, ops: ops, requireUp: true}}
	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch v := item.(type) {
		case downStackItem:
			switch part := v.part.(type) {
			case *Query:
				key := hashOperators(v.ops...)
				if part.explored[key] {
					continue
				}
				part.explored[key] = true
				for i := range part.bodies {
					stack = append(stack, downStackItem{part: &part.bodies[i], ops: v.ops, requireUp: v.requireUp})
				}
			case *queryBody:
				for k, partBody := range part.body {
					// We only process the first part of the body to avoid duplicate results.
					// This assumes the first part is the "driving" table.
					if k != 0 {
						continue
					}

					matchedOps := make([]Op, 0, len(v.ops))
					for _, op := range v.ops {
						if slices.Contains(partBody.Columns(), op.Field) {
							matchedOps = append(matchedOps, op)
						}
					}
					switch node := partBody.(type) {
					case *Persistent:
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
							stack = append(stack, upStackItem{part: part, value: e, index: k})
						}
					case *Query:
						stack = append(stack, downStackItem{part: node, ops: matchedOps, requireUp: node.backing != nil || v.requireUp})
					case *QueryProjection:
						stack = append(stack, downStackItem{part: node.base, ops: matchedOps, requireUp: v.requireUp})
					default:
						return fmt.Errorf("unsupported selectable type")
					}
				}
			}
		case upStackItem:
			switch part := v.part.(type) {
			case *queryBody:
				iterJoined, err := part.join(part.headIndex, v.value, v.index)
				if err != nil {
					return err
				}
				for joinedEntry, err := range iterJoined {
					if err != nil {
						return err
					}
					if part.head != nil {
						// Propagate up to the head Query
						stack = append(stack, upStackItem{part: part.head, value: joinedEntry, index: part.headIndex})
					}
				}
			case *Query:
				if part.backing != nil {
					if err := part.backing.Insert(v.value); err != nil {
						return err
					}
				}
				for _, parent := range part.parent {
					stack = append(stack, upStackItem{part: parent.body, value: v.value, index: parent.index})
				}
			case *QueryProjection:
				mappedValue := make(map[string]any)
				for fromKey, toKey := range v.part.(*QueryProjection).fromBase {
					if val, ok := v.value[fromKey]; ok {
						mappedValue[toKey] = val
					}
				}
				stack = append(stack, upStackItem{part: v.part.(*QueryProjection).base, value: mappedValue, index: 0})
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
}

func (qb *queryBody) join(curIdx int, e map[string]any, skipIdx int) (iter.Seq2[map[string]any, error], error) {
	if curIdx >= len(qb.body) {
		return func(yield func(map[string]any, error) bool) {
			yield(e, nil)
		}, nil
	}
	if curIdx == skipIdx {
		return qb.join(curIdx+1, e, skipIdx)
	}
	selectable := qb.body[curIdx]
	columns := selectable.Columns()
	ops := make([]Op, 0, len(columns))
	for _, col := range columns {
		if val, ok := e[col]; ok {
			ops = append(ops, Op{
				Field: col,
				Type:  OpEq,
				Value: val,
			})
		}
	}

	iterEntries, err := selectable.Select(ops...)
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
			iterJoined, err := qb.join(curIdx+1, merged, skipIdx)
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
