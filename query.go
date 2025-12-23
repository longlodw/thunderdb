package thunder

import (
	"bytes"
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
			fields[col] = ColumnSpec{
				Indexed: true,
			}
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
	return q.columns
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

func (q *Query) Select(ranges map[string]*keyRange) (iter.Seq2[map[string]any, error], error) {
	if q.backing != nil {
		err := q.explore(ranges)
		if err != nil {
			return nil, err
		}
		return q.backing.Select(ranges)
	}
	return func(yield func(map[string]any, error) bool) {
		for _, body := range q.bodies {
			iterEntries, err := body.join(0, map[string]any{}, -1, ranges)
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

func (q *Query) explore(ranges map[string]*keyRange) error {
	stack := []any{downStackItem{part: q, ranges: ranges, requireUp: q.backing != nil}}
	for len(stack) > 0 {
		item := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch v := item.(type) {
		case downStackItem:
			switch part := v.part.(type) {
			case *Query:
				key := hashOperators(v.ranges)
				// no need to explore again or there is no backing (meaning the query is not recursive)
				if part.backing == nil || part.explored[key] {
					continue
				}
				part.explored[key] = true
				for i := range part.bodies {
					stack = append(stack, downStackItem{part: part.bodies[i], ranges: ranges, requireUp: v.requireUp})
				}
			case *queryBody:
				// only first part is needed, because joins are handled in upStackItem
				if len(part.body) == 0 {
					continue
				}
				partBody := part.body[0]
				matchedRanges := make(map[string]*keyRange)
				for name, rangeObj := range v.ranges {
					if slices.Contains(partBody.Columns(), name) {
						matchedRanges[name] = rangeObj
					}
				}
				switch node := partBody.(type) {
				case *Query:
					stack = append(stack, downStackItem{part: node, ranges: matchedRanges, requireUp: node.backing != nil || v.requireUp})
				case *QueryProjection:
					stack = append(stack, downStackItem{part: node.base, ranges: matchedRanges, requireUp: v.requireUp})
				case *Persistent, *Projection:
					if !v.requireUp {
						continue
					}
					entries, err := node.Select(matchedRanges)
					if err != nil {
						return err
					}
					for e, err := range entries {
						if err != nil {
							return err
						}
						stack = append(stack, upStackItem{part: part, value: e, index: 0, ranges: v.ranges})
					}
				default:
					return ErrUnsupportedSelector(node)
				}
			}
		case upStackItem:
			switch part := v.part.(type) {
			case *queryBody:
				iterJoined, err := part.join(0, v.value, v.index, v.ranges)
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
						stack = append(stack, upStackItem{part: part.head, value: filteredEntry, index: part.headIndex, ranges: v.ranges})
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
					stack = append(stack, upStackItem{part: parent.body, value: v.value, index: parent.index, ranges: v.ranges})
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
					stack = append(stack, upStackItem{part: parent.body, value: mappedValue, index: parent.index, ranges: v.ranges})
				}
			}
		}
	}
	return nil
}

type downStackItem struct {
	part      any
	ranges    map[string]*keyRange
	requireUp bool
}

type upStackItem struct {
	part   any
	value  map[string]any
	index  int
	ranges map[string]*keyRange
}

func (qb *queryBody) join(curIdx int, e map[string]any, skipIdx int, ranges map[string]*keyRange) (iter.Seq2[map[string]any, error], error) {
	if curIdx >= len(qb.body) {
		return func(yield func(map[string]any, error) bool) {
			yield(e, nil)
		}, nil
	}
	if curIdx == skipIdx {
		return qb.join(curIdx+1, e, skipIdx, ranges)
	}
	selectable := qb.body[curIdx]
	columns := selectable.Columns()
	joinedRanges := make(map[string]*keyRange, len(columns)+len(ranges))
	for _, col := range columns {
		if val, ok := e[col]; ok {
			key, err := orderedMa.Marshal([]any{val})
			if err != nil {
				return nil, err
			}
			kr := &keyRange{
				includeStart: true,
				includeEnd:   true,
				startKey:     key,
				endKey:       key,
			}
			joinedRanges[col] = kr
		}
	}
	// Add external ranges that apply to this selectable
	for name, kr := range ranges {
		if slices.Contains(columns, name) {
			if r, exists := joinedRanges[name]; !exists {
				joinedRanges[name] = kr
			} else {
				if r.startKey == nil || (kr.startKey != nil && bytes.Compare(kr.startKey, r.startKey) > 0) {
					r.startKey = kr.startKey
					r.includeStart = kr.includeStart
				}
				if r.endKey == nil || (kr.endKey != nil && bytes.Compare(kr.endKey, r.endKey) < 0) {
					r.endKey = kr.endKey
					r.includeEnd = kr.includeEnd
				}
			}
		}
	}

	iterEntries, err := selectable.Select(joinedRanges)
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
			joinedEntry := maps.Clone(e)
			maps.Copy(joinedEntry, en)
			iterJoined, err := qb.join(curIdx+1, joinedEntry, skipIdx, ranges)
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

func hashOperators(ranges map[string]*keyRange) string {
	var opStrings []string
	for name, rangeObj := range ranges {
		opStrings = append(opStrings, fmt.Sprintf("%s:%v", name, rangeObj))
	}
	sort.Strings(opStrings)
	result := strings.Join(opStrings, "|")
	return result
}
