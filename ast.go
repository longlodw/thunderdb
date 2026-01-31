package thunderdb

import "bytes"

// Query is the interface for all query types in ThunderDB. It provides methods
// for building complex queries through projections and joins.
//
// Query implementations include:
//   - StoredQuery: represents a stored relation
//   - ProjectedQuery: represents a projection (column selection/reordering)
//   - JoinedQuery: represents a join between two queries
//   - Closure: represents a recursive/datalog query
type Query interface {
	// Project creates a new query that selects and reorders columns.
	// The cols parameter specifies which columns to include by their indices.
	// Columns can be duplicated or reordered.
	Project(cols ...int) (Query, error)

	// Join creates a new query that joins this query with another.
	// The conditions specify how rows from the two queries should be matched.
	// The resulting query has columns from both queries concatenated
	// (left columns first, then right columns).
	Join(other Query, conditions ...JoinOn) (Query, error)

	// Metadata returns the query's metadata including column count and indexes.
	Metadata() *Metadata
}

// Closure represents a recursive (datalog-style) query. It allows defining
// queries that reference themselves, enabling traversal of hierarchical data
// structures like trees or graphs.
//
// A Closure must be bound to one or more body queries using ClosedUnder() before
// it can be executed with Select().
//
// Example (finding all descendants in an org chart):
//
//	// Create recursive query with 2 columns: ancestor, descendant
//	qPath, _ := thunderdb.NewClosure(2, []thunderdb.IndexInfo{
//	    {ReferencedCols: []int{0}, IsUnique: false},
//	})
//
//	// Base case: direct reports
//	baseProj, _ := employees.Project(2, 0) // manager_id, id
//
//	// Recursive case: join employees with path
//	joined, _ := employees.Join(qPath, thunderdb.JoinOn{
//	    LeftField: 0, RightField: 0, Operator: thunderdb.EQ,
//	})
//	recursiveProj, _ := joined.Project(2, 4)
//
//	// Bind both branches
//	qPath.ClosedUnder(baseProj, recursiveProj)
type Closure struct {
	bodies   []Query
	metadata Metadata
}

// NewClosure creates a new recursive query with the specified number of
// columns and index specifications. The query must be bound to body queries
// using ClosedUnder() before execution.
//
// The maximum number of columns is 64.
func NewClosure(colsCount int, indexInfos []IndexInfo) (*Closure, error) {
	result := &Closure{}
	if err := initStoredMetadata(&result.metadata, colsCount, indexInfos); err != nil {
		return nil, err
	}
	return result, nil
}

// Project creates a projected query selecting the specified columns.
func (h *Closure) Project(cols ...int) (Query, error) {
	return newProjectedQuery(h, cols)
}

// Join creates a joined query with another query.
func (h *Closure) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(h, other, conditions)
}

// Metadata returns the query's metadata.
func (h *Closure) Metadata() *Metadata {
	return &h.metadata
}

// ClosedUnder associates one or more body queries with this recursive query.
// All body queries must have the same number of columns as the Closure.
// At least one body should be non-recursive (base case) to ensure termination.
func (h *Closure) ClosedUnder(bodies ...Query) error {
	for _, body := range bodies {
		if body.Metadata().ColumnsCount != h.metadata.ColumnsCount {
			return ErrFieldCountMismatch(h.Metadata().ColumnsCount, body.Metadata().ColumnsCount)
		}
	}
	h.bodies = bodies
	return nil
}

// ProjectedQuery represents a query with column projection (selection/reordering).
// It wraps another query and transforms its output by selecting specific columns.
type ProjectedQuery struct {
	child    Query
	cols     []int
	metadata Metadata
}

func newProjectedQuery(
	child Query,
	cols []int,
) (*ProjectedQuery, error) {
	result := &ProjectedQuery{
		child: child,
		cols:  cols,
	}
	if err := initProjectedMetadata(&result.metadata, child.Metadata(), cols); err != nil {
		return nil, err
	}
	return result, nil
}

// Project creates a projected query selecting the specified columns.
func (ph *ProjectedQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(ph, cols)
}

// Join creates a joined query with another query.
func (ph *ProjectedQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(ph, other, conditions)
}

// Metadata returns the query's metadata.
func (ph *ProjectedQuery) Metadata() *Metadata {
	return &ph.metadata
}

// JoinedQuery represents a join between two queries. The resulting columns
// are the concatenation of the left query's columns followed by the right
// query's columns.
type JoinedQuery struct {
	left       Query
	right      Query
	conditions []JoinOn
	metadata   Metadata
}

func newJoinedQuery(
	left Query,
	right Query,
	conditions []JoinOn,
) (*JoinedQuery, error) {
	result := &JoinedQuery{
		left:       left,
		right:      right,
		conditions: conditions,
	}
	if err := initJoinedMetadata(&result.metadata, left.Metadata(), right.Metadata()); err != nil {
		return nil, err
	}
	return result, nil
}

// Project creates a projected query selecting the specified columns.
func (jh *JoinedQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(jh, cols)
}

// Join creates a joined query with another query.
func (jh *JoinedQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(jh, other, conditions)
}

// Metadata returns the query's metadata.
func (jh *JoinedQuery) Metadata() *Metadata {
	return &jh.metadata
}

// Op represents a comparison operator used in conditions and joins.
type Op int

const (
	// EQ tests for equality (==)
	EQ = Op(iota)
	// NEQ tests for inequality (!=)
	NEQ
	// LT tests for less than (<)
	LT
	// LTE tests for less than or equal (<=)
	LTE
	// GT tests for greater than (>)
	GT
	// GTE tests for greater than or equal (>=)
	GTE
)

// JoinOn specifies a join condition between two queries. It defines how
// columns from the left and right queries should be compared.
//
// Example:
//
//	// Join users and orders where users.id = orders.user_id
//	joined, _ := users.Join(orders, thunderdb.JoinOn{
//	    LeftField:  0,  // users.id (column 0 of left query)
//	    RightField: 2,  // orders.user_id (column 2 of right query)
//	    Operator:   thunderdb.EQ,
//	})
type JoinOn struct {
	LeftField  int // Column index in the left query
	RightField int // Column index in the right query
	Operator   Op  // Comparison operator
}

// StoredQuery represents a query backed by a persistent storage relation.
// It is created using Tx.StoredQuery() and can be used as a base for building
// more complex queries with projections and joins.
type StoredQuery struct {
	storageName string
	metadata    Metadata
}

// Project creates a projected query selecting the specified columns.
func (ph *StoredQuery) Project(cols ...int) (Query, error) {
	return newProjectedQuery(ph, cols)
}

// Join creates a joined query with another query.
func (ph *StoredQuery) Join(other Query, conditions ...JoinOn) (Query, error) {
	return newJoinedQuery(ph, other, conditions)
}

// Metadata returns the query's metadata.
func (ph *StoredQuery) Metadata() *Metadata {
	return &ph.metadata
}

// IndexInfo specifies an index to be created on a storage relation.
// Indexes improve query performance for filtered selects and enforce
// uniqueness constraints when IsUnique is true.
type IndexInfo struct {
	// ReferencedCols lists the column indices that make up this index.
	// For a single-column index, this contains one element.
	// For a composite index, this contains multiple elements in order.
	ReferencedCols []int

	// IsUnique indicates whether this index enforces a uniqueness constraint.
	// If true, no two rows can have the same values for the indexed columns.
	IsUnique bool
}

// Condition specifies a filter condition for Select, Delete, or Update operations.
// Multiple conditions are combined with AND logic.
//
// Example:
//
//	// Find users where role = "admin" AND age >= 18
//	results, _ := tx.Select(users,
//	    thunderdb.Condition{Field: 2, Operator: thunderdb.EQ, Value: "admin"},
//	    thunderdb.Condition{Field: 3, Operator: thunderdb.GTE, Value: 18},
//	)
type Condition struct {
	Field    int // Column index to filter on
	Operator Op  // Comparison operator
	Value    any // Value to compare against
}

func parseConditions(conditions []Condition) (equals map[int]*Value, ranges map[int]*Range, exclusion map[int][]*Value, possible bool, err error) {
	equals = make(map[int]*Value)
	ranges = make(map[int]*Range)
	exclusion = make(map[int][]*Value)
	possible = true
	equalsBytes := make(map[int][]byte)
	exclusionBytes := make(map[int]map[string]bool)

	for _, cond := range conditions {
		val := ValueOfLiteral(cond.Value)
		valBytes, err := val.GetRaw()
		if err != nil {
			return nil, nil, nil, false, err
		}
		switch cond.Operator {
		case EQ:
			if existing, exists := equalsBytes[cond.Field]; exists {
				if !bytes.Equal(existing, valBytes) {
					possible = false
					return nil, nil, nil, false, nil
				}
			} else {
				equalsBytes[cond.Field] = valBytes
			}
			if rng, exists := ranges[cond.Field]; exists {
				inRange, err := rng.Contains(val)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				delete(ranges, cond.Field)
			}
			if exclusions, exists := exclusionBytes[cond.Field]; exists {
				if exclusions[string(valBytes)] {
					possible = false
					return nil, nil, nil, false, nil
				}
			}
			equals[cond.Field] = val
		case NEQ:
			if existing, exists := equalsBytes[cond.Field]; exists {
				if bytes.Equal(existing, valBytes) {
					possible = false
					return nil, nil, nil, false, nil
				}
			}
			if _, exists := exclusionBytes[cond.Field]; !exists {
				exclusionBytes[cond.Field] = make(map[string]bool)
				exclusion[cond.Field] = append(exclusion[cond.Field], val)
			}
		case LT:
			curRange, err := NewRangeFromValue(nil, val, false, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case LTE:
			curRange, err := NewRangeFromValue(nil, val, false, true)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case GT:
			curRange, err := NewRangeFromValue(val, nil, false, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case GTE:
			curRange, err := NewRangeFromValue(val, nil, true, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Field]; exists {
				inRange, err := curRange.Contains(eqVal)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				continue
			}
			rng, exists := ranges[cond.Field]
			if !exists {
				ranges[cond.Field] = curRange
			} else {
				ranges[cond.Field], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		}
	}
	return
}
