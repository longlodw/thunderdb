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
	Join(other Query, conditions ...JoinCondition) (Query, error)

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
//	qPath, _ := tx.ClosureQuery(2,
//	    thunderdb.Index(0), // index on ancestor column
//	)
//
//	// Base case: direct reports
//	baseProj, _ := employees.Project(2, 0) // manager_id, id
//
//	// Recursive case: join employees with path
//	joined, _ := employees.Join(qPath,
//	    thunderdb.OnEQ(0, 0), // employees.id = path.ancestor
//	)
//	recursiveProj, _ := joined.Project(2, 4)
//
//	// Bind both branches
//	qPath.ClosedUnder(baseProj, recursiveProj)
type Closure struct {
	bodies     []Query
	metadata   Metadata
	storageIdx uint64
}

// Project creates a projected query selecting the specified columns.
func (h *Closure) Project(cols ...int) (Query, error) {
	return newProjectedQuery(h, cols)
}

// Join creates a joined query with another query.
func (h *Closure) Join(other Query, conditions ...JoinCondition) (Query, error) {
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
func (ph *ProjectedQuery) Join(other Query, conditions ...JoinCondition) (Query, error) {
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
	conditions []JoinCondition
	metadata   Metadata
}

func newJoinedQuery(
	left Query,
	right Query,
	conditions []JoinCondition,
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
func (jh *JoinedQuery) Join(other Query, conditions ...JoinCondition) (Query, error) {
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

// JoinCondition specifies a join condition between two queries. It defines how
// columns from the left and right queries should be compared.
//
// Example:
//
//	// Join users and orders where users.id = orders.user_id
//	// Using JoinCondition struct directly:
//	joined, _ := users.Join(orders, thunderdb.JoinCondition{
//	    Left:     0,  // users.id (column 0 of left query)
//	    Right:    2,  // orders.user_id (column 2 of right query)
//	    Operator: thunderdb.EQ,
//	})
//
//	// Or using helper function:
//	joined, _ = users.Join(orders,
//	    thunderdb.OnEQ(0, 2), // users.id = orders.user_id
//	)
type JoinCondition struct {
	Left     int // Column index in the left query
	Right    int // Column index in the right query
	Operator Op  // Comparison operator
}

func OnEQ(leftCol, rightCol int) JoinCondition {
	return JoinCondition{
		Left:     leftCol,
		Right:    rightCol,
		Operator: EQ,
	}
}

func OnNEQ(leftCol, rightCol int) JoinCondition {
	return JoinCondition{
		Left:     leftCol,
		Right:    rightCol,
		Operator: NEQ,
	}
}

func OnLT(leftCol, rightCol int) JoinCondition {
	return JoinCondition{
		Left:     leftCol,
		Right:    rightCol,
		Operator: LT,
	}
}

func OnLTE(leftCol, rightCol int) JoinCondition {
	return JoinCondition{
		Left:     leftCol,
		Right:    rightCol,
		Operator: LTE,
	}
}

func OnGT(leftCol, rightCol int) JoinCondition {
	return JoinCondition{
		Left:     leftCol,
		Right:    rightCol,
		Operator: GT,
	}
}

func OnGTE(leftCol, rightCol int) JoinCondition {
	return JoinCondition{
		Left:     leftCol,
		Right:    rightCol,
		Operator: GTE,
	}
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
func (ph *StoredQuery) Join(other Query, conditions ...JoinCondition) (Query, error) {
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

// Unique creates a unique index on the specified columns.
func Unique(cols ...int) IndexInfo {
	return IndexInfo{
		ReferencedCols: cols,
		IsUnique:       true,
	}
}

// Index creates a non-unique index on the specified columns.
func Index(cols ...int) IndexInfo {
	return IndexInfo{
		ReferencedCols: cols,
		IsUnique:       false,
	}
}

// IndexAllCols creates a non-unique index on all columns of the relation.
func IndexAllCols(count int) IndexInfo {
	cols := make([]int, count)
	for i := range count {
		cols[i] = i
	}
	return IndexInfo{
		ReferencedCols: cols,
		IsUnique:       false,
	}
}

// UniqueAllCols creates a unique index on all columns of the relation.
func UniqueAllCols(count int) IndexInfo {
	cols := make([]int, count)
	for i := range count {
		cols[i] = i
	}
	return IndexInfo{
		ReferencedCols: cols,
		IsUnique:       true,
	}
}

// SelectCondition specifies a filter condition for Select, Delete, or Update operations.
// Multiple conditions are combined with AND logic.
//
// Example:
//
//	// Find users where role = "admin" AND age >= 18
//	// Using SelectCondition struct directly:
//	results, _ := tx.Select(users,
//	    thunderdb.SelectCondition{Col: 2, Operator: thunderdb.EQ, Value: "admin"},
//	    thunderdb.SelectCondition{Col: 3, Operator: thunderdb.GTE, Value: 18},
//	)
//
//	// Or using helper functions:
//	results, _ = tx.Select(users,
//	    thunderdb.WhereEQ(2, "admin"),   // role = "admin"
//	    thunderdb.WhereGTE(3, 18),       // age >= 18
//	)
type SelectCondition struct {
	Col      int // Column index to filter on
	Operator Op  // Comparison operator
	Value    any // Value to compare against
}

func WhereEQ(col int, value any) SelectCondition {
	return SelectCondition{
		Col:      col,
		Operator: EQ,
		Value:    value,
	}
}

func WhereNEQ(col int, value any) SelectCondition {
	return SelectCondition{
		Col:      col,
		Operator: NEQ,
		Value:    value,
	}
}

func WhereLT(col int, value any) SelectCondition {
	return SelectCondition{
		Col:      col,
		Operator: LT,
		Value:    value,
	}
}

func WhereLTE(col int, value any) SelectCondition {
	return SelectCondition{
		Col:      col,
		Operator: LTE,
		Value:    value,
	}
}

func WhereGT(col int, value any) SelectCondition {
	return SelectCondition{
		Col:      col,
		Operator: GT,
		Value:    value,
	}
}

func WhereGTE(col int, value any) SelectCondition {
	return SelectCondition{
		Col:      col,
		Operator: GTE,
		Value:    value,
	}
}

func parseConditions(conditions []SelectCondition) (equals map[int]*Value, ranges map[int]*interval, exclusion map[int][]*Value, possible bool, err error) {
	equals = make(map[int]*Value)
	ranges = make(map[int]*interval)
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
			if existing, exists := equalsBytes[cond.Col]; exists {
				if !bytes.Equal(existing, valBytes) {
					possible = false
					return nil, nil, nil, false, nil
				}
			} else {
				equalsBytes[cond.Col] = valBytes
			}
			if rng, exists := ranges[cond.Col]; exists {
				inRange, err := rng.Contains(val)
				if err != nil {
					return nil, nil, nil, false, err
				}
				if !inRange {
					possible = false
					return nil, nil, nil, false, nil
				}
				delete(ranges, cond.Col)
			}
			if exclusions, exists := exclusionBytes[cond.Col]; exists {
				if exclusions[string(valBytes)] {
					possible = false
					return nil, nil, nil, false, nil
				}
			}
			equals[cond.Col] = val
		case NEQ:
			if existing, exists := equalsBytes[cond.Col]; exists {
				if bytes.Equal(existing, valBytes) {
					possible = false
					return nil, nil, nil, false, nil
				}
			}
			if _, exists := exclusionBytes[cond.Col]; !exists {
				exclusionBytes[cond.Col] = make(map[string]bool)
				exclusion[cond.Col] = append(exclusion[cond.Col], val)
			}
		case LT:
			curRange, err := newIntervalFromValue(nil, val, false, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Col]; exists {
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
			rng, exists := ranges[cond.Col]
			if !exists {
				ranges[cond.Col] = curRange
			} else {
				ranges[cond.Col], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case LTE:
			curRange, err := newIntervalFromValue(nil, val, false, true)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Col]; exists {
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
			rng, exists := ranges[cond.Col]
			if !exists {
				ranges[cond.Col] = curRange
			} else {
				ranges[cond.Col], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case GT:
			curRange, err := newIntervalFromValue(val, nil, false, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Col]; exists {
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
			rng, exists := ranges[cond.Col]
			if !exists {
				ranges[cond.Col] = curRange
			} else {
				ranges[cond.Col], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		case GTE:
			curRange, err := newIntervalFromValue(val, nil, true, false)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if eqVal, exists := equals[cond.Col]; exists {
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
			rng, exists := ranges[cond.Col]
			if !exists {
				ranges[cond.Col] = curRange
			} else {
				ranges[cond.Col], err = rng.Merge(curRange)
				if err != nil {
					return nil, nil, nil, false, err
				}
			}
		}
	}
	return
}
