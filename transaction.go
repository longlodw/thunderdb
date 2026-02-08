package thunderdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"maps"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openkvlab/boltdb"
)

// Tx represents a database transaction. It provides methods for creating
// storage relations, inserting, updating, deleting, and querying data.
//
// A transaction must be committed or rolled back when finished.
// For managed transactions (created via View, Update, or Batch), this is
// handled automatically. For manual transactions (created via Begin),
// you must call Commit or Rollback explicitly.
//
// A Tx is not safe for concurrent use by multiple goroutines.
type Tx struct {
	tx           *boltdb.Tx
	tempTx       *boltdb.Tx
	tempDb       *boltdb.DB
	tempFilePath string
	managed      bool
	stores       map[string]*storage
	db           *DB                 // Reference to parent DB for stats
	startTime    time.Time           // Transaction start time for duration tracking
	tempTableID  uint64              // Counter for temporary table names
	tempStores   map[uint64]*storage // Cache for temporary storages
}

// Commit writes all changes to disk and closes the transaction.
// It panics if called on a managed transaction (one created via View, Update, or Batch).
// After Commit returns successfully, any subsequent Rollback call will be a no-op.
func (tx *Tx) Commit() error {
	if tx.managed {
		panic("cannot commit a managed transaction")
	}
	err := tx.tx.Commit()
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.openTx, -1)
		if err == nil {
			atomic.AddInt64(&tx.db.stats.commits, 1)
		}
		atomic.AddInt64(&tx.db.stats.txDuration, int64(time.Since(tx.startTime)))
	}
	return err
}

// Rollback discards all changes and closes the transaction.
// It panics if called on a managed transaction (one created via View, Update, or Batch).
// It is safe to call Rollback after a successful Commit (it will be a no-op).
// Always defer Rollback() when using manual transactions to ensure cleanup.
func (tx *Tx) Rollback() error {
	if tx.managed {
		panic("cannot rollback a managed transaction")
	}
	err := errors.Join(tx.tx.Rollback(), tx.cleanupTempTx())
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.openTx, -1)
		atomic.AddInt64(&tx.db.stats.rollbacks, 1)
		atomic.AddInt64(&tx.db.stats.txDuration, int64(time.Since(tx.startTime)))
	}
	return err
}

func (tx *Tx) cleanupTempTx() error {
	if tx.tempTx != nil {
		if err := tx.tempTx.Rollback(); err != nil {
			return err
		}
		tx.tempTx = nil
	}
	if tx.tempDb != nil {
		if err := tx.tempDb.Close(); err != nil {
			return err
		}
		tx.tempDb = nil
	}
	if tx.tempFilePath != "" {
		if err := os.Remove(tx.tempFilePath); err != nil {
			return err
		}
		tx.tempFilePath = ""
	}
	return nil
}

func (tx *Tx) ensureTempTx() (*boltdb.Tx, error) {
	if tx.tempTx != nil {
		return tx.tempTx, nil
	}
	tempFile, err := os.CreateTemp("", "thunder_tempdb_*.db")
	if err != nil {
		return nil, err
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()

	tempDb, err := boltdb.Open(tempFilePath, 0600, &DBOptions{
		NoSync:         true,
		NoGrowSync:     true,
		NoFreelistSync: true,
	})
	if err != nil {
		os.Remove(tempFilePath)
		return nil, err
	}
	tempTx, err := tempDb.Begin(true)
	if err != nil {
		tempDb.Close()
		os.Remove(tempFilePath)
		return nil, err
	}
	tx.tempTx = tempTx
	tx.tempDb = tempDb
	tx.tempFilePath = tempFilePath
	return tempTx, nil
}

// ID returns the transaction's unique identifier.
func (tx *Tx) ID() int {
	return tx.tx.ID()
}

// wrapIteratorWithStats wraps an iterator to count rows read for statistics.
func (tx *Tx) wrapIteratorWithStats(it iter.Seq2[*Row, error]) iter.Seq2[*Row, error] {
	return func(yield func(*Row, error) bool) {
		for row, err := range it {
			if err == nil {
				atomic.AddInt64(&tx.db.stats.reads, 1)
			}
			if !yield(row, err) {
				return
			}
		}
	}
}

// CreateStorage creates a new persistent storage relation with the given name,
// column count, and index specifications. The relation is stored in the database
// and persists across transactions.
//
// Columns are referenced by zero-based integer indices. The columnCount specifies
// the total number of columns. Indexes are defined using IndexInfo which specifies
// which columns to index and whether the index enforces uniqueness.
//
// Example:
//
//	// Using IndexInfo struct directly:
//	err := tx.CreateStorage("users", 3,
//	    IndexInfo{ReferencedCols: []int{0}, IsUnique: true},  // unique index on column 0
//	    IndexInfo{ReferencedCols: []int{1}, IsUnique: false}, // non-unique index on column 1
//	    IndexInfo{ReferencedCols: []int{1, 2}, IsUnique: false}, // composite index on columns 1 and 2
//	)
//
//	// Or using helper functions:
//	err = tx.CreateStorage("products", 3,
//	    Unique(0),       // unique index on column 0 (ID)
//	    Index(1),        // non-unique index on column 1 (category)
//	    Index(1, 2),     // composite index on columns 1 and 2
//	)
func (tx *Tx) CreateStorage(
	relation string,
	columnCount int,
	indexInfos ...IndexInfo,
) error {
	var metadataObj Metadata
	if err := initStoredMetadata(&metadataObj, columnCount, indexInfos); err != nil {
		return err
	}
	if s, err := newStorage(tx.tx, relation, metadataObj.ColumnsCount, metadataObj.Indexes); err != nil {
		return err
	} else {
		tx.stores[relation] = s
	}
	atomic.AddInt64(&tx.db.stats.storagesCreated, 1)
	tx.db.storedMeta.Store(relation, &metadataObj)
	return nil
}

// Insert adds a new row to the specified relation. The value map uses column
// indices as keys and the column values as map values.
//
// Returns an error if the insert would violate a unique constraint.
//
// Example:
//
//	err := tx.Insert("users", map[int]any{
//	    0: "user-123",    // id
//	    1: "alice",       // username
//	    2: "admin",       // role
//	})
func (tx *Tx) Insert(relation string, value map[int]any) error {
	start := time.Now()
	s, err := tx.loadStorage(relation)
	if err != nil {
		return err
	}
	err = s.Insert(value)
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.insertDuration, int64(time.Since(start)))
		if err == nil {
			atomic.AddInt64(&tx.db.stats.inserts, 1)
		}
	}
	return err
}

func (tx *Tx) loadStorage(relation string) (*storage, error) {
	s, ok := tx.stores[relation]
	if ok {
		return s, nil
	}
	s, err := loadStorage(tx.tx, relation, &tx.db.storedMeta)
	if err != nil {
		return nil, err
	}
	tx.stores[relation] = s
	return s, nil
}

// Delete removes rows from the specified relation that match the given conditions.
// If no conditions are provided, all rows are deleted.
//
// Example:
//
//	// Delete user with id "user-123"
//	err := tx.Delete("users", thunderdb.Condition{
//	    Field:    0,
//	    Operator: thunderdb.EQ,
//	    Value:    "user-123",
//	})
func (tx *Tx) Delete(
	relation string,
	conditions ...SelectCondition,
) error {
	start := time.Now()
	equals, ranges, exclusion, possible, err := parseConditions(conditions)
	if err != nil {
		return err
	}
	if !possible {
		return nil
	}
	s, err := tx.loadStorage(relation)
	if err != nil {
		return err
	}
	deleted, err := s.Delete(equals, ranges, exclusion)
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.deleteDuration, int64(time.Since(start)))
		atomic.AddInt64(&tx.db.stats.deletes, deleted)
	}
	return err
}

// Update modifies rows in the specified relation that match the given conditions.
// The updates map specifies which columns to update and their new values.
//
// Example:
//
//	// Update role to "superadmin" for user with id "user-123"
//	err := tx.Update("users",
//	    map[int]any{2: "superadmin"}, // update column 2 (role)
//	    thunderdb.Condition{
//	        Field:    0,
//	        Operator: thunderdb.EQ,
//	        Value:    "user-123",
//	    },
//	)
func (tx *Tx) Update(
	relation string,
	updates map[int]any,
	conditions ...SelectCondition,
) error {
	start := time.Now()
	equals, ranges, exclusion, possible, err := parseConditions(conditions)
	if err != nil {
		return err
	}
	if !possible {
		return nil
	}
	s, err := tx.loadStorage(relation)
	if err != nil {
		return err
	}
	updated, err := s.Update(equals, ranges, exclusion, updates)
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.updateDuration, int64(time.Since(start)))
		atomic.AddInt64(&tx.db.stats.updates, updated)
	}
	return err
}

// DeleteStorage removes a storage relation and all its data from the database.
func (tx *Tx) DeleteStorage(relation string) error {
	tnx := tx.tx
	if err := deleteStorage(tnx, relation); err != nil {
		return err
	}
	delete(tx.stores, relation)
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.storagesDeleted, 1)
	}
	return nil
}

// StoredQuery returns a Query representing a stored relation, which can be used
// to build complex queries with projections, joins, and filters.
//
// Example:
//
//	users, err := tx.StoredQuery("users")
//	if err != nil {
//	    return err
//	}
//	results, err := tx.Select(users, thunderdb.Condition{
//	    Field:    1,
//	    Operator: thunderdb.EQ,
//	    Value:    "alice",
//	})
func (tx *Tx) StoredQuery(name string) (*StoredQuery, error) {
	var metadataObj Metadata
	if err := loadMetadata(tx.tx, name, &metadataObj, &tx.db.storedMeta); err != nil {
		return nil, err
	}
	return &StoredQuery{
		storageName: name,
		metadata:    metadataObj,
	}, nil
}

// ClosureQuery creates a new recursive query with the specified number of
// columns and index specifications. The query must be bound to body queries
// using ClosedUnder() before execution.
//
// The maximum number of columns is 64.
func (tx *Tx) ClosureQuery(colsCount int, indexInfos ...IndexInfo) (*Closure, error) {
	if tx.tempTableID == ^uint64(0) {
		return nil, ErrTooManyClosures()
	}
	result := &Closure{
		storageIdx: tx.tempTableID,
	}
	indexInfos = append(indexInfos, UniqueAllCols(colsCount))
	if err := initStoredMetadata(&result.metadata, colsCount, indexInfos); err != nil {
		return nil, err
	}
	tempTx, err := tx.ensureTempTx()
	if err != nil {
		return nil, err
	}

	backingNameBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(backingNameBytes, result.storageIdx)
	backingName := string(backingNameBytes)
	backingStorage, err := newStorage(
		tempTx,
		backingName,
		result.Metadata().ColumnsCount,
		result.Metadata().Indexes,
	)
	if err != nil {
		return nil, err
	}
	tx.tempStores[result.storageIdx] = backingStorage
	tx.tempTableID++
	return result, nil
}

// Metadata returns the metadata for a stored relation, including column count
// and index information.
func (tx *Tx) Metadata(relation string) (*Metadata, error) {
	var metadataObj Metadata
	if err := loadMetadata(tx.tx, relation, &metadataObj, &tx.db.storedMeta); err != nil {
		return nil, err
	}
	return &metadataObj, nil
}

// Select executes a query and returns an iterator over the matching rows.
// The query can be a StoredQuery, ProjectedQuery, JoinedQuery, or Closure
// (for recursive/datalog queries).
//
// Conditions filter the results using field comparisons. Multiple conditions
// are combined with AND logic.
//
// The returned iterator yields (Row, error) pairs. Always check the error value.
//
// Example:
//
//	users, _ := tx.StoredQuery("users")
//	results, err := tx.Select(users,
//	    thunderdb.Condition{Field: 1, Operator: thunderdb.EQ, Value: "alice"},
//	    thunderdb.Condition{Field: 2, Operator: thunderdb.NEQ, Value: "banned"},
//	)
//	if err != nil {
//	    return err
//	}
//	for row, err := range results {
//	    if err != nil {
//	        return err
//	    }
//	    var username string
//	    row.Get(1, &username)
//	    fmt.Println(username)
//	}
func (tx *Tx) Select(
	body Query,
	conditions ...SelectCondition,
) (iter.Seq2[*Row, error], error) {
	start := time.Now()
	if tx.db != nil {
		atomic.AddInt64(&tx.db.stats.queries, 1)
	}

	equals, ranges, exclusion, possible, err := parseConditions(conditions)
	if err != nil {
		return nil, err
	}
	if !possible {
		if tx.db != nil {
			atomic.AddInt64(&tx.db.stats.queryDuration, int64(time.Since(start)))
		}
		return func(yield func(*Row, error) bool) {}, nil
	}
	explored := make(map[bodyFilter]queryNode)
	baseNodes := make([]*backedQueryNode, 0)
	rootNode, err := tx.constructQueryGraph(explored, &baseNodes, body, equals, ranges, exclusion, true)
	if err != nil {
		return nil, err
	}
	for _, bn := range baseNodes {
		// fmt.Printf("Propagating from base node %p\n", bn)
		if err := bn.propagateToParents(nil, nil); err != nil {
			return nil, err
		}
	}
	bestIndex, bestIndexRange, err := rootNode.metadata().bestIndex(equals, ranges)
	if err != nil {
		return nil, err
	}
	cols := make(map[int]bool, rootNode.metadata().ColumnsCount)
	for i := range rootNode.metadata().ColumnsCount {
		cols[i] = true
	}

	// Track index scan vs full scan
	if tx.db != nil {
		if bestIndex != 0 {
			atomic.AddInt64(&tx.db.stats.indexScans, 1)
		} else {
			atomic.AddInt64(&tx.db.stats.fullScans, 1)
		}
		atomic.AddInt64(&tx.db.stats.queryDuration, int64(time.Since(start)))
	}

	result, err := rootNode.Find(bestIndex, bestIndexRange, equals, ranges, exclusion, cols)
	if err != nil {
		return nil, err
	}

	// Wrap the iterator to count rows read
	if tx.db != nil {
		return tx.wrapIteratorWithStats(result), nil
	}
	return result, nil
}

func (tx *Tx) constructQueryGraph(
	explored map[bodyFilter]queryNode,
	baseNodes *[]*backedQueryNode,
	body Query,
	equals map[int]*Value,
	ranges map[int]*interval,
	exclusion map[int][]*Value,
	skipBase bool,
) (queryNode, error) {
	equalsStr := equalsToString(equals)
	rangesStr := rangesToString(ranges)
	exclusionStr := exclusionToString(exclusion)

	bf := bodyFilter{
		body:   body,
		filter: equalsStr + "|" + rangesStr + "|" + exclusionStr,
	}
	if node, ok := explored[bf]; ok {
		return node, nil
	}
	switch b := body.(type) {
	case *Closure:
		result := &closureFilterNode{}
		backingBf := bodyFilter{
			body:   b,
			filter: "",
		}
		resultBackingNode, ok := explored[backingBf]
		if !ok {
			// Create and immediately initialize the closure node's metadata
			// before processing children, so recursive references see valid metadata
			resultBackingNode = &closureNode{}
			backingStorage, exist := tx.tempStores[b.storageIdx]
			if !exist {
				panic(fmt.Sprintf("backing storage for closure with temp ID %d not found", b.storageIdx))
			}
			// Initialize metadata immediately
			initClosureNode(resultBackingNode.(*closureNode), backingStorage)
			explored[backingBf] = resultBackingNode
		}
		// Initialize the closureFilterNode's metadata early, before processing children
		initClosureFilterNode(result, resultBackingNode.(*closureNode), equals, ranges, exclusion)
		explored[bf] = result
		children := make([]queryNode, 0, len(b.bodies))
		for _, bbody := range b.bodies {
			childNode, err := tx.constructQueryGraph(explored, baseNodes, bbody, equals, ranges, exclusion, false)
			if err != nil {
				return nil, err
			}
			children = append(children, childNode)
		}
		if ok {
			for _, child := range children {
				child.AddParent(resultBackingNode)
			}
		} else {
			// Add parent relationships for newly created node
			for _, child := range children {
				child.AddParent(resultBackingNode)
			}
		}
		resultBackingNode.AddParent(result)
		return result, nil
	case *ProjectedQuery:
		result := &projectedQueryNode{}
		explored[bf] = result

		childEquals := make(map[int]*Value)
		for field, v := range equals {
			if field < len(b.cols) {
				childEquals[b.cols[field]] = v
			}
		}
		childRanges := make(map[int]*interval)
		for field, r := range ranges {
			if field < len(b.cols) {
				childRanges[b.cols[field]] = r
			}
		}
		childExclusion := make(map[int][]*Value)
		for field, vals := range exclusion {
			if field < len(b.cols) {
				childExclusion[b.cols[field]] = vals
			}
		}
		childNode, err := tx.constructQueryGraph(explored, baseNodes, b.child, childEquals, childRanges, childExclusion, skipBase)
		if err != nil {
			return nil, err
		}
		if err := initProjectedQueryNode(result, childNode, b.cols); err != nil {
			return nil, err
		}
		return result, nil
	case *JoinedQuery:
		result := &joinedQueryNode{}
		explored[bf] = result
		// Count join for stats
		if tx.db != nil {
			atomic.AddInt64(&tx.db.stats.joins, 1)
		}
		equalsLeft, equalsRight, err := splitEquals(b.left.Metadata(), b.right.Metadata(), equals)
		if err != nil {
			return nil, err
		}
		rangesLeft, rangesRight, err := splitRanges(b.left.Metadata(), b.right.Metadata(), ranges)
		if err != nil {
			return nil, err
		}
		exclusionLeft, exclusionRight, err := splitExclusion(b.left.Metadata(), b.right.Metadata(), exclusion)
		if err != nil {
			return nil, err
		}
		bestIndexRight, _, err := b.right.Metadata().bestIndex(equalsRight, rangesRight)
		if err != nil {
			return nil, err
		}
		var leftNode, rightNode queryNode
		if bestIndexRight == 0 {
			leftNode, err = tx.constructQueryGraph(explored, baseNodes, b.left, equalsLeft, rangesLeft, exclusionLeft, skipBase)
			if err != nil {
				return nil, err
			}
			rightNode, err = tx.constructQueryGraph(explored, baseNodes, b.right, equalsRight, rangesRight, exclusionRight, true)
			if err != nil {
				return nil, err
			}
		} else {
			rightNode, err = tx.constructQueryGraph(explored, baseNodes, b.right, equalsRight, rangesRight, exclusionRight, skipBase)
			if err != nil {
				return nil, err
			}
			leftNode, err = tx.constructQueryGraph(explored, baseNodes, b.left, equalsLeft, rangesLeft, exclusionLeft, true)
			if err != nil {
				return nil, err
			}
		}
		initJoinedQueryNode(result, leftNode, rightNode, b.conditions)
		return result, nil
	case *StoredQuery:
		result := &backedQueryNode{}
		explored[bf] = result
		storage, err := loadStorage(tx.tx, b.storageName, &tx.db.storedMeta)
		if err != nil {
			return nil, err
		}
		initBackedQueryNode(result, storage, equals, ranges, exclusion)
		if !skipBase {
			*baseNodes = append(*baseNodes, result)
		} else {
			result.explored = true
		}
		return result, nil
	default:
		panic(fmt.Sprintf("unsupported body type: %T", body))
	}
}

type bodyFilter struct {
	body   Query
	filter string
}

func rangesToString(ranges map[int]*interval) string {
	keys := slices.Collect(maps.Keys(ranges))
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%d:%s", k, ranges[k].ToString())
	}
	return strings.Join(parts, ";")
}

func equalsToString(equals map[int]*Value) string {
	keys := slices.Collect(maps.Keys(equals))
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		valBytes, err := equals[k].GetRaw()
		if err != nil {
			parts[i] = fmt.Sprintf("%d:ERR", k)
		} else {
			parts[i] = fmt.Sprintf("%d:%x", k, valBytes)
		}
	}
	return strings.Join(parts, ";")
}

func exclusionToString(exclusion map[int][]*Value) string {
	keys := slices.Collect(maps.Keys(exclusion))
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		valParts := make([]string, len(exclusion[k]))
		for j, v := range exclusion[k] {
			valBytes, err := v.GetRaw()
			if err != nil {
				valParts[j] = "ERR"
			} else {
				valParts[j] = fmt.Sprintf("%x", valBytes)
			}
		}
		parts[i] = fmt.Sprintf("%d:[%s]", k, strings.Join(valParts, ","))
	}
	return strings.Join(parts, ";")
}
