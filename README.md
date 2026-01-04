# Thunderdb

Thunderdb is a Go library that provides a lightweight, persistent, and datalog-like database interface.
It leverages `bolt` for underlying storage and supports serialization via `MessagePack`, `JSON`, `Gob`, or custom marshalers/unmarshalers.

This library is designed for Go applications needing an embedded database with capabilities for schema definitions, indexing, filtering, and complex query operations, including recursive queries for hierarchical data.

## Features

- **Embedded Database:** Built on top of `bolt` for reliable, file-based persistence.
- **Relational Operations:** Supports creating persistent "relations" (tables), inserting data, and querying with filters.
- **Flexible Schema:** Define column specifications with optional indexing.
- **Datalog-like Queries:**
  - Support for recursive queries (e.g., finding all descendants in a tree structure).
  - Projections and joins (implicit in query construction).
- **Flexible Serialization:** Uses pluggable marshalers/unmarshalers (MessagePack, JSON, Gob, or custom).
- **Transaction Support:** Full support for ACID transactions (Read-Only and Read-Write).

Note: Always call `tx.Rollback()` using `defer` to ensure the transaction is closed properly. To persist changes, you must explicitly call `tx.Commit()`. If `Commit()` is successful, the deferred `Rollback()` will be a no-op.

## Installation

```bash
go get github.com/longlodw/thunderdb
```

## Usage

### Basic Usage

This example shows how to open a database, define a schema, insert data, and perform a basic query.

```go
package main

import (
	"fmt"
	"os"

	"github.com/longlodw/thunderdb"
)

func main() {
	// 1. Open the database
	// We use MsgpackMaUn for MessagePack marshaling/unmarshaling
	db, err := thunderdb.OpenDB(&thunderdb.MsgpackMaUn, "my.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Insert Data (Read-Write Transaction)
	// We use Update() for automatic transaction management
	err = db.Update(func(tx *thunderdb.Tx) error {
		// 3. Define Schema (Create a Relation)
		users, err := tx.CreatePersistent("users", map[string]thunderdb.ColumnSpec{
			"id":       {},
			"username": {Indexed: true},
			"role":     {},
		})
		if err != nil {
			return err
		}

		// 4. Insert Data
		return users.Insert(map[string]any{"id": "1", "username": "alice", "role": "admin"})
	})
	if err != nil {
		panic(err)
	}

	// 5. Query Data (Read-Only Transaction)
	// We use View() for read-only operations
	err = db.View(func(tx *thunderdb.Tx) error {
		users, err := tx.LoadPersistent("users")
		if err != nil {
			return err
		}

		// Filter for username "alice"
		key, err := thunderdb.ToKey("alice")
		if err != nil {
			return err
		}
		filter := map[string]*thunderdb.BytesRange{
			"username": thunderdb.NewBytesRange(key, key, true, true, nil),
		}
		
		// Execute Select
		results, err := users.Select(filter, nil)
		if err != nil {
			return err
		}

		for row := range results {
			fmt.Printf("User: %s, Role: %s\n", row["username"], row["role"])
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
```

### Batch Updates (Group Commit)

For high-concurrency write scenarios, use `Batch()`. This allows multiple concurrent updates to be grouped into a single disk sync, significantly improving throughput at the cost of slightly higher latency per transaction.

```go
// Concurrent goroutines can call this safely
err := db.Batch(func(tx *thunderdb.Tx) error {
    users, _ := tx.LoadPersistent("users")
    return users.Insert(userData)
})
```

You can tune the batching behavior:
```go
db.SetMaxBatchSize(1000)          // Max size of a batch
db.SetMaxBatchDelay(10 * time.Millisecond) // Max wait time for a batch
```

### Manual Transaction Management

While `Update`, `View`, and `Batch` are recommended for most use cases, you can also manage transactions manually if you need fine-grained control (e.g., maintaining a transaction handle across multiple function calls).

**Important:** You must always ensure `Rollback()` is called. If you commit successfully, the deferred rollback will be a no-op and only clean up resources.

```go
// Start a Read-Write Transaction
tx, err := db.Begin(true)
if err != nil {
    panic(err)
}
defer tx.Rollback()

// ... perform operations ...

// Commit changes explicitly
if err := tx.Commit(); err != nil {
    panic(err)
}
```

### Uniques and Composite Indexes

Thunderdb supports defining unique constraints and composite indexes.
A **Composite Index** is an index that spans multiple columns.
A **Unique Constraint** ensures that all values in a column (or a set of columns) are distinct across the table.

```go
// Define Schema with Unique and Composite Index
users, err := tx.CreatePersistent("users", map[string]thunderdb.ColumnSpec{
    "id":       {Unique: true}, // Unique constraint on single column
    "username": {Indexed: true},
    "first":    {},
    "last":     {},
    // Composite Index on (first, last) named "name"
    // This allows efficient querying by both first and last name together
    "name": {
        ReferenceCols: []string{"first", "last"},
        Indexed:       true,
    },
    // Composite Unique Constraint on (username, first) named "user_identity"
    // This ensures that the combination of username and first name is unique
    "user_identity": {
        ReferenceCols: []string{"username", "first"},
        Unique:        true,
    },
})

// Querying using the composite index
// Note: Pass values as a slice in the same order as ReferenceCols
key, _ := thunderdb.ToKey("John", "Doe")
filter := map[string]*thunderdb.BytesRange{
    "name": thunderdb.NewBytesRange(key, key, true, true, nil),
}
results, _ := users.Select(filter, nil)
```

### Recursive Queries

Thunderdb supports recursive Datalog-style queries, useful for traversing hierarchical data like organizational charts or file systems.

```go
// Example: Find all descendants of a manager
// Assume 'employees' table exists with 'id' and 'manager_id'

// Define a recursive query "path" with columns "ancestor" and "descendant"
// The new API uses CreateRecursion instead of CreateQuery
qPath, _ := tx.CreateRecursion("path", map[string]thunderdb.ColumnSpec{
    "ancestor":   {},
    "descendant": {},
})

// Rule 1: Direct reports (Base case)
// path(A, B) :- employees(manager_id=A, id=B)
baseProj := employees.Project(map[string]string{
    "ancestor":   "manager_id",
    "descendant": "id",
})
qPath.AddBranch(baseProj)

// Rule 2: Indirect reports (Recursive step)
// path(A, C) :- employees(manager_id=A, id=B), path(ancestor=B, descendant=C)
// Join employees(manager=a, id=b) with path(ancestor=b, descendant=c)

// employees(A, B) -> project B as "join_key"
edgeProj := employees.Project(map[string]string{
    "ancestor": "manager_id", // A
    "join_key": "id",         // B
})

// path(B, C) -> project B as "join_key"
pathProj := qPath.Project(map[string]string{
    "join_key":   "ancestor",   // B
    "descendant": "descendant", // C
})

// Join edgeProj and pathProj, then project final result
recursiveStep := edgeProj.Join(pathProj).Project(map[string]string{
    "ancestor":   "ancestor",
    "descendant": "descendant",
})

qPath.AddBranch(recursiveStep)

// Execute query to find descendants of ID "1"
key, _ := thunderdb.ToKey("1")
filter := map[string]*thunderdb.BytesRange{
    "ancestor": thunderdb.NewBytesRange(key, key, true, true, nil),
}
results, _ := qPath.Select(filter, nil)
```

## License

See the [LICENSE](LICENSE) file for details.
