# Thunderdb

Thunderdb is a Go library that provides a lightweight, persistent, and datalog-like database interface.
It leverages `bolt` for underlying storage.

This library is designed for Go applications needing an embedded database with capabilities for schema definitions, indexing, filtering, and complex query operations, including recursive queries for hierarchical data.

## Features

- **Embedded Database:** Built on top of `bolt` for reliable, file-based persistence.
- **Relational Operations:** Supports creating persistent "relations" (tables), inserting data, and querying with filters.
- **Flexible Schema:** Define column specifications with optional indexing.
- **Datalog-like Queries:**
  - Support for recursive queries (e.g., finding all descendants in a tree structure).
  - Projections and joins (implicit in query construction).
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

	"github.com/longlodw/thunderdb"
)

func main() {
	// 1. Open the database
	db, err := thunderdb.OpenDB("my.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Insert Data (Read-Write Transaction)
	// We use Update() for automatic transaction management
	err = db.Update(func(tx *thunderdb.Tx) error {
		// 3. Define Schema (Create a Relation)
		// Columns: 0=id, 1=username, 2=role
		// Index on column 1 (username), with unique constraint on column 0 (id)
		err := tx.CreateStorage("users", 3, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},  // id (unique)
			{ReferencedCols: []int{1}, IsUnique: false}, // username (indexed)
		})
		if err != nil {
			return err
		}

		// 4. Insert Data
		return tx.Insert("users", map[int]any{0: "1", 1: "alice", 2: "admin"})
	})
	if err != nil {
		panic(err)
	}

	// 5. Query Data (Read-Only Transaction)
	// We use View() for read-only operations
	err = db.View(func(tx *thunderdb.Tx) error {
		users, err := tx.StoredQuery("users")
		if err != nil {
			return err
		}

		// Execute Select with filter for username "alice"
		results, err := tx.Select(users, thunderdb.Condition{
			Field:    1, // username column
			Operator: thunderdb.EQ,
			Value:    "alice",
		})
		if err != nil {
			return err
		}

		for row, err := range results {
			if err != nil {
				return err
			}
			var username, role string
			if err := row.Get(1, &username); err != nil {
				return err
			}
			if err := row.Get(2, &role); err != nil {
				return err
			}
			fmt.Printf("User: %s, Role: %s\n", username, role)
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
    return tx.Insert("users", userData)
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

// Define schema: 2 columns, unique index on column 0
err = tx.CreateStorage("users", 2, []thunderdb.IndexInfo{
    {ReferencedCols: []int{0}, IsUnique: true},
})
if err != nil {
    panic(err)
}

// Insert data
if err := tx.Insert("users", map[int]any{0: "1", 1: "Manual User"}); err != nil {
    panic(err)
}

// Commit changes explicitly
// If Commit succeeds, the deferred Rollback becomes a no-op
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
// Columns: 0=id, 1=username, 2=first, 3=last
err := tx.CreateStorage("users", 4, []thunderdb.IndexInfo{
    {ReferencedCols: []int{0}, IsUnique: true},     // Unique constraint on id
    {ReferencedCols: []int{1}, IsUnique: false},    // Index on username
    {ReferencedCols: []int{2, 3}, IsUnique: false}, // Composite index on (first, last)
    {ReferencedCols: []int{1, 2}, IsUnique: true},  // Composite unique on (username, first)
})

// Querying using a condition
users, _ := tx.StoredQuery("users")
results, _ := tx.Select(users, thunderdb.Condition{
    Field:    1, // username column
    Operator: thunderdb.EQ,
    Value:    "alice",
})
```

### Recursive Queries

Thunderdb supports recursive Datalog-style queries, useful for traversing hierarchical data like organizational charts or file systems.

```go
// Example: Find all descendants of a manager
// Assume 'employees' table exists with columns: 0=id, 1=name, 2=manager_id

err = db.Update(func(tx *thunderdb.Tx) error {
    employees, err := tx.StoredQuery("employees")
    if err != nil {
        return err
    }

    // Create a recursive query for path traversal
    // Schema: 0=ancestor, 1=descendant
    qPath, err := thunderdb.NewDatalogQuery(2, []thunderdb.IndexInfo{
        {ReferencedCols: []int{0}, IsUnique: false}, // ancestor
        {ReferencedCols: []int{1}, IsUnique: false}, // descendant
    })
    if err != nil {
        return err
    }

    // Rule 1 (Base Case): Direct reports
    // path(manager_id, id) :- employees(id, ..., manager_id)
    baseProj, err := employees.Project(2, 0) // select manager_id, id
    if err != nil {
        return err
    }

    // Rule 2 (Recursive Step): Indirect reports
    // path(a, c) :- employees(b, ..., a), path(b, c)
    // Join employees with path on employees.id = path.ancestor
    joinedPathProj, err := employees.Join(qPath, thunderdb.JoinOn{
        LeftField:  0, // employees.id
        RightField: 0, // path.ancestor
        Operator:   thunderdb.EQ,
    })
    if err != nil {
        return err
    }
    pathProj, err := joinedPathProj.Project(2, 4) // select manager_id, descendant
    if err != nil {
        return err
    }

    // Bind both branches to the recursive query
    if err := qPath.Bind(baseProj, pathProj); err != nil {
        return err
    }

    // Execute: Find all descendants of ID "1"
    seq, err := tx.Select(qPath, thunderdb.Condition{
        Field:    0, // ancestor
        Operator: thunderdb.EQ,
        Value:    "1",
    })
    if err != nil {
        return err
    }

    for row, err := range seq {
        if err != nil {
            return err
        }
        var descID string
        if err := row.Get(1, &descID); err != nil {
            return err
        }
        fmt.Printf("Descendant ID: %s\n", descID)
    }
    return nil
})
```

## License

See the [LICENSE](LICENSE) file for details.
