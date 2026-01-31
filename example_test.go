package thunderdb_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/longlodw/thunderdb"
)

// ExampleDB_Basic demonstrates how to open a database, create a relation,
// insert data, and perform a simple query.
func Example() {
	// Create a temporary file for the database
	tmpfile, err := os.CreateTemp("", "thunder_example_*.db")
	if err != nil {
		panic(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath) // Clean up database file

	// Open the database
	db, err := thunderdb.OpenDB(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 1. Setup Schema and Insert Data
	err = db.Update(func(tx *thunderdb.Tx) error {
		// Create 'users' relation
		usersRel := "users"
		err := tx.CreateStorage(usersRel, 3, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: false},
			{ReferencedCols: []int{1}, IsUnique: true},
		})
		if err != nil {
			return err
		}

		// Insert Data
		if err := tx.Insert(usersRel, map[int]any{0: "1", 1: "alice", 2: "admin"}); err != nil {
			return err
		}
		if err := tx.Insert(usersRel, map[int]any{0: "2", 1: "bob", 2: "user"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// 2. Query Data
	err = db.View(func(tx *thunderdb.Tx) error {
		// Load the relation
		usersRel := "users"
		users, err := tx.StoredQuery(usersRel)
		if err != nil {
			return err
		}

		// Execute Select
		seq, err := tx.Select(users, thunderdb.Condition{
			Field:    1,
			Operator: thunderdb.EQ,
			Value:    "alice",
		})
		if err != nil {
			return err
		}

		// Iterate over results
		for row, err := range seq {
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
			fmt.Printf("Found user: %s, Role: %s\n", username, role)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// Found user: alice, Role: admin
}

// ExampleDB_ManualTx demonstrates how to manually manage transactions using Begin/Commit/Rollback.
// This is useful when you need to maintain a transaction handle across multiple function calls
// or have complex logic that doesn't fit well into a closure.
func Example_manualTx() {
	// 1. Setup Database
	tmpfile, err := os.CreateTemp("", "thunder_example_manual_*.db")
	if err != nil {
		panic(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := thunderdb.OpenDB(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	func() {
		// 2. Start a Read-Write Transaction manually
		tx, err := db.Begin(true)
		if err != nil {
			panic(err)
		}
		// Always defer Rollback to ensure safety if a panic occurs or we return early with error
		defer tx.Rollback()

		// 3. Define Schema
		err = tx.CreateStorage("users", 2, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},
		})
		if err != nil {
			panic(err)
		}

		// 4. Insert Data
		if err := tx.Insert("users", map[int]any{0: "1", 1: "Manual User"}); err != nil {
			panic(err)
		}

		// 5. Commit Changes explicitly
		// If Commit succeeds, the deferred Rollback becomes a no-op
		if err := tx.Commit(); err != nil {
			panic(err)
		}
	}()

	// 6. Verify with Read-Only Transaction
	readTx, err := db.Begin(false)
	if err != nil {
		panic(err)
	}
	defer readTx.Rollback()

	users, err := readTx.StoredQuery("users")
	if err != nil {
		panic(err)
	}
	iter, err := readTx.Select(users)
	if err != nil {
		panic(err)
	}
	for row, err := range iter {
		if err != nil {
			panic(err)
		}
		var id, role string
		if err := row.Get(0, &id); err != nil {
			panic(err)
		}
		if err := row.Get(1, &role); err != nil {
			panic(err)
		}
		fmt.Printf("Found: %s, %s\n", id, role)
	}

	// Output:
	// Found: 1, Manual User
}

// ExampleDB_Recursive demonstrates a recursive query to find descendants
// in a hierarchical organizational structure (Employee -> Manager).
func Example_recursive() {
	// 1. Setup Database
	tmpfile, err := os.CreateTemp("", "thunder_example_recursive_*.db")
	if err != nil {
		panic(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := thunderdb.OpenDB(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Insert Hierarchy Data
	err = db.Update(func(tx *thunderdb.Tx) error {
		err := tx.CreateStorage("employees", 3, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},  // id
			{ReferencedCols: []int{2}, IsUnique: false}, // manager_id
		})
		if err != nil {
			return err
		}

		// Hierarchy: Alice (CEO) -> Bob -> Charlie
		if err := tx.Insert("employees", map[int]any{0: "1", 1: "Alice", 2: ""}); err != nil {
			return err
		}
		if err := tx.Insert("employees", map[int]any{0: "2", 1: "Bob", 2: "1"}); err != nil {
			return err
		}
		if err := tx.Insert("employees", map[int]any{0: "3", 1: "Charlie", 2: "2"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// 3. Define and Execute Recursive Query
	// Needs write lock (Update) to create temp tables for recursion
	err = db.Update(func(tx *thunderdb.Tx) error {
		employees, err := tx.StoredQuery("employees")
		if err != nil {
			return err
		}

		// Create a recursive query named "path"
		// Schema: ancestor, descendant
		// recursive=true
		qPath, err := thunderdb.NewDatalogQuery(2, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: false}, // ancestor
			{ReferencedCols: []int{1}, IsUnique: false}, // descendant
		})
		if err != nil {
			return err
		}

		// Rule 1 (Base Case): Direct reports
		// path(manager_id, id) :- employees(id, ..., manager_id)
		baseProj, err := employees.Project(2, 0)
		if err != nil {
			return err
		}

		// Rule 2 (Recursive Step): Indirect reports
		// path(a, c) :- employees(b, ..., a), path(b, c)
		// We join 'employees' (manager=a, id=b) with 'path' (ancestor=b, descendant=c)
		// Join key is implicit 'b' (mapped to the same name "join_key")

		joinedPathProj, err := employees.Join(qPath, thunderdb.JoinOn{
			LeftField:  0, // employees.id -> b
			RightField: 0, // path.ancestor -> b
			Operator:   thunderdb.EQ,
		})
		if err != nil {
			return err
		}
		pathProj, err := joinedPathProj.Project(2, 4) // select a, c
		if err != nil {
			return err
		}

		if err := qPath.Bind(baseProj, pathProj); err != nil {
			return err
		}

		// Execute: Find all descendants of Alice (id=1)
		seq, err := tx.Select(qPath, thunderdb.Condition{
			Field:    0, // ancestor
			Operator: thunderdb.EQ,
			Value:    "1", // Alice's ID
		})
		if err != nil {
			return err
		}

		// Iterate and collect results
		// Expect Bob (2) and Charlie (3)
		for row, err := range seq {
			if err != nil {
				return err
			}
			// Fetch the name for the descendant ID to make the output readable
			// (In a real app, you might join back to the employees table)
			var descID string
			if err := row.Get(1, &descID); err != nil {
				return err
			}
			var name string
			switch descID {
			case "2":
				name = "Bob"
			case "3":
				name = "Charlie"
			}
			fmt.Printf("Descendant of Alice: %s (ID: %s)\n", name, descID)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Unordered output is common in map iteration, but for deterministic test output:
	// Output:
	// Descendant of Alice: Bob (ID: 2)
	// Descendant of Alice: Charlie (ID: 3)
}

// Example_stats demonstrates how to use the Stats API to monitor database
// performance and behavior.
func Example_stats() {
	// Setup database
	tmpfile, err := os.CreateTemp("", "thunder_example_stats_*.db")
	if err != nil {
		panic(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := thunderdb.OpenDB(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Perform some operations
	err = db.Update(func(tx *thunderdb.Tx) error {
		err := tx.CreateStorage("products", 3, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},  // id
			{ReferencedCols: []int{1}, IsUnique: false}, // category
		})
		if err != nil {
			return err
		}

		// Insert some products
		for i := 0; i < 100; i++ {
			category := "electronics"
			if i%2 == 0 {
				category = "books"
			}
			err = tx.Insert("products", map[int]any{0: i, 1: category, 2: fmt.Sprintf("Product %d", i)})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Query with index (category lookup)
	err = db.View(func(tx *thunderdb.Tx) error {
		products, err := tx.StoredQuery("products")
		if err != nil {
			return err
		}
		results, err := tx.Select(products, thunderdb.Condition{
			Field:    1, // category (indexed)
			Operator: thunderdb.EQ,
			Value:    "electronics",
		})
		if err != nil {
			return err
		}
		count := 0
		for _, err := range results {
			if err != nil {
				return err
			}
			count++
		}
		fmt.Printf("Found %d electronics products\n", count)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Get statistics
	stats := db.Stats()

	// Print key metrics
	fmt.Printf("Write transactions: %d\n", stats.WriteTxTotal)
	fmt.Printf("Read transactions: %d\n", stats.ReadTxTotal)
	fmt.Printf("Rows inserted: %d\n", stats.RowsInserted)
	fmt.Printf("Queries executed: %d\n", stats.QueriesTotal)
	fmt.Printf("Index scans: %d\n", stats.IndexScansTotal)
	fmt.Printf("Rows read: %d\n", stats.RowsRead)

	// Verify timing is tracked
	if stats.InsertDuration > 0 {
		fmt.Println("Insert timing tracked: yes")
	}
	if stats.QueryDuration > 0 {
		fmt.Println("Query timing tracked: yes")
	}

	// Print the full stats summary (showing it works, but not checking exact output)
	fullStats := stats.String()
	if strings.Contains(fullStats, "ThunderDB Stats") {
		fmt.Println("Full stats summary available: yes")
	}

	// Reset stats
	db.ResetStats()
	statsAfterReset := db.Stats()
	fmt.Printf("Rows inserted after reset: %d\n", statsAfterReset.RowsInserted)

	// Output:
	// Found 50 electronics products
	// Write transactions: 1
	// Read transactions: 1
	// Rows inserted: 100
	// Queries executed: 1
	// Index scans: 1
	// Rows read: 50
	// Insert timing tracked: yes
	// Query timing tracked: yes
	// Full stats summary available: yes
	// Rows inserted after reset: 0
}

// Example_snapshot demonstrates how to create a consistent point-in-time
// backup of a database using the Snapshot method.
//
// Snapshots can be written to any io.Writer, making it easy to back up to:
//   - Local files
//   - Network connections
//   - Cloud storage (S3, GCS, etc.)
//   - Compression streams
func Example_snapshot() {
	// 1. Setup: Create and populate a database
	tmpfile, err := os.CreateTemp("", "thunder_example_snapshot_*.db")
	if err != nil {
		panic(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := thunderdb.OpenDB(dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}

	// Create schema and insert data
	err = db.Update(func(tx *thunderdb.Tx) error {
		if err := tx.CreateStorage("users", 3, []thunderdb.IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},
		}); err != nil {
			return err
		}
		if err := tx.Insert("users", map[int]any{0: "1", 1: "alice", 2: "admin"}); err != nil {
			return err
		}
		if err := tx.Insert("users", map[int]any{0: "2", 1: "bob", 2: "user"}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// 2. Create a snapshot (backup)
	// The snapshot is taken within a read transaction, so it's safe to call
	// while other goroutines are reading or writing to the database.
	backupFile, err := os.CreateTemp("", "thunder_backup_*.db")
	if err != nil {
		panic(err)
	}
	backupPath := backupFile.Name()
	defer os.Remove(backupPath)

	n, err := db.Snapshot(backupFile)
	if err != nil {
		panic(err)
	}
	backupFile.Close()
	db.Close()

	if n > 0 {
		fmt.Println("Snapshot created successfully")
	}

	// 3. Restore: Open the backup as a new database
	backupDB, err := thunderdb.OpenDB(backupPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer backupDB.Close()

	// 4. Verify: Query the restored database
	err = backupDB.View(func(tx *thunderdb.Tx) error {
		users, err := tx.StoredQuery("users")
		if err != nil {
			return err
		}
		results, err := tx.Select(users)
		if err != nil {
			return err
		}
		count := 0
		for row, err := range results {
			if err != nil {
				return err
			}
			var username string
			if err := row.Get(1, &username); err != nil {
				return err
			}
			fmt.Printf("Restored user: %s\n", username)
			count++
		}
		fmt.Printf("Total users restored: %d\n", count)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Output:
	// Snapshot created successfully
	// Restored user: alice
	// Restored user: bob
	// Total users restored: 2
}
