package thunderdb_test

import (
	"fmt"
	"os"

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
