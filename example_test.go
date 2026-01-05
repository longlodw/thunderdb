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

	// Open the database using MessagePack marshaler (default recommended)
	db, err := thunderdb.OpenDB(&thunderdb.MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 1. Setup Schema and Insert Data
	err = db.Update(func(tx *thunderdb.Tx) error {
		// Create 'users' relation
		usersRel := "users"
		users, err := tx.CreatePersistent(usersRel, map[string]thunderdb.ColumnSpec{
			"id":       {},
			"username": {Indexed: true},
			"role":     {Indexed: true},
		})
		if err != nil {
			return err
		}

		// Insert Data
		if err := users.Insert(map[string]any{"id": "1", "username": "alice", "role": "admin"}); err != nil {
			return err
		}
		if err := users.Insert(map[string]any{"id": "2", "username": "bob", "role": "user"}); err != nil {
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
		users, err := tx.LoadPersistent(usersRel)
		if err != nil {
			return err
		}

		// Create a filter: username == "alice"
		key, err := thunderdb.ToKey("alice")
		if err != nil {
			return err
		}
		f := map[string]*thunderdb.BytesRange{
			"username": thunderdb.NewBytesRange(key, key, true, true, nil),
		}

		// Execute Select
		seq, err := users.Select(f)
		if err != nil {
			return err
		}

		// Iterate over results
		for row, err := range seq {
			if err != nil {
				return err
			}
			username, _ := row.Get("username")
			role, _ := row.Get("role")
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

	db, err := thunderdb.OpenDB(&thunderdb.MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Start a Read-Write Transaction manually
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	// Always defer Rollback to ensure safety if a panic occurs or we return early with error
	defer tx.Rollback()

	// 3. Define Schema
	users, err := tx.CreatePersistent("users", map[string]thunderdb.ColumnSpec{
		"id":   {},
		"name": {},
	})
	if err != nil {
		panic(err)
	}

	// 4. Insert Data
	if err := users.Insert(map[string]any{"id": "1", "name": "Manual User"}); err != nil {
		panic(err)
	}

	// 5. Commit Changes explicitly
	// If Commit succeeds, the deferred Rollback becomes a no-op
	if err := tx.Commit(); err != nil {
		panic(err)
	}

	// 6. Verify with Read-Only Transaction
	readTx, err := db.Begin(false)
	if err != nil {
		panic(err)
	}
	defer readTx.Rollback()

	users, _ = readTx.LoadPersistent("users")
	iter, _ := users.Select(nil)
	for row, _ := range iter {
		name, _ := row.Get("name")
		fmt.Printf("Found: %s\n", name)
	}

	// Output:
	// Found: Manual User
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

	db, err := thunderdb.OpenDB(&thunderdb.MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Insert Hierarchy Data
	err = db.Update(func(tx *thunderdb.Tx) error {
		employees, err := tx.CreatePersistent("employees", map[string]thunderdb.ColumnSpec{
			"id":         {},
			"name":       {},
			"manager_id": {}, // The link to the parent node
		})
		if err != nil {
			return err
		}

		// Hierarchy: Alice (CEO) -> Bob -> Charlie
		if err := employees.Insert(map[string]any{"id": "1", "name": "Alice", "manager_id": ""}); err != nil {
			return err
		}
		if err := employees.Insert(map[string]any{"id": "2", "name": "Bob", "manager_id": "1"}); err != nil {
			return err
		}
		if err := employees.Insert(map[string]any{"id": "3", "name": "Charlie", "manager_id": "2"}); err != nil {
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
		employees, _ := tx.LoadPersistent("employees")

		// Create a recursive query named "path"
		// Schema: ancestor, descendant
		// recursive=true
		qPath, err := tx.CreateRecursion("path", map[string]thunderdb.ColumnSpec{
			"ancestor":   {},
			"descendant": {},
		})
		if err != nil {
			return err
		}

		// Rule 1 (Base Case): Direct reports
		// path(manager_id, id) :- employees(id, ..., manager_id)
		baseProj := employees.Project(map[string]string{
			"ancestor":   "manager_id",
			"descendant": "id",
		})
		if err := qPath.AddBranch(baseProj); err != nil {
			return err
		}

		// Rule 2 (Recursive Step): Indirect reports
		// path(a, c) :- employees(b, ..., a), path(b, c)
		// We join 'employees' (manager=a, id=b) with 'path' (ancestor=b, descendant=c)
		// Join key is implicit 'b' (mapped to the same name "join_key")

		edgeProj := employees.Project(map[string]string{
			"ancestor": "manager_id", // a
			"join_key": "id",         // b
		})

		pathProj := qPath.Project(map[string]string{
			"join_key":   "ancestor",   // b
			"descendant": "descendant", // c
		})

		if err := qPath.AddBranch(edgeProj.Join(pathProj).Project(map[string]string{
			"ancestor":   "ancestor",
			"descendant": "descendant",
		})); err != nil {
			return err
		}

		// Execute: Find all descendants of Alice (id=1)
		key, err := thunderdb.ToKey("1")
		if err != nil {
			return err
		}
		f := map[string]*thunderdb.BytesRange{
			"ancestor": thunderdb.NewBytesRange(key, key, true, true, nil),
		}

		seq, err := qPath.Select(f)
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
			val, _ := row.Get("descendant")
			descID := val.(string)
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
