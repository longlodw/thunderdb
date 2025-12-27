package thunder_test

import (
	"fmt"
	"os"

	"github.com/longlodw/thunder"
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
	db, err := thunder.OpenDB(&thunder.MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 1. Setup Schema and Insert Data
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback() // Safe to call, will be ignored if committed

	// Create 'users' relation
	usersRel := "users"
	users, err := tx.CreatePersistent(usersRel, map[string]thunder.ColumnSpec{
		"id":       {},
		"username": {Indexed: true},
		"role":     {Indexed: true},
	})
	if err != nil {
		panic(err)
	}

	// Insert Data
	if err := users.Insert(map[string]any{"id": "1", "username": "alice", "role": "admin"}); err != nil {
		panic(err)
	}
	if err := users.Insert(map[string]any{"id": "2", "username": "bob", "role": "user"}); err != nil {
		panic(err)
	}

	if err := tx.Commit(); err != nil {
		panic(err)
	}

	// 2. Query Data
	tx, err = db.Begin(false) // Read-only transaction
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	// Load the relation
	users, err = tx.LoadPersistent(usersRel)
	if err != nil {
		panic(err)
	}

	// Create a filter: username == "alice"
	op := thunder.Eq("username", "alice")
	f, err := thunder.ToKeyRanges(op)
	if err != nil {
		panic(err)
	}

	// Execute Select
	seq, err := users.Select(f)
	if err != nil {
		panic(err)
	}

	// Iterate over results
	for val, err := range seq {
		if err != nil {
			panic(err)
		}
		fmt.Printf("Found user: %s, Role: %s\n", val["username"], val["role"])
	}

	// Output:
	// Found user: alice, Role: admin
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

	db, err := thunder.OpenDB(&thunder.MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 2. Insert Hierarchy Data
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	employees, err := tx.CreatePersistent("employees", map[string]thunder.ColumnSpec{
		"id":         {},
		"name":       {},
		"manager_id": {}, // The link to the parent node
	})
	if err != nil {
		panic(err)
	}

	// Hierarchy: Alice (CEO) -> Bob -> Charlie
	employees.Insert(map[string]any{"id": "1", "name": "Alice", "manager_id": ""})
	employees.Insert(map[string]any{"id": "2", "name": "Bob", "manager_id": "1"})
	employees.Insert(map[string]any{"id": "3", "name": "Charlie", "manager_id": "2"})

	if err := tx.Commit(); err != nil {
		panic(err)
	}

	// 3. Define and Execute Recursive Query
	tx, err = db.Begin(true) // Needs write lock to create temp tables for recursion
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	employees, _ = tx.LoadPersistent("employees")

	// Create a recursive query named "path"
	// Schema: ancestor, descendant
	// recursive=true
	qPath, err := tx.CreateRecursion("path", map[string]thunder.ColumnSpec{
		"ancestor":   {},
		"descendant": {},
	})
	if err != nil {
		panic(err)
	}

	// Rule 1 (Base Case): Direct reports
	// path(manager_id, id) :- employees(id, ..., manager_id)
	baseProj := employees.Project(map[string]string{
		"ancestor":   "manager_id",
		"descendant": "id",
	})
	if err := qPath.AddBranch(baseProj); err != nil {
		panic(err)
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
		panic(err)
	}

	// Execute: Find all descendants of Alice (id=1)
	f, err := thunder.ToKeyRanges(thunder.Eq("ancestor", "1"))
	if err != nil {
		panic(err)
	}
	seq, err := qPath.Select(f)
	if err != nil {
		panic(err)
	}

	// Iterate and collect results
	// Expect Bob (2) and Charlie (3)
	for val, err := range seq {
		if err != nil {
			panic(err)
		}
		// Fetch the name for the descendant ID to make the output readable
		// (In a real app, you might join back to the employees table)
		descID := val["descendant"].(string)
		var name string
		switch descID {
		case "2":
			name = "Bob"
		case "3":
			name = "Charlie"
		}
		fmt.Printf("Descendant of Alice: %s (ID: %s)\n", name, descID)
	}

	// Unordered output is common in map iteration, but for deterministic test output:
	// Output:
	// Descendant of Alice: Bob (ID: 2)
	// Descendant of Alice: Charlie (ID: 3)
}
