package thunder

import (
	"testing"
)

func TestQuery_Recursive(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Schema: Employees (id, name, manager_id)
	// Typical recursive structure: manager is also an employee
	employees, err := tx.CreatePersistent("employees", map[string]ColumnSpec{
		"id":         {},
		"name":       {},
		"manager_id": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert Data (Hierarchy)
	// Alice (CEO, no manager) -> Bob -> Charlie -> Dave
	employees.Insert(map[string]any{"id": "1", "name": "Alice", "manager_id": ""})
	employees.Insert(map[string]any{"id": "2", "name": "Bob", "manager_id": "1"})
	employees.Insert(map[string]any{"id": "3", "name": "Charlie", "manager_id": "2"})
	employees.Insert(map[string]any{"id": "4", "name": "Dave", "manager_id": "3"})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	employees, _ = tx.LoadPersistent("employees")

	// 3. Define Recursive Query: "Path(ancestor, descendant)"
	// Modeled after Datalog: path(a, c) :- edge(a, b), path(b, c).
	// OR simpler left-linear: path(a, b) :- edge(a, b).
	//                         path(a, c) :- edge(a, b), path(b, c).

	qPath, err := tx.CreateQuery("path", []string{"ancestor", "descendant"}, true)
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Base Case
	// path(a, b) :- edge(a, b).
	// edge is 'employees' table: manager_id (a) -> id (b).
	baseProj, err := employees.Project(map[string]string{
		"manager_id": "ancestor",
		"id":         "descendant",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := qPath.AddBody(baseProj); err != nil {
		t.Fatal(err)
	}

	// Body 2: Recursive Case
	// path(a, c) :- edge(a, b), path(b, c).
	// Join on 'b'.
	// edge (employees): manager_id (a) -> ancestor, id (b) -> join_key
	// path (qPath): ancestor (b) -> join_key, descendant (c) -> descendant

	edgeProj2, err := employees.Project(map[string]string{
		"manager_id": "ancestor", // a
		"id":         "join_key", // b
	})
	if err != nil {
		t.Fatal(err)
	}

	pathProj2, err := qPath.Project(map[string]string{
		"ancestor":   "join_key",   // b
		"descendant": "descendant", // c
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := qPath.AddBody(edgeProj2, pathProj2); err != nil {
		t.Fatal(err)
	}

	// 4. Execution
	// Find all descendants of Alice (id=1).
	// query: path(ancestor=1, descendant=X).
	seq, err := qPath.Select(Eq("ancestor", "1"))
	if err != nil {
		t.Fatal(err)
	}

	results := make(map[string]bool)
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		// Expected results should have ancestor="1"
		if val["ancestor"] != "1" {
			t.Errorf("Expected ancestor=1, got %v", val["ancestor"])
		}
		results[val["descendant"].(string)] = true
	}

	// Expected descendants: Bob (2), Charlie (3), Dave (4)
	if len(results) != 3 {
		t.Errorf("Expected 3 descendants, got %d", len(results))
	}
	expectedDescendants := []string{"2", "3", "4"}
	for _, desc := range expectedDescendants {
		if !results[desc] {
			t.Errorf("Expected descendant %s not found in results", desc)
		}
	}
}
