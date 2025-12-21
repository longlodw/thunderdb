package thunder

import (
	"testing"
)

func TestQuery_Basic(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Setup Schema
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create 'users' relation
	usersRel := "users"
	usersCols := []string{"id", "username", "department"}
	usersIdx := map[string][]string{"username": {"username"}, "department": {"department"}}
	users, err := tx.CreatePersistent(usersRel, usersCols, usersIdx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Create 'departments' relation
	deptRel := "departments"
	deptCols := []string{"department", "location"}
	deptIdx := map[string][]string{"department": {"department"}}
	depts, err := tx.CreatePersistent(deptRel, deptCols, deptIdx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Insert Data
	if err := users.Insert(map[string]any{"id": "1", "username": "alice", "department": "engineering"}); err != nil {
		t.Fatal(err)
	}
	if err := users.Insert(map[string]any{"id": "2", "username": "bob", "department": "hr"}); err != nil {
		t.Fatal(err)
	}
	if err := depts.Insert(map[string]any{"department": "engineering", "location": "building A"}); err != nil {
		t.Fatal(err)
	}
	if err := depts.Insert(map[string]any{"department": "hr", "location": "building B"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 2. Test Query Join
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	users, err = tx.LoadPersistent(usersRel)
	if err != nil {
		t.Fatal(err)
	}
	depts, err = tx.LoadPersistent(deptRel)
	if err != nil {
		t.Fatal(err)
	}

	// Define a query that joins users and departments
	// We want to find users in 'engineering' and their location
	q, err := tx.CreateQuery("agg", []string{"id", "username", "department", "location"}, false)
	if err != nil {
		t.Fatal(err)
	}

	// Query Body: Join users and departments on 'department'
	// The join logic is implicit: both have 'department' column
	if err := q.AddBody(users, depts); err != nil {
		t.Fatal(err)
	}

	// Execute Select on the Query
	// Filter by username 'alice'
	op := Eq("username", "alice")
	seq, err := q.Select(op)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["username"] != "alice" {
			t.Errorf("Expected username alice, got %v", val["username"])
		}
		if val["department"] != "engineering" {
			t.Errorf("Expected department engineering, got %v", val["department"])
		}
		if val["location"] != "building A" {
			t.Errorf("Expected location building A, got %v", val["location"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}
