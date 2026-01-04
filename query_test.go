package thunderdb

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
	users, err := tx.CreatePersistent(usersRel, map[string]ColumnSpec{
		"id":         {},
		"username":   {Indexed: true},
		"department": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create 'departments' relation
	deptRel := "departments"
	depts, err := tx.CreatePersistent(deptRel, map[string]ColumnSpec{
		"department": {Indexed: true},
		"location":   {},
	})
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
	q := users.Join(depts)

	// Execute Select on the Query
	// Filter by username 'alice'
	key, err := ToKey("alice")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"username": NewBytesRange(key, key, true, true, nil),
	}
	seq, err := q.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		username, _ := row.Get("username")
		if username != "alice" {
			t.Errorf("Expected username alice, got %v", username)
		}
		department, _ := row.Get("department")
		if department != "engineering" {
			t.Errorf("Expected department engineering, got %v", department)
		}
		location, _ := row.Get("location")
		if location != "building A" {
			t.Errorf("Expected location building A, got %v", location)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}
