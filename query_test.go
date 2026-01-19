package thunderdb

import (
	"os"
	"testing"
)

func setupTestDBForQuery(t *testing.T) (*DB, func()) {
	// Create temp file
	f, err := os.CreateTemp("", "thunder_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()

	// Open DB
	db, err := OpenDB(MsgpackMaUn, name, 0600, nil)
	if err != nil {
		os.Remove(name)
		t.Fatal(err)
	}

	return db, func() {
		db.Close()
		os.Remove(name)
	}
}

func TestQuery_Basic(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	// 1. Setup Schema
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create 'users' relation
	usersRel := "users"
	err = tx.CreateStorage(usersRel, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true}, // id
		{ReferencedCols: []int{1}, IsUnique: true}, // username
		{ReferencedCols: []int{2}, IsUnique: true}, // department
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create 'departments' relation
	deptRel := "departments"
	err = tx.CreateStorage(deptRel, 2, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true}, // department
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert Data
	if err := tx.Insert(usersRel, map[int]any{0: "1", 1: "alice", 2: "engineering"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(usersRel, map[int]any{0: "2", 1: "bob", 2: "hr"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(deptRel, map[int]any{0: "engineering", 1: "building A"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(deptRel, map[int]any{0: "hr", 1: "building B"}); err != nil {
		t.Fatal(err)
	}

	// 2. Test Query Join

	users, err := tx.LoadStoredBody(usersRel)
	if err != nil {
		t.Fatal(err)
	}
	depts, err := tx.LoadStoredBody(deptRel)
	if err != nil {
		t.Fatal(err)
	}

	// Define a query that joins users and departments
	// We want to find users in 'engineering' and their location
	// Users: id(0), username(1), department(2)
	// Depts: department(0), location(1)
	// Join condition: users.department (2) == depts.department (0)
	q, err := users.Join(depts, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Execute Select on the Query
	// Filter by username 'alice'. Username is col 1 in users.
	key, err := ToKey("alice")
	if err != nil {
		t.Fatal(err)
	}
	f := map[int]*Range{
		1: func() *Range {
			r, _ := NewRangeFromBytes(key, key, true, true)
			return r
		}(), // username is at index 1
	}

	seq, err := tx.Select(q, nil, f, nil)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++

		var username string
		if err := row.Get(1, &username); err != nil {
			t.Fatalf("failed to get username: %v", err)
		}
		if username != "alice" {
			t.Errorf("Expected username alice, got %v", username)
		}

		var department string
		if err := row.Get(2, &department); err != nil {
			t.Fatalf("failed to get department: %v", err)
		}
		if department != "engineering" {
			t.Errorf("Expected department engineering, got %v", department)
		}

		var location string
		// 3 is dept from right table, 4 is location
		if err := row.Get(4, &location); err != nil {
			t.Fatalf("failed to get location: %v", err)
		}
		if location != "building A" {
			t.Errorf("Expected location building A, got %v", location)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}
