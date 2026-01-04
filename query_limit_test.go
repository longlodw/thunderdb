package thunderdb

import (
	"fmt"
	"testing"
)

func TestQuery_Recursive_DepthLimit(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	edges, err := tx.CreatePersistent("edges", map[string]ColumnSpec{
		"u": {Indexed: true},
		"v": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a chain: 0 -> 1 -> ... -> 100
	// We use 100 to be safe and fast.
	chainLen := 100
	for i := 0; i < chainLen; i++ {
		edges.Insert(map[string]any{
			"u": fmt.Sprintf("%d", i),
			"v": fmt.Sprintf("%d", i+1),
		})
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	edges, _ = tx.LoadPersistent("edges")

	qPath, err := tx.CreateRecursion("path", map[string]ColumnSpec{
		"start": {Indexed: true},
		"end":   {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Base: edge(u, v)
	qPath.AddBranch(edges.Project(map[string]string{
		"start": "u",
		"end":   "v",
	}))

	qPath.SetMaxDepth(1000)

	// Rec: edge(u, m) + path(m, v)
	qPath.AddBranch(edges.Project(map[string]string{
		"start": "u",
		"join":  "v",
	}).Join(qPath.Project(map[string]string{
		"join": "start",
		"end":  "end",
	})).Project(map[string]string{
		"start": "start",
		"end":   "end",
	}))

	// Query: path(0, chainLen)
	key0, _ := ToKey("0")
	f := map[string]*BytesRange{
		"start": NewBytesRange(key0, key0, true, true, nil),
	}

	seq, err := qPath.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	found := false
	target := fmt.Sprintf("%d", chainLen)
	for row := range seq {
		val, _ := row.Get("end")
		if val.(string) == target {
			found = true
		}
	}

	if !found {
		t.Errorf("Expected to find path to %d", chainLen)
	}

	// Now test the limit
	qPath.SetMaxDepth(50)
	_, err = qPath.Select(f, nil)
	if err == nil {
		t.Fatal("Expected recursion depth error with limit 50, got nil")
	} else {
		if err.Error() != "recursion depth exceeded: 50" {
			t.Fatalf("Expected 'recursion depth exceeded: 50', got: %v", err)
		}
	}
}
