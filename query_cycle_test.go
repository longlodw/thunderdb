package thunderdb

import (
	"testing"
	"time"
)

func TestQuery_Recursive_Cycle(t *testing.T) {
	// Enforce a timeout to detect infinite loops
	done := make(chan bool)
	go func() {
		testQuery_Recursive_Cycle_Body(t)
		done <- true
	}()

	select {
	case <-done:
		// Test completed
	case <-time.After(2 * time.Second):
		t.Fatal("Test timed out - likely infinite recursion loop due to cycle")
	}
}

func testQuery_Recursive_Cycle_Body(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Schema: Node (id, target)
	// Graph: A -> B -> A (Cycle)
	nodes, err := tx.CreatePersistent("nodes", map[string]ColumnSpec{
		"source": {},
		"target": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert Data (Cycle)
	nodes.Insert(map[string]any{"source": "A", "target": "B"})
	nodes.Insert(map[string]any{"source": "B", "target": "A"})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	nodes, _ = tx.LoadPersistent("nodes")

	// 3. Define Recursive Query: "Reach(source, target)"
	// reach(X, Y) :- nodes(X, Y).
	// reach(X, Z) :- reach(X, Y), nodes(Y, Z).

	qReach, err := tx.CreateRecursion("reach", map[string]ColumnSpec{
		"source": {},
		"target": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Base Case
	// reach(X, Y) :- nodes(X, Y).
	baseProj := nodes.Project(map[string]string{
		"source": "source",
		"target": "target",
	})
	if err := qReach.AddBranch(baseProj); err != nil {
		t.Fatal(err)
	}

	// Body 2: Recursive Case (Left Recursion)
	// reach(X, Z) :- reach(X, Y), nodes(Y, Z).
	// Join on Y (reach.target == nodes.source)

	// reach(X, Y) -> project Y as "join_key"
	reachProj := qReach.Project(map[string]string{
		"source":   "source",
		"join_key": "target", // Y
	})

	// nodes(Y, Z) -> project Y as "join_key"
	nodeProj := nodes.Project(map[string]string{
		"join_key": "source", // Y
		"target":   "target", // Z
	})

	if err := qReach.AddBranch(reachProj.Join(nodeProj).Project(map[string]string{
		"source": "source",
		"target": "target",
	})); err != nil {
		t.Fatal(err)
	}

	// 4. Execution
	// Find all reachable nodes from A.
	// Expected: A -> B, B -> A, so reachable: B, A.
	// If cycle is not handled, this will loop A->B->A->B...
	f, err := ToKeyRanges(Eq("source", "A"))
	if err != nil {
		t.Fatal(err)
	}
	seq, err := qReach.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		// Just consuming to check for termination
		_ = val
	}

	// We expect at least 2 results (A->B, A->A).
	// Standard Datalog usually implies Set semantics, so 2 results.
	// If Bag semantics, it could be infinite.
	if count < 2 {
		t.Errorf("Expected at least 2 reachable paths, got %d", count)
	}
}
