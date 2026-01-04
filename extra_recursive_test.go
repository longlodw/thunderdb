package thunderdb

import (
	"fmt"
	"testing"
)

// TestQuery_MutualRecursion tests a scenario where two recursive relations depend on each other.
// Even though ThunderDB's current `CreateRecursion` only creates a single recursive handle,
// we can simulate mutual recursion or complex recursion by having multiple branches that refer to the same recursive relation
// in different ways, or conceptually if we had support for multiple CTEs.
// Since we only have one `Recursion` object, we test "Multiple Recursive Branches" which is a form of non-linear recursion logic.
// Logic:
//
//	even_path(x, y) :- edge(x, y)  (base case 1: length 1 is odd... wait, let's do reachability parity)
//	reach(x, y) :- edge(x, y)
//	reach(x, y) :- edge(x, z), reach(z, y)
//
// Let's test "Odd/Even Path Lengths" if we can store "type" in the recursion.
// Actually, let's test a case with 2 Base cases and 2 Recursive branches.
// Relation: connection(src, dst)
// Query:
//
//	reachable(x, y) :- connection(x, y)
//	reachable(x, y) :- connection(y, x) (undirected base)
//	reachable(x, y) :- reachable(x, z), connection(z, y)
//	reachable(x, y) :- reachable(x, z), connection(y, z) (undirected recursive step)
func TestQuery_ComplexRecursiveBranches(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Graph
	// 1 -> 2 -> 3
	// 4 -> 5
	// No connection between {1,2,3} and {4,5}
	// But we will add undirected logic so 3->2->1 should be reachable from 1 via implicit back-edge
	conns, err := tx.CreatePersistent("connections", map[string]ColumnSpec{
		"src": {Indexed: true},
		"dst": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	conns.Insert(map[string]any{"src": "1", "dst": "2"})
	conns.Insert(map[string]any{"src": "2", "dst": "3"})
	conns.Insert(map[string]any{"src": "4", "dst": "5"})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	defer tx.Rollback()

	conns, _ = tx.LoadPersistent("connections")

	qReach, err := tx.CreateRecursion("reachable", map[string]ColumnSpec{
		"start": {Indexed: true}, // x
		"end":   {Indexed: true}, // y
	})
	if err != nil {
		t.Fatal(err)
	}

	// Base 1: x->y
	qReach.AddBranch(conns.Project(map[string]string{
		"start": "src",
		"end":   "dst",
	}))

	// Base 2: y->x (treat as undirected base)
	qReach.AddBranch(conns.Project(map[string]string{
		"start": "dst",
		"end":   "src",
	}))

	// Rec 1: reach(x,z) + conn(z,y) -> reach(x,y)
	// join on z
	qReach.AddBranch(qReach.Project(map[string]string{
		"start": "start",
		"join":  "end",
	}).Join(conns.Project(map[string]string{
		"join": "src",
		"end":  "dst",
	})).Project(map[string]string{
		"start": "start",
		"end":   "end",
	}))

	// Rec 2: reach(x,z) + conn(y,z) -> reach(x,y) (undirected extension)
	// join on z (which is dst of conn)
	qReach.AddBranch(qReach.Project(map[string]string{
		"start": "start",
		"join":  "end",
	}).Join(conns.Project(map[string]string{
		"join": "dst",
		"end":  "src",
	})).Project(map[string]string{
		"start": "start",
		"end":   "end",
	}))

	// Query: reachable from "1"
	// Should include: 2, 3 (direct/transitive forward)
	// And if fully undirected logic holds:
	//   1->2 (base)
	//   2->1 (base undirected)
	//   2->3 (base)
	//   3->2 (base undirected)
	//   1->3 (rec1: 1->2, 2->3)
	//   3->1 (rec1: 3->2, 2->1)
	// Basically {1,2,3} is a connected component.
	// 1 is reachable from 1? Usually recursion doesn't imply reflexivity unless explicitly added or loop exists.
	// But 1->2->1 is a valid path. So 1 should be in result.

	key1, _ := ToKey("1")
	seq, err := qReach.Select(map[string]*BytesRange{
		"start": NewBytesRange(key1, key1, true, true, nil),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	found := make(map[string]bool)
	for row := range seq {
		val, _ := row.Get("end")
		found[val.(string)] = true
	}

	expected := []string{"1", "2", "3"}
	for _, e := range expected {
		if !found[e] {
			t.Errorf("Expected %s to be reachable from 1", e)
		}
	}
	if found["4"] || found["5"] {
		t.Errorf("Should not reach disconnected component {4,5}")
	}
}

// TestQuery_LongJoinChain verifies a recursive branch that involves a 3-way join.
// path(x, z) :- edge(x, a), edge(a, y), path(y, z). (Skip-one-hop recursion)
func TestQuery_LongJoinChain(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 6
	edges, err := tx.CreatePersistent("edges", map[string]ColumnSpec{
		"u": {Indexed: true},
		"v": {Indexed: true},
	})
	for i := 0; i < 6; i++ {
		edges.Insert(map[string]any{
			"u": fmt.Sprintf("%d", i),
			"v": fmt.Sprintf("%d", i+1),
		})
	}
	tx.Commit()

	tx, err = db.Begin(true)
	defer tx.Rollback()
	edges, _ = tx.LoadPersistent("edges")

	// skip_path(start, end)
	qSkip, err := tx.CreateRecursion("skip_path", map[string]ColumnSpec{
		"start": {Indexed: true},
		"end":   {Indexed: true},
	})

	// Base case: edge(u, v)
	qSkip.AddBranch(edges.Project(map[string]string{
		"start": "u",
		"end":   "v",
	}))

	// Recursive step: edge(start, m1), edge(m1, m2), skip_path(m2, end)
	// This steps 2 hops in the graph before recurring.
	// effectively: start -> m1 -> m2 ... -> end
	// Join 1: edge(start, m1) + edge(m1, m2) => (start, m2)
	// Join 2: (start, m2) + skip_path(m2, end) => (start, end)

	// We can compose this using nested joins or a single 3-way join.
	// Let's try chained joins: E1.Join(E2).Join(Rec)

	e1 := edges.Project(map[string]string{
		"start": "u",
		"j1":    "v",
	})
	e2 := edges.Project(map[string]string{
		"j1": "u",
		"j2": "v",
	})
	rec := qSkip.Project(map[string]string{
		"j2":  "start",
		"end": "end",
	})

	// (E1 join E2) produces (start, j1, j2)
	// join Rec produces (start, j1, j2, end)
	// Project to (start, end)

	complexBranch := e1.Join(e2).Join(rec).Project(map[string]string{
		"start": "start",
		"end":   "end",
	})

	qSkip.AddBranch(complexBranch)

	// Query from 0
	// 1 hop: 0->1 (base)
	// 3 hops: 0->1->2 + path(2, end) -> 0->1->2->3 (rec base 2->3)
	// 5 hops: 0->1->2 + path(2, 5) [2->3->4 + path(4,5)] -> 0..5
	// So we expect 0->1, 0->3, 0->5.
	// 0->2 is NOT reachable directly via this specific recursion logic?
	// Wait, base case handles 1 hop.
	// Rec step consumes 2 hops then delegates.
	// So depth 1 (base) -> 1
	// depth 3 (2 hops + base) -> 3
	// depth 5 (2 hops + 2 hops + base) -> 5
	// What about 2?
	// 0->1->2 requires splitting as edge(0,1), path(1,2).
	// But our recursive step is edge, edge, path.
	// So path(1,2) would need edge(1,a), edge(a,b), path(b, 2).
	// edge(1,2) is base.
	// edge(1,2), edge(2,3), path(3,2)? No path(3,2).
	// So 2 should NOT be reachable via the recursive branch, but is it reachable via base?
	// Base is just edge(x,y). 0->1 is base.
	// 0->2 is NOT an edge. So 0->2 is not in base.
	// Can 0->2 be formed by rec?
	// edge(0,1), edge(1,2), path(2, 2)?
	// If path is reflexive? We didn't add reflexivity.
	// So we expect only odd distances: 1, 3, 5.

	key0, _ := ToKey("0")
	seq, _ := qSkip.Select(map[string]*BytesRange{
		"start": NewBytesRange(key0, key0, true, true, nil),
	}, nil)

	found := make(map[string]bool)
	for row := range seq {
		val, _ := row.Get("end")
		found[val.(string)] = true
	}

	if !found["1"] {
		t.Error("Expected 1 (dist 1)")
	}
	if !found["3"] {
		t.Error("Expected 3 (dist 3)")
	}
	if !found["5"] {
		t.Error("Expected 5 (dist 5)")
	}

	if found["2"] {
		t.Error("Did not expect 2 (dist 2)")
	}
	if found["4"] {
		t.Error("Did not expect 4 (dist 4)")
	}
	if found["6"] {
		t.Error("Did not expect 6 (dist 6)")
	} // 0->6 is dist 6, even.
}
