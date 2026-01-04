package thunderdb

import (
	"fmt"
	"iter"
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

	qPath, err := tx.CreateRecursion("path", map[string]ColumnSpec{
		"ancestor":   {},
		"descendant": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Recursive Case
	// path(a, c) :- edge(a, b), path(b, c).
	// Join on 'b'.
	// edge (employees): manager_id (a) -> ancestor, id (b) -> join_key
	// path (qPath): ancestor (b) -> join_key, descendant (c) -> descendant

	edgeProj2 := employees.Project(map[string]string{
		"manager":  "manager_id", // a
		"ancestor": "id",         // b
	})

	pathProj2 := qPath.Project(map[string]string{
		"ancestor":   "ancestor",   // b
		"descendant": "descendant", // c
	})

	if err := qPath.AddBranch(edgeProj2.Join(pathProj2).Project(map[string]string{
		"ancestor":   "manager",
		"descendant": "descendant",
	})); err != nil {
		t.Fatal(err)
	}

	// Body 2: Base Case
	// path(a, b) :- edge(a, b).
	// edge is 'employees' table: manager_id (a) -> id (b).
	baseProj := employees.Project(map[string]string{
		"ancestor":   "manager_id",
		"descendant": "id",
	})
	if err := qPath.AddBranch(baseProj); err != nil {
		t.Fatal(err)
	}

	// 4. Execution
	// Find all descendants of Alice (id=1).
	// query: path(ancestor=1, descendant=X).
	key, err := ToKey("1")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"ancestor": NewBytesRange(key, key, true, true, nil),
	}
	seq, err := qPath.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	results := make(map[string]bool)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		// Expected results should have ancestor="1"
		ancestor, _ := row.Get("ancestor")
		if ancestor != "1" {
			t.Errorf("Expected ancestor=1, got %v", ancestor)
		}
		descendant, _ := row.Get("descendant")
		results[descendant.(string)] = true
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

// TestQuery_ComplexRecursiveBranches tests a scenario where two recursive relations depend on each other.
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
		"pre_start": "start",
		"start":     "end",
	}).Join(conns.Project(map[string]string{
		"start": "src",
		"end":   "dst",
	})).Project(map[string]string{
		"start": "pre_start",
		"end":   "end",
	}))

	// Rec 2: reach(x,z) + conn(y,z) -> reach(x,y) (undirected extension)
	// join on z (which is dst of conn)
	qReach.AddBranch(qReach.Project(map[string]string{
		"pre_start": "start",
		"start":     "end",
	}).Join(conns.Project(map[string]string{
		"start": "dst",
		"end":   "src",
	})).Project(map[string]string{
		"start": "pre_start",
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
	for i := range 16 {
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
		"u":  "u",
		"j1": "v",
	})
	e2 := edges.Project(map[string]string{
		"j1":    "u",
		"start": "v",
	})
	rec := qSkip.Project(map[string]string{
		"start": "start",
		"end":   "end",
	})

	// (E1 join E2) produces (start, j1, j2)
	// join Rec produces (start, j1, j2, end)
	// Project to (start, end)

	complexBranch := e1.Join(e2).Join(rec).Project(map[string]string{
		"start": "u",
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

// TestQuery_ComplexProjectionFilter tests a scenario where a complex chain of projections
// and filters might confuse the recursive engine's push-down logic.
func TestQuery_ComplexProjectionFilter(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Setup Schema
	// Table A: source of edges
	tableA, err := tx.CreatePersistent("table_a", map[string]ColumnSpec{
		"id":    {Indexed: true},
		"val_a": {Indexed: true},
		"next":  {Indexed: true}, // pointer to next A
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	// 1 (Green) -> 2 (Green) -> 3 (Red)
	tableA.Insert(map[string]any{"id": "1", "val_a": "Green", "next": "2"})
	tableA.Insert(map[string]any{"id": "2", "val_a": "Green", "next": "3"})
	tableA.Insert(map[string]any{"id": "3", "val_a": "Red", "next": ""})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	tableA, _ = tx.LoadPersistent("table_a")

	// Recursion: Path(start, end, color)
	// We want to find paths where ALL nodes have the same color?
	// Or just the path itself carries a color property.

	// Let's define Path(start, end, color)
	// Base: A(id=start, val_a=color, next=end)
	// Rec:  A(id=start, val_a=color, next=m) + Path(m, end, color)
	//
	// This enforces that the 'head' of the path matches the 'tail' of the path in color.
	// i.e. Homogeneous color path.

	qPath, err := tx.CreateRecursion("path", map[string]ColumnSpec{
		"start": {Indexed: true},
		"end":   {Indexed: true},
		"color": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Base
	// Project A: id->start, next->end, val_a->color
	base := tableA.Project(map[string]string{
		"start": "id",
		"end":   "next",
		"color": "val_a",
	})
	qPath.AddBranch(base)

	// Recursive Step
	// Join A and Path.
	// A(id=start, val_a=c1, next=m)
	// Path(start=m, end=end, color=c2)
	// Constraint: c1 = c2 = output_color.

	// Projection 1 (Table A): id->start, next->join, val_a->shared_color
	pA := tableA.Project(map[string]string{
		"start":        "id",
		"join":         "next",
		"shared_color": "val_a",
	})

	// Projection 2 (Path): start->join, end->end, color->shared_color
	pPath := qPath.Project(map[string]string{
		"join":         "start",
		"end":          "end",
		"shared_color": "color",
	})

	// Join on 'join' (A.next = Path.start) AND 'shared_color' (A.val_a = Path.color).
	// This is a multi-column join effectively, or implicit filter.
	// ThunderDB `Join` joins on matching column names.
	// So it joins on "join" and "shared_color".

	joined := pA.Join(pPath).Project(map[string]string{
		"start": "start",
		"end":   "end",
		"color": "shared_color",
	})

	qPath.AddBranch(joined)

	// Query: Find paths with color='Green' starting at 1.
	// Expected:
	// 1->2 (Green). Base case.
	// 1->2->3?
	//   Step 1: A(1). next=2, color=Green.
	//   Step 2: Path(2, 3, Green).
	//     Base Path(2, 3). A(2). next=3, color=Green.
	//     Matches!
	// So 1->3 should be found as a Green path.

	// Query: Find paths with color='Green' starting at 2.
	// Base: 2->3. A(2) is Green. Found.

	// Query: Find paths with color='Green' starting at 3.
	// Base: 3->?. A(3) is Red.
	// Filter color='Green' should prune this immediately.

	keyGreen, _ := ToKey("Green")
	f := map[string]*BytesRange{
		"color": NewBytesRange(keyGreen, keyGreen, true, true, nil),
	}

	seq, err := qPath.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	found := make(map[string]bool)
	for row := range seq {
		s, _ := row.Get("start")
		e, _ := row.Get("end")
		c, _ := row.Get("color")
		str := fmt.Sprintf("%s->%s (%s)", s, e, c)
		found[str] = true
	}

	// 1->2 (Green): Yes.
	if !found["1->2 (Green)"] {
		t.Errorf("Expected 1->2 (Green)")
	}
	// 2->3 (Green): Yes.
	if !found["2->3 (Green)"] {
		t.Errorf("Expected 2->3 (Green)")
	}
	// 1->3 (Green): Yes.
	// A(1)=Green. A(2)=Green.
	if !found["1->3 (Green)"] {
		t.Errorf("Expected 1->3 (Green)")
	}
}

// TestQuery_PushDown_DeepNestedAliases ensures that filters traverse multiple layers of projection
// correctly when aliases change at each level.
// Chain: T1(a) -> P1(b) -> P2(c) -> P3(d) -> Join -> Result.
// Filter on 'd'. Check if it reaches 'a'.
func TestQuery_PushDown_DeepNestedAliases(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	t1, err := tx.CreatePersistent("t1", map[string]ColumnSpec{
		"col_a": {Indexed: true},
		"id":    {},
	})
	if err != nil {
		t.Fatal(err)
	}
	t1.Insert(map[string]any{"id": "1", "col_a": "target"})
	t1.Insert(map[string]any{"id": "2", "col_a": "ignore"})
	tx.Commit()

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	t1, _ = tx.LoadPersistent("t1")

	// P1: col_a -> col_b
	p1 := t1.Project(map[string]string{"col_b": "col_a", "id": "id"})
	// P2: col_b -> col_c
	p2 := p1.Project(map[string]string{"col_c": "col_b", "id": "id"})
	// P3: col_c -> col_d
	p3 := p2.Project(map[string]string{"col_d": "col_c", "id": "id"})

	// Wrap in recursion to force engine usage (Select on pure projection might not use push down logic the same way?)
	// Actually `Project.Select` does do mapping.
	// But let's use a dummy recursion to be sure we hit `propagateUp` logic if relevant,
	// though `propagateUp` is for building results.
	// The Push Down happens in `explore` (implied) or `Select` call stack.

	// `Selector.Select(ranges)` calls `s.source.Select(mappedRanges)`.
	// This is standard recursive descent of the Select call.
	// The "Optimization" we fixed was specifically about `Recursion` passing these ranges
	// into the *recursive step* join.

	// So let's use a Recursion that uses P3 as a base.
	r, _ := tx.CreateRecursion("dummy", map[string]ColumnSpec{
		"final": {Indexed: true},
	})

	// Branch: P3 maps col_d -> final
	r.AddBranch(p3.Project(map[string]string{"final": "col_d"}))

	key, _ := ToKey("target")
	f := map[string]*BytesRange{
		"final": NewBytesRange(key, key, true, true, nil),
	}

	seq, err := r.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range seq {
		count++
	}
	if count != 1 {
		t.Errorf("Expected 1 result after deep aliasing, got %d", count)
	}
}

// TestQuery_FilterScopeConfusion tests the scenario where a user applies a filter intended for the
// destination (e.g., type=leaf) to a recursive definition that effectively maps that filter
// to intermediate nodes (e.g., type=branch) due to a projection choice.
//
// Graph: 1(Root) -> 2(Branch) -> 3(Leaf)
// Query: Find paths where "node_type" is "Leaf".
//
// Recursive Definition:
//
//	Path(start, end, node_type)
//	Branch 1 (Base): Edge(u, v) + Node(v). node_type = v.type.
//	Branch 2 (Rec):  Edge(u, m) + Node(m) + Path(m, v).
//	                 Projection maps m.type -> node_type (The user error).
//
// Expected Behavior:
//   - Path(2, 3) is found (Base case: 3 is Leaf).
//   - Path(1, 3) is MISSING.
//     Why? To form 1->3, we go 1->2 (m=2).
//     The query requests node_type='Leaf'.
//     The recursion maps node_type -> m.type.
//     So the engine filters for m.type='Leaf'.
//     m=2 has type='Branch'.
//     The join fails. Path 1->3 is pruned.
func TestQuery_FilterScopeConfusion(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Data
	nodes, err := tx.CreatePersistent("nodes", map[string]ColumnSpec{
		"id":   {Indexed: true},
		"type": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes.Insert(map[string]any{"id": "1", "type": "Root"})
	nodes.Insert(map[string]any{"id": "2", "type": "Branch"})
	nodes.Insert(map[string]any{"id": "3", "type": "Leaf"})

	edges, err := tx.CreatePersistent("edges", map[string]ColumnSpec{
		"src": {Indexed: true},
		"dst": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	edges.Insert(map[string]any{"src": "1", "dst": "2"})
	edges.Insert(map[string]any{"src": "2", "dst": "3"})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	nodes, _ = tx.LoadPersistent("nodes")
	edges, _ = tx.LoadPersistent("edges")

	// 2. Define Recursion
	qPath, err := tx.CreateRecursion("path", map[string]ColumnSpec{
		"start":     {Indexed: true},
		"end":       {Indexed: true},
		"node_type": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Base Case: Edge(u, v) -> Node(v).
	// We need to join Edge and Node.
	// E(src, dst) join N(id, type) on dst=id.
	base := edges.Project(map[string]string{
		"start": "src",
		"end":   "dst",
	}).Join(nodes.Project(map[string]string{
		"end":  "id",
		"type": "type",
	})).Project(map[string]string{
		"start":     "start",
		"end":       "end",  // dst is the end of this hop
		"node_type": "type", // Map destination type to output
	})
	qPath.AddBranch(base)

	// Recursive Step: Edge(u, m) -> Node(m) -> Path(m, v).
	// Mistake: We map m.type (intermediate) to output node_type.
	//
	// Join 1: Edge(src, dst) join Node(id, type) on dst=id. (Find intermediate node M)
	// Output: start=src, m=dst, m_type=type.
	step1 := edges.Project(map[string]string{
		"start": "src",
		"join":  "dst",
	}).Join(nodes.Project(map[string]string{
		"join":   "id",
		"m_type": "type",
	})).Project(map[string]string{
		"start":  "start",
		"m":      "join",
		"m_type": "m_type",
	})

	// Join 2: step1 join Path(start, end, node_type) on m=start.
	// We join 'm' (from step1) with 'start' (from Path).
	// We have 'm_type' available from step1.
	// We map 'm_type' to the final 'node_type'.
	recBranch := step1.Join(qPath.Project(map[string]string{
		"m":   "start",
		"end": "end",
		// We do NOT map Path.node_type here, so we ignore the tail's type.
	})).Project(map[string]string{
		"start":     "start",
		"end":       "end",
		"node_type": "m_type", // <--- THE MISTAKE: Mapping intermediate type to output
	})

	qPath.AddBranch(recBranch)

	// 3. Execute Query
	// We want paths where node_type = 'Leaf'.
	keyLeaf, _ := ToKey("Leaf")
	f := map[string]*BytesRange{
		"node_type": NewBytesRange(keyLeaf, keyLeaf, true, true, nil),
	}

	seq, err := qPath.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	foundPaths := make(map[string]bool)

	for row := range seq {
		start, _ := row.Get("start")
		end, _ := row.Get("end")
		nt, _ := row.Get("node_type")

		pathStr := start.(string) + "->" + end.(string) + "(" + nt.(string) + ")"
		t.Logf("Found path: %s", pathStr)
		foundPaths[pathStr] = true
	}

	// Expectation:
	// 2->3(Leaf): Found. Base case. Node(3).type is Leaf. Matches filter.
	// 1->3(Leaf): MISSING.
	//   Rec step: Edge(1,2), Node(2).type="Branch".
	//   Mapped to node_type="Branch".
	//   Filter node_type="Leaf" rejects it.

	if !foundPaths["2->3(Leaf)"] {
		t.Errorf("Expected path 2->3(Leaf) to be found, but it was missing.")
	}

	if foundPaths["1->3(Leaf)"] {
		t.Errorf("Did not expect path 1->3(Leaf). It should be filtered because intermediate node 2 is 'Branch'.")
	} else {
		t.Log("Correctly pruned 1->3 because the filter mapped to the intermediate node.")
	}
}

// TestQuery_MutualRecursion tests two recursive definitions that depend on each other.
// even(x) :- x=0.
// odd(x)  :- even(y), edge(y, x).
// even(x) :- odd(y), edge(y, x).
// Graph: 0 -> 1 -> 2 -> 3
// Result: Even={0, 2}, Odd={1, 3}
func TestQuery_MutualRecursion(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Data
	edges, err := tx.CreatePersistent("edges", map[string]ColumnSpec{
		"src": {Indexed: true},
		"dst": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	edges.Insert(map[string]any{"src": "0", "dst": "1"})
	edges.Insert(map[string]any{"src": "1", "dst": "2"})
	edges.Insert(map[string]any{"src": "2", "dst": "3"})

	// Also insert a disconnected odd path starting at 10 (base for odd) if we wanted,
	// but here 0 is the only true base.

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	edges, _ = tx.LoadPersistent("edges")

	// 2. Define Recursions
	qEven, err := tx.CreateRecursion("even", map[string]ColumnSpec{
		"val": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	qOdd, err := tx.CreateRecursion("odd", map[string]ColumnSpec{
		"val": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Base for Even: 0 (we can fake this as a single row relation or use a literal if supported,
	// but here we'll just create a dummy table or just say if edge.src='0' then '0' is even?
	// Let's create a 'roots' table for cleaner base case)
	roots, err := tx.CreatePersistent("roots", map[string]ColumnSpec{"val": {Indexed: true}})
	if err != nil {
		t.Fatal(err)
	}
	roots.Insert(map[string]any{"val": "0"})

	// Even Base: from roots
	qEven.AddBranch(roots.Project(map[string]string{"val": "val"}))

	// Odd Rec: Even(y) + Edge(y, x) -> Odd(x)
	// Join Even(val=y) and Edge(src=y, dst=x)
	oddBranch := qEven.Project(map[string]string{
		"join": "val",
	}).Join(edges.Project(map[string]string{
		"join": "src",
		"val":  "dst",
	})).Project(map[string]string{
		"val": "val",
	})
	qOdd.AddBranch(oddBranch)

	// Even Rec: Odd(y) + Edge(y, x) -> Even(x)
	evenBranch := qOdd.Project(map[string]string{
		"join": "val",
	}).Join(edges.Project(map[string]string{
		"join": "src",
		"val":  "dst",
	})).Project(map[string]string{
		"val": "val",
	})
	qEven.AddBranch(evenBranch)

	// 3. Execution
	// Select from Even
	// Should get 0 (base), 2 (from odd 1, from even 0)
	seqEven, err := qEven.Select(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	foundEven := make(map[string]bool)
	for row := range seqEven {
		val, _ := row.Get("val")
		foundEven[val.(string)] = true
	}

	if !foundEven["0"] {
		t.Error("Expected 0 in Even")
	}
	if !foundEven["2"] {
		t.Error("Expected 2 in Even")
	}
	if foundEven["1"] {
		t.Error("Did not expect 1 in Even")
	}
	if foundEven["3"] {
		t.Error("Did not expect 3 in Even")
	}

	// Select from Odd
	// Should get 1 (from even 0), 3 (from even 2)
	seqOdd, err := qOdd.Select(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	foundOdd := make(map[string]bool)
	for row := range seqOdd {
		val, _ := row.Get("val")
		foundOdd[val.(string)] = true
	}

	if !foundOdd["1"] {
		t.Error("Expected 1 in Odd")
	}
	if !foundOdd["3"] {
		t.Error("Expected 3 in Odd")
	}
	if foundOdd["0"] {
		t.Error("Did not expect 0 in Odd")
	}
	if foundOdd["2"] {
		t.Error("Did not expect 2 in Odd")
	}
}

type BadSelector struct {
	real linkedSelector
}

func (b *BadSelector) Select(ranges map[string]*BytesRange, refRanges map[string][]*RefRange) (iter.Seq2[Row, error], error) {
	// INTENTIONALLY IGNORE RANGES to simulate a "leaky" selector (e.g. Full Scan)
	return b.real.Select(nil, nil)
}

func (b *BadSelector) Columns() []string {
	return b.real.Columns()
}

func (b *BadSelector) IsRecursive() bool {
	return b.real.IsRecursive()
}

func (b *BadSelector) addParent(parent *queryParent) {
	b.real.addParent(parent)
}

func (b *BadSelector) parents() []*queryParent {
	return b.real.parents()
}

func (b *BadSelector) Project(mapping map[string]string) Selector {
	return newProjection(b, mapping)
}

func (b *BadSelector) Join(bodies ...Selector) Selector {
	// Not needed for this test, but satisfied Selector interface if we added it to interface?
	// Selector interface only has Select and Columns.
	// But in tests I might use Join.
	// For this test, I won't call Join on BadSelector.
	return nil
}

// TestQuery_Recursive_BackingStoreNoise ...
func TestQuery_Recursive_BackingStoreNoise(t *testing.T) {
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

	// 1. Insert Target Component: 1->2->3
	edges.Insert(map[string]any{"u": "1", "v": "2"})
	edges.Insert(map[string]any{"u": "2", "v": "3"})

	// 2. Insert Noise Component: 100 disconnected edges
	// 10->11, 12->13, ...
	noiseCount := 100
	for i := range noiseCount {
		u := fmt.Sprintf("%d", 10+i*2)
		v := fmt.Sprintf("%d", 10+i*2+1)
		edges.Insert(map[string]any{"u": u, "v": v})
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	defer tx.Rollback()
	edges, _ = tx.LoadPersistent("edges")

	// 3. Define Recursion: Path(start, end)
	qPath, err := tx.CreateRecursion("path", map[string]ColumnSpec{
		"start": {Indexed: true},
		"end":   {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Base: edge(u, v)
	baseProj := edges.Project(map[string]string{
		"start": "u",
		"end":   "v",
	})
	qPath.AddBranch(baseProj)

	// Rec: edge(u, m) + path(m, v)

	// We leave Rec branch normal.
	rec := edges.Project(map[string]string{
		"start": "u",
		"join":  "v",
	}).Join(qPath.Project(map[string]string{
		"join": "start",
		"end":  "end",
	})).Project(map[string]string{
		"start": "start",
		"end":   "end",
	})
	qPath.AddBranch(rec)

	// 4. Query from '1'
	key1, _ := ToKey("1")
	f := map[string]*BytesRange{
		"start": NewBytesRange(key1, key1, true, true, nil),
	}

	seq, err := qPath.Select(f, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Consume results
	count := 0
	for range seq {
		count++
	}

	// Access private backing store
	backingStore := qPath.backing
	iter, err := backingStore.Select(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	backingCount := 0
	for range iter {
		backingCount++
	}

	t.Logf("Result Count: %d", count)
	t.Logf("Backing Store Count: %d", backingCount)

	// We expect NOISE to be present if optimization is missing.
	// Noise count = 100.
	// Relevant count ~ 3.
	if backingCount > 3 {
		t.Errorf("Performance Leak: Backing store contains %d items, likely including noise. Expected < 10.", backingCount)
	}
}
