package thunderdb

import (
	"os"
	"testing"
	"time"
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

func TestQuery_DeeplyNestedAndMultipleBodies(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	// 1. Setup Data
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema
	// users: u_id(0), u_name(1), group_id(2)
	err = tx.CreateStorage("users", 3, nil)
	if err != nil {
		t.Fatal(err)
	}

	// admins: u_id(0), u_name(1), group_id(2)
	err = tx.CreateStorage("admins", 3, nil)
	if err != nil {
		t.Fatal(err)
	}

	// groups: group_id(0), g_name(1), org_id(2)
	err = tx.CreateStorage("groups", 3, nil)
	if err != nil {
		t.Fatal(err)
	}

	// orgs: org_id(0), o_name(1), region(2)
	err = tx.CreateStorage("orgs", 3, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Insert Data
	// Orgs
	tx.Insert("orgs", map[int]any{0: "o1", 1: "TechCorp", 2: "North"})
	tx.Insert("orgs", map[int]any{0: "o2", 1: "BizInc", 2: "South"})

	// Groups
	tx.Insert("groups", map[int]any{0: "g1", 1: "Dev", 2: "o1"})   // North
	tx.Insert("groups", map[int]any{0: "g2", 1: "Sales", 2: "o2"}) // South

	// Users
	tx.Insert("users", map[int]any{0: "u1", 1: "Alice", 2: "g1"}) // North
	tx.Insert("users", map[int]any{0: "u2", 1: "Bob", 2: "g2"})   // South

	// Admins
	tx.Insert("admins", map[int]any{0: "a1", 1: "Charlie", 2: "g1"}) // North

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 2. Build Query
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	users, _ := tx.LoadStoredBody("users")
	admins, _ := tx.LoadStoredBody("admins")
	groups, _ := tx.LoadStoredBody("groups")
	orgs, _ := tx.LoadStoredBody("orgs")

	// Nested Query: qGroupsOrgs (Groups + Orgs)
	// Groups: 0:group_id, 1:g_name, 2:org_id
	// Orgs: 0:org_id, 1:o_name, 2:region
	// Join condition: groups.org_id (2) == orgs.org_id (0)
	qGroupsOrgs, err := groups.Join(orgs, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Result Cols: 0-2 (groups), 3-5 (orgs).
	// Org Region is at index 3+2 = 5.

	// Branch 1: Users + qGroupsOrgs
	// Users: 0:u_id, 1:u_name, 2:group_id
	// qGroupsOrgs: 0-2 (groups), 3-5 (orgs) -> will become 3-8 in final
	// Join condition: users.group_id (2) == groups.group_id (0 from right side)
	branch1, err := users.Join(qGroupsOrgs, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Branch 1 Schema indices:
	// Users: 0, 1, 2
	// Groups: 3, 4, 5
	// Orgs: 6, 7, 8
	// Region is at 8.

	// Branch 2: Admins + qGroupsOrgs
	// Admins: 0:u_id, 1:u_name, 2:group_id
	// Same structure.
	branch2, err := admins.Join(qGroupsOrgs, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 3. Select Region="North"
	// Should return Alice (user) and Charlie (admin)
	key, err := ToKey("North")
	if err != nil {
		t.Fatal(err)
	}
	f := map[int]*Range{
		8: func() *Range {
			r, _ := NewRangeFromBytes(key, key, true, true)
			return r
		}(), // Region is at index 8
	}

	seq, err := tx.Select(branch1, nil, f, nil)
	if err != nil {
		t.Fatal(err)
	}

	results := make([]string, 0)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		var name string
		row.Get(1, &name) // u_name
		results = append(results, name)
	}

	seq2, err := tx.Select(branch2, nil, f, nil)
	if err != nil {
		t.Fatal(err)
	}
	for row, err := range seq2 {
		if err != nil {
			t.Fatal(err)
		}
		var name string
		row.Get(1, &name) // u_name
		results = append(results, name)
	}

	// Verify results directly (expecting exactly 2 results, no duplicates)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d. Raw results: %v", len(results), results)
	}

	names := make(map[string]bool)
	for _, name := range results {
		names[name] = true
	}

	if !names["Alice"] {
		t.Error("Expected Alice in results")
	}
	if !names["Charlie"] {
		t.Error("Expected Charlie in results")
	}
}

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
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Schema: Node (source, target)
	// Graph: A -> B -> A (Cycle)
	nodesRel := "nodes"
	// 0: source, 1: target
	err = tx.CreateStorage(nodesRel, 2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true}, // (source, target) unique
		{ReferencedCols: []int{0}, IsUnique: false},    // index on source
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert Data (Cycle)
	// A -> B
	if err := tx.Insert(nodesRel, map[int]any{0: "A", 1: "B"}); err != nil {
		t.Fatal(err)
	}
	// B -> A
	if err := tx.Insert(nodesRel, map[int]any{0: "B", 1: "A"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	nodes, err := tx.LoadStoredBody(nodesRel)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Define Recursive Query: "Reach(source, target)"
	// reach(X, Y) :- nodes(X, Y).
	// reach(X, Z) :- reach(X, Y), nodes(Y, Z).

	// Reach schema: source(0), target(1)
	qReach, err := NewHeadQuery(2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Base Case
	// reach(X, Y) :- nodes(X, Y).
	// Project nodes(0,1) -> reach(0,1)
	baseProj, err := nodes.Project([]int{0, 1})
	if err != nil {
		t.Fatal(err)
	}

	// Body 2: Recursive Case (Left Recursion)
	// reach(X, Z) :- reach(X, Y), nodes(Y, Z).
	// Join on Y: reach.target (1) == nodes.source (0)
	
	// Join condition: qReach.col1 == nodes.col0
	recJoin, err := qReach.Join(nodes, []JoinOn{
		{leftField: 1, rightField: 0, operator: EQ},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Result of Join:
	// 0: reach.source (X)
	// 1: reach.target (Y)
	// 2: nodes.source (Y)
	// 3: nodes.target (Z)
	
	// We want Reach(X, Z) -> project cols 0 and 3
	recProj, err := recJoin.Project([]int{0, 3})
	if err != nil {
		t.Fatal(err)
	}

	// Bind the bodies to the HeadQuery
	qReach.Bind([]Query{baseProj, recProj})

	// 4. Execution
	// Find all reachable nodes from A.
	// Expected: A -> B, B -> A, so reachable: B, A.
	// If cycle is not handled, this will loop A->B->A->B...
	key, err := ToKey("A")
	if err != nil {
		t.Fatal(err)
	}
	f := map[int]*Range{
		0: func() *Range {
			r, _ := NewRangeFromBytes(key, key, true, true)
			return r
		}(), // source is at index 0
	}
	
	seq, err := tx.Select(qReach, nil, f, nil)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	results := make([]string, 0)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		
		var target string
		if err := row.Get(1, &target); err != nil {
			t.Fatalf("failed to get target: %v", err)
		}
		results = append(results, target)
	}

	// We expect at least 2 results (A->B, A->A).
	// Standard Datalog usually implies Set semantics, so 2 results if uniqueness is handled.
	// If Bag semantics (or just naive loop), it could be infinite, but we have a timeout.
	// Since we defined a Unique Index on qReach (source, target), duplicates should be suppressed 
	// by the backing storage of the HeadQuery node if implemented correctly.
	
	if count < 2 {
		t.Errorf("Expected at least 2 reachable paths, got %d. Results: %v", count, results)
	}
	
	foundA := false
	foundB := false
	for _, res := range results {
		if res == "A" { foundA = true }
		if res == "B" { foundB = true }
	}
	if !foundA { t.Error("Expected A to be reachable from A (A->B->A)") }
	if !foundB { t.Error("Expected B to be reachable from A (A->B)") }
}

// TestQuery_Recursive validates the recursive query logic using the new APIs.
// This test replaces the old TestQuery_Recursive from query_recursive_test.go.
func TestQuery_Recursive(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Schema: Employees (id, name, manager_id)
	employeesRel := "employees"
	// 0: id, 1: name, 2: manager_id
	err = tx.CreateStorage(employeesRel, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true}, // id
		{ReferencedCols: []int{2}, IsUnique: false}, // manager_id
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert Data (Hierarchy)
	// Alice (1) -> Bob (2) -> Charlie (3) -> Dave (4)
	tx.Insert(employeesRel, map[int]any{0: "1", 1: "Alice", 2: ""})
	tx.Insert(employeesRel, map[int]any{0: "2", 1: "Bob", 2: "1"})
	tx.Insert(employeesRel, map[int]any{0: "3", 1: "Charlie", 2: "2"})
	tx.Insert(employeesRel, map[int]any{0: "4", 1: "Dave", 2: "3"})

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	employees, err := tx.LoadStoredBody(employeesRel)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Define Recursive Query: "Path(ancestor, descendant)"
	// path(a, c) :- edge(a, b), path(b, c).
	// OR simpler left-linear: path(a, b) :- edge(a, b).
	//                         path(a, c) :- edge(a, b), path(b, c).

	// Path schema: ancestor(0), descendant(1)
	qPath, err := NewHeadQuery(2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Base Case
	// path(a, b) :- employees(id:b, manager_id:a)
	// Project employees: manager_id(2) -> ancestor(0), id(0) -> descendant(1)
	baseProj, err := employees.Project([]int{2, 0})
	if err != nil {
		t.Fatal(err)
	}

	// Body 2: Recursive Case
	// path(a, c) :- employees(id:b, manager_id:a), path(ancestor:b, descendant:c)
	// Join on b: employees.id (0) == path.ancestor (0)
	
	// Join condition: employees.col0 == qPath.col0
	recJoin, err := employees.Join(qPath, []JoinOn{
		{leftField: 0, rightField: 0, operator: EQ},
	})
	if err != nil {
		t.Fatal(err)
	}
	
	// Result of Join:
	// 0: employees.id (b)
	// 1: employees.name
	// 2: employees.manager_id (a)
	// 3: path.ancestor (b)
	// 4: path.descendant (c)

	// We want path(a, c) -> project cols 2 (a) and 4 (c)
	recProj, err := recJoin.Project([]int{2, 4})
	if err != nil {
		t.Fatal(err)
	}

	// Bind bodies
	qPath.Bind([]Query{baseProj, recProj})

	// 4. Execution
	// Find all descendants of Alice (id=1).
	// query: path(ancestor=1, descendant=X).
	key, err := ToKey("1")
	if err != nil {
		t.Fatal(err)
	}
	f := map[int]*Range{
		0: func() *Range {
			r, _ := NewRangeFromBytes(key, key, true, true)
			return r
		}(),
	}
	
	seq, err := tx.Select(qPath, nil, f, nil)
	if err != nil {
		t.Fatal(err)
	}

	results := make(map[string]bool)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		
		var ancestor string
		if err := row.Get(0, &ancestor); err != nil {
			t.Fatalf("failed to get ancestor: %v", err)
		}
		if ancestor != "1" {
			t.Errorf("Expected ancestor=1, got %v", ancestor)
		}
		
		var descendant string
		if err := row.Get(1, &descendant); err != nil {
			t.Fatalf("failed to get descendant: %v", err)
		}
		results[descendant] = true
	}

	// Expected descendants: Bob (2), Charlie (3), Dave (4)
	if len(results) != 3 {
		t.Errorf("Expected 3 descendants, got %d. Found: %v", len(results), results)
	}
	expectedDescendants := []string{"2", "3", "4"}
	for _, desc := range expectedDescendants {
		if !results[desc] {
			t.Errorf("Expected descendant %s not found in results", desc)
		}
	}
}
