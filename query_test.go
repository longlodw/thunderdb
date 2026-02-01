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
	db, err := OpenDB(name, 0600, nil)
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

	users, err := tx.StoredQuery(usersRel)
	if err != nil {
		t.Fatal(err)
	}
	depts, err := tx.StoredQuery(deptRel)
	if err != nil {
		t.Fatal(err)
	}

	// Define a query that joins users and departments
	// We want to find users in 'engineering' and their location
	// Users: id(0), username(1), department(2)
	// Depts: department(0), location(1)
	// Join condition: users.department (2) == depts.department (0)
	q, err := users.Join(depts, JoinOn{LeftField: 2, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Execute Select on the Query
	// Filter by username 'alice'. Username is col 1 in users.
	seq, err := tx.Select(q, Condition{Field: 1, Operator: EQ, Value: "alice"})
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

	users, _ := tx.StoredQuery("users")
	admins, _ := tx.StoredQuery("admins")
	groups, _ := tx.StoredQuery("groups")
	orgs, _ := tx.StoredQuery("orgs")

	// Nested Query: qGroupsOrgs (Groups + Orgs)
	// Groups: 0:group_id, 1:g_name, 2:org_id
	// Orgs: 0:org_id, 1:o_name, 2:region
	// Join condition: groups.org_id (2) == orgs.org_id (0)
	qGroupsOrgs, err := groups.Join(orgs, JoinOn{LeftField: 2, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Result Cols: 0-2 (groups), 3-5 (orgs).
	// Org Region is at index 3+2 = 5.

	// Branch 1: Users + qGroupsOrgs
	// Users: 0:u_id, 1:u_name, 2:group_id
	// qGroupsOrgs: 0-2 (groups), 3-5 (orgs) -> will become 3-8 in final
	// Join condition: users.group_id (2) == groups.group_id (0 from right side)
	branch1, err := users.Join(qGroupsOrgs, JoinOn{LeftField: 2, RightField: 0, Operator: EQ})
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
	branch2, err := admins.Join(qGroupsOrgs, JoinOn{LeftField: 2, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	headQuery, err := tx.ClosureQuery(9, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Bind both branches to head query
	if err := headQuery.ClosedUnder(branch1, branch2); err != nil {
		t.Fatal(err)
	}

	// 3. Select Region="North"
	// Should return Alice (user) and Charlie (admin)
	seq, err := tx.Select(headQuery, Condition{Field: 8, Operator: EQ, Value: "North"})
	if err != nil {
		t.Fatal(err)
	}

	results := make([]string, 0)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		var name string
		err := row.Get(1, &name) // u_name
		if err != nil {
			t.Fatalf("failed to get name: %v", err)
		}
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
		{ReferencedCols: []int{0}, IsUnique: false},   // index on source
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

	nodes, err := tx.StoredQuery(nodesRel)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Define Recursive Query: "Reach(source, target)"
	// reach(X, Y) :- nodes(X, Y).
	// reach(X, Z) :- reach(X, Y), nodes(Y, Z).

	// Reach schema: source(0), target(1)
	qReach, err := tx.ClosureQuery(2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Base Case
	// reach(X, Y) :- nodes(X, Y).
	// Project nodes(0,1) -> reach(0,1)
	baseProj, err := nodes.Project(0, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Body 2: Recursive Case (Left Recursion)
	// reach(X, Z) :- reach(X, Y), nodes(Y, Z).
	// Join on Y: reach.target (1) == nodes.source (0)

	// Join condition: qReach.col1 == nodes.col0
	recJoin, err := qReach.Join(nodes, JoinOn{LeftField: 1, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Result of Join:
	// 0: reach.source (X)
	// 1: reach.target (Y)
	// 2: nodes.source (Y)
	// 3: nodes.target (Z)

	// We want Reach(X, Z) -> project cols 0 and 3
	recProj, err := recJoin.Project(0, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Bind the bodies to the Closure
	if err := qReach.ClosedUnder(baseProj, recProj); err != nil {
		t.Fatal(err)
	}

	// 4. Execution
	// Find all reachable nodes from A.
	// Expected: A -> B, B -> A, so reachable: B, A.
	// If cycle is not handled, this will loop A->B->A->B...
	seq, err := tx.Select(qReach, Condition{Field: 0, Operator: EQ, Value: "A"})
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
	// by the backing storage of the Closure node if implemented correctly.

	if count < 2 {
		t.Errorf("Expected at least 2 reachable paths, got %d. Results: %v", count, results)
	}

	foundA := false
	foundB := false
	for _, res := range results {
		if res == "A" {
			foundA = true
		}
		if res == "B" {
			foundB = true
		}
	}
	if !foundA {
		t.Error("Expected A to be reachable from A (A->B->A)")
	}
	if !foundB {
		t.Error("Expected B to be reachable from A (A->B)")
	}
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
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // manager_id
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert Data (Hierarchy)
	// Alice (1) -> Bob (2) -> Charlie (3) -> Dave (4)
	err = tx.Insert(employeesRel, map[int]any{0: "1", 1: "Alice", 2: ""})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Insert(employeesRel, map[int]any{0: "2", 1: "Bob", 2: "1"})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Insert(employeesRel, map[int]any{0: "3", 1: "Charlie", 2: "2"})
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Insert(employeesRel, map[int]any{0: "4", 1: "Dave", 2: "3"})
	if err != nil {
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

	employees, err := tx.StoredQuery(employeesRel)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Define Recursive Query: "Path(ancestor, descendant)"
	// path(a, c) :- edge(a, b), path(b, c).
	// OR simpler left-linear: path(a, b) :- edge(a, b).
	//                         path(a, c) :- edge(a, b), path(b, c).

	// Path schema: ancestor(0), descendant(1)
	qPath, err := tx.ClosureQuery(2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Base Case
	// path(a, b) :- employees(id:b, manager_id:a)
	// Project employees: manager_id(2) -> ancestor(0), id(0) -> descendant(1)
	baseProj, err := employees.Project(2, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Body 2: Recursive Case
	// path(a, c) :- employees(id:b, manager_id:a), path(ancestor:b, descendant:c)
	// Join on b: employees.id (0) == path.ancestor (0)

	// Join condition: employees.col0 == qPath.col0
	recJoin, err := employees.Join(qPath, JoinOn{LeftField: 0, RightField: 0, Operator: EQ})
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
	recProj, err := recJoin.Project(2, 4)
	if err != nil {
		t.Fatal(err)
	}

	// Bind bodies
	if err := qPath.ClosedUnder(baseProj, recProj); err != nil {
		t.Fatal(err)
	}

	// 4. Execution
	// Find all descendants of Alice (id=1).
	// query: path(ancestor=1, descendant=X).
	seq, err := tx.Select(qPath, Condition{Field: 0, Operator: EQ, Value: "1"})
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

func TestQuery_MutualRecursion(t *testing.T) {
	// Enforce a timeout to detect infinite loops
	done := make(chan bool)
	go func() {
		testQuery_MutualRecursion_Body(t)
		done <- true
	}()

	select {
	case <-done:
		// Test completed
	case <-time.After(3 * time.Second):
		t.Fatal("Test timed out - likely infinite recursion loop due to mutual recursion")
	}
}

// TestQuery_MultiColumnIndex tests queries with composite (multi-column) indexes
func TestQuery_MultiColumnIndex(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: products(id, category, subcategory, price, name)
	// Composite index on (category, subcategory) to speed up category browsing
	productsRel := "products"
	err = tx.CreateStorage(productsRel, 5, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},     // id unique
		{ReferencedCols: []int{1, 2}, IsUnique: false}, // (category, subcategory) composite
		{ReferencedCols: []int{1}, IsUnique: false},    // category alone
		{ReferencedCols: []int{3}, IsUnique: false},    // price for range queries
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	testProducts := []map[int]any{
		{0: "p1", 1: "electronics", 2: "phones", 3: int64(500), 4: "iPhone"},
		{0: "p2", 1: "electronics", 2: "phones", 3: int64(400), 4: "Android"},
		{0: "p3", 1: "electronics", 2: "laptops", 3: int64(1000), 4: "MacBook"},
		{0: "p4", 1: "electronics", 2: "laptops", 3: int64(800), 4: "ThinkPad"},
		{0: "p5", 1: "clothing", 2: "shirts", 3: int64(50), 4: "T-Shirt"},
		{0: "p6", 1: "clothing", 2: "pants", 3: int64(80), 4: "Jeans"},
		{0: "p7", 1: "clothing", 2: "shirts", 3: int64(100), 4: "Dress Shirt"},
	}
	for _, p := range testProducts {
		if err := tx.Insert(productsRel, p); err != nil {
			t.Fatalf("Failed to insert product: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Query tests
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	products, err := tx.StoredQuery(productsRel)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Query with both columns of composite index (category=electronics, subcategory=phones)
	t.Run("CompositeIndexBothColumns", func(t *testing.T) {
		seq, err := tx.Select(products,
			Condition{Field: 1, Operator: EQ, Value: "electronics"},
			Condition{Field: 2, Operator: EQ, Value: "phones"},
		)
		if err != nil {
			t.Fatal(err)
		}

		var names []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var name string
			if err := row.Get(4, &name); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
		}

		if len(names) != 2 {
			t.Errorf("Expected 2 results, got %d: %v", len(names), names)
		}
	})

	// Test 2: Query with first column of composite index only
	t.Run("CompositeIndexFirstColumnOnly", func(t *testing.T) {
		seq, err := tx.Select(products,
			Condition{Field: 1, Operator: EQ, Value: "electronics"},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		if count != 4 {
			t.Errorf("Expected 4 electronics products, got %d", count)
		}
	})

	// Test 3: Query combining composite index with another condition
	t.Run("CompositeIndexWithAdditionalFilter", func(t *testing.T) {
		seq, err := tx.Select(products,
			Condition{Field: 1, Operator: EQ, Value: "electronics"},
			Condition{Field: 2, Operator: EQ, Value: "laptops"},
			Condition{Field: 3, Operator: GT, Value: int64(900)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var names []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var name string
			if err := row.Get(4, &name); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
		}

		if len(names) != 1 || names[0] != "MacBook" {
			t.Errorf("Expected only MacBook (price > 900), got: %v", names)
		}
	})
}

// TestQuery_LessThanConditions tests LT and LTE filtering
func TestQuery_LessThanConditions(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: scores(id, player, score, level)
	scoresRel := "scores"
	err = tx.CreateStorage(scoresRel, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // score index for range queries
		{ReferencedCols: []int{3}, IsUnique: false}, // level index
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data with various scores
	testScores := []map[int]any{
		{0: "s1", 1: "Alice", 2: int64(100), 3: int64(1)},
		{0: "s2", 1: "Bob", 2: int64(200), 3: int64(2)},
		{0: "s3", 1: "Charlie", 2: int64(300), 3: int64(2)},
		{0: "s4", 1: "Diana", 2: int64(400), 3: int64(3)},
		{0: "s5", 1: "Eve", 2: int64(500), 3: int64(3)},
		{0: "s6", 1: "Frank", 2: int64(150), 3: int64(1)},
	}
	for _, s := range testScores {
		if err := tx.Insert(scoresRel, s); err != nil {
			t.Fatalf("Failed to insert score: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	scores, err := tx.StoredQuery(scoresRel)
	if err != nil {
		t.Fatal(err)
	}

	// Test LT: scores < 200
	t.Run("LessThan", func(t *testing.T) {
		seq, err := tx.Select(scores,
			Condition{Field: 2, Operator: LT, Value: int64(200)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var players []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var player string
			if err := row.Get(1, &player); err != nil {
				t.Fatal(err)
			}
			players = append(players, player)
		}

		// Expect Alice (100) and Frank (150)
		if len(players) != 2 {
			t.Errorf("Expected 2 players with score < 200, got %d: %v", len(players), players)
		}
	})

	// Test LTE: scores <= 200
	t.Run("LessThanOrEqual", func(t *testing.T) {
		seq, err := tx.Select(scores,
			Condition{Field: 2, Operator: LTE, Value: int64(200)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var players []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var player string
			if err := row.Get(1, &player); err != nil {
				t.Fatal(err)
			}
			players = append(players, player)
		}

		// Expect Alice (100), Frank (150), Bob (200)
		if len(players) != 3 {
			t.Errorf("Expected 3 players with score <= 200, got %d: %v", len(players), players)
		}
	})

	// Test LT with additional EQ condition
	t.Run("LessThanWithEquality", func(t *testing.T) {
		seq, err := tx.Select(scores,
			Condition{Field: 2, Operator: LT, Value: int64(350)},
			Condition{Field: 3, Operator: EQ, Value: int64(2)}, // level = 2
		)
		if err != nil {
			t.Fatal(err)
		}

		var players []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var player string
			if err := row.Get(1, &player); err != nil {
				t.Fatal(err)
			}
			players = append(players, player)
		}

		// Expect Bob (200, level 2) and Charlie (300, level 2)
		if len(players) != 2 {
			t.Errorf("Expected 2 players (level 2, score < 350), got %d: %v", len(players), players)
		}
	})
}

// TestQuery_GreaterThanConditions tests GT and GTE filtering
func TestQuery_GreaterThanConditions(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: inventory(id, product, quantity, warehouse)
	inventoryRel := "inventory"
	err = tx.CreateStorage(inventoryRel, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // quantity for range queries
		{ReferencedCols: []int{3}, IsUnique: false}, // warehouse
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	testItems := []map[int]any{
		{0: "i1", 1: "Widget A", 2: int64(10), 3: "warehouse1"},
		{0: "i2", 1: "Widget B", 2: int64(50), 3: "warehouse1"},
		{0: "i3", 1: "Gadget X", 2: int64(100), 3: "warehouse2"},
		{0: "i4", 1: "Gadget Y", 2: int64(200), 3: "warehouse2"},
		{0: "i5", 1: "Tool Z", 2: int64(75), 3: "warehouse1"},
		{0: "i6", 1: "Part Q", 2: int64(150), 3: "warehouse2"},
	}
	for _, item := range testItems {
		if err := tx.Insert(inventoryRel, item); err != nil {
			t.Fatalf("Failed to insert item: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	inventory, err := tx.StoredQuery(inventoryRel)
	if err != nil {
		t.Fatal(err)
	}

	// Test GT: quantity > 100
	t.Run("GreaterThan", func(t *testing.T) {
		seq, err := tx.Select(inventory,
			Condition{Field: 2, Operator: GT, Value: int64(100)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var products []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var product string
			if err := row.Get(1, &product); err != nil {
				t.Fatal(err)
			}
			products = append(products, product)
		}

		// Expect Gadget Y (200) and Part Q (150)
		if len(products) != 2 {
			t.Errorf("Expected 2 items with quantity > 100, got %d: %v", len(products), products)
		}
	})

	// Test GTE: quantity >= 100
	t.Run("GreaterThanOrEqual", func(t *testing.T) {
		seq, err := tx.Select(inventory,
			Condition{Field: 2, Operator: GTE, Value: int64(100)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var products []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var product string
			if err := row.Get(1, &product); err != nil {
				t.Fatal(err)
			}
			products = append(products, product)
		}

		// Expect Gadget X (100), Gadget Y (200), Part Q (150)
		if len(products) != 3 {
			t.Errorf("Expected 3 items with quantity >= 100, got %d: %v", len(products), products)
		}
	})

	// Test GT with warehouse filter
	t.Run("GreaterThanWithEquality", func(t *testing.T) {
		seq, err := tx.Select(inventory,
			Condition{Field: 2, Operator: GT, Value: int64(25)},
			Condition{Field: 3, Operator: EQ, Value: "warehouse1"},
		)
		if err != nil {
			t.Fatal(err)
		}

		var products []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var product string
			if err := row.Get(1, &product); err != nil {
				t.Fatal(err)
			}
			products = append(products, product)
		}

		// Expect Widget B (50) and Tool Z (75) in warehouse1
		if len(products) != 2 {
			t.Errorf("Expected 2 items (warehouse1, quantity > 25), got %d: %v", len(products), products)
		}
	})
}

// TestQuery_CombinedRangeConditions tests combining LT/LTE and GT/GTE in the same query
func TestQuery_CombinedRangeConditions(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: temperatures(id, city, temp, date)
	tempsRel := "temperatures"
	err = tx.CreateStorage(tempsRel, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // temp for range queries
		{ReferencedCols: []int{1}, IsUnique: false}, // city
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data: temperatures in various ranges
	testTemps := []map[int]any{
		{0: "t1", 1: "CityA", 2: int64(-10), 3: "2024-01-01"},
		{0: "t2", 1: "CityA", 2: int64(5), 3: "2024-02-01"},
		{0: "t3", 1: "CityA", 2: int64(20), 3: "2024-03-01"},
		{0: "t4", 1: "CityB", 2: int64(15), 3: "2024-01-01"},
		{0: "t5", 1: "CityB", 2: int64(25), 3: "2024-02-01"},
		{0: "t6", 1: "CityB", 2: int64(35), 3: "2024-03-01"},
		{0: "t7", 1: "CityC", 2: int64(10), 3: "2024-01-01"},
		{0: "t8", 1: "CityC", 2: int64(22), 3: "2024-02-01"},
	}
	for _, temp := range testTemps {
		if err := tx.Insert(tempsRel, temp); err != nil {
			t.Fatalf("Failed to insert temp: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	temps, err := tx.StoredQuery(tempsRel)
	if err != nil {
		t.Fatal(err)
	}

	// Test: 10 <= temp <= 25 (range query)
	t.Run("BetweenRangeInclusive", func(t *testing.T) {
		seq, err := tx.Select(temps,
			Condition{Field: 2, Operator: GTE, Value: int64(10)},
			Condition{Field: 2, Operator: LTE, Value: int64(25)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var cities []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var city string
			if err := row.Get(1, &city); err != nil {
				t.Fatal(err)
			}
			cities = append(cities, city)
		}

		// Expect: CityA(20), CityB(15), CityB(25), CityC(10), CityC(22)
		if len(cities) != 5 {
			t.Errorf("Expected 5 records with 10 <= temp <= 25, got %d: %v", len(cities), cities)
		}
	})

	// Test: 10 < temp < 25 (exclusive range)
	t.Run("BetweenRangeExclusive", func(t *testing.T) {
		seq, err := tx.Select(temps,
			Condition{Field: 2, Operator: GT, Value: int64(10)},
			Condition{Field: 2, Operator: LT, Value: int64(25)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var cities []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var city string
			if err := row.Get(1, &city); err != nil {
				t.Fatal(err)
			}
			cities = append(cities, city)
		}

		// Expect: CityA(20), CityB(15), CityC(22) -- excludes 10 and 25
		if len(cities) != 3 {
			t.Errorf("Expected 3 records with 10 < temp < 25, got %d: %v", len(cities), cities)
		}
	})

	// Test: range + equality filter on city
	t.Run("RangeWithCityFilter", func(t *testing.T) {
		seq, err := tx.Select(temps,
			Condition{Field: 2, Operator: GTE, Value: int64(0)},
			Condition{Field: 2, Operator: LT, Value: int64(30)},
			Condition{Field: 1, Operator: EQ, Value: "CityB"},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		// Expect: CityB(15), CityB(25) -- 35 is excluded
		if count != 2 {
			t.Errorf("Expected 2 CityB records with 0 <= temp < 30, got %d", count)
		}
	})

	// Test: multiple conditions that result in empty set
	t.Run("EmptyResultRange", func(t *testing.T) {
		seq, err := tx.Select(temps,
			Condition{Field: 2, Operator: GT, Value: int64(100)},
			Condition{Field: 2, Operator: LT, Value: int64(50)},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		// No temps > 100 AND < 50 (impossible range)
		if count != 0 {
			t.Errorf("Expected 0 results for impossible range, got %d", count)
		}
	})
}

// TestQuery_JoinWithRangeConditions tests joins combined with range filters
func TestQuery_JoinWithRangeConditions(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: orders(id, customer_id, total, status)
	ordersRel := "orders"
	err = tx.CreateStorage(ordersRel, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{1}, IsUnique: false}, // customer_id for joins
		{ReferencedCols: []int{2}, IsUnique: false}, // total for range queries
	})
	if err != nil {
		t.Fatal(err)
	}

	// Schema: customers(id, name, tier)
	customersRel := "customers"
	err = tx.CreateStorage(customersRel, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // tier
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert customers
	customers := []map[int]any{
		{0: "c1", 1: "Alice", 2: "gold"},
		{0: "c2", 1: "Bob", 2: "silver"},
		{0: "c3", 1: "Charlie", 2: "gold"},
	}
	for _, c := range customers {
		if err := tx.Insert(customersRel, c); err != nil {
			t.Fatalf("Failed to insert customer: %v", err)
		}
	}

	// Insert orders
	orders := []map[int]any{
		{0: "o1", 1: "c1", 2: int64(500), 3: "completed"},
		{0: "o2", 1: "c1", 2: int64(150), 3: "completed"},
		{0: "o3", 1: "c2", 2: int64(300), 3: "pending"},
		{0: "o4", 1: "c2", 2: int64(75), 3: "completed"},
		{0: "o5", 1: "c3", 2: int64(1000), 3: "completed"},
		{0: "o6", 1: "c3", 2: int64(200), 3: "pending"},
	}
	for _, o := range orders {
		if err := tx.Insert(ordersRel, o); err != nil {
			t.Fatalf("Failed to insert order: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	ordersQ, err := tx.StoredQuery(ordersRel)
	if err != nil {
		t.Fatal(err)
	}
	customersQ, err := tx.StoredQuery(customersRel)
	if err != nil {
		t.Fatal(err)
	}

	// Join orders with customers on customer_id
	// orders: id(0), customer_id(1), total(2), status(3)
	// customers: id(4), name(5), tier(6)
	joinedQ, err := ordersQ.Join(customersQ, JoinOn{LeftField: 1, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Test: Find all gold tier customers with orders > 200
	t.Run("JoinWithRangeAndEquality", func(t *testing.T) {
		seq, err := tx.Select(joinedQ,
			Condition{Field: 6, Operator: EQ, Value: "gold"},     // tier = gold
			Condition{Field: 2, Operator: GT, Value: int64(200)}, // total > 200
		)
		if err != nil {
			t.Fatal(err)
		}

		var results []struct {
			name  string
			total int64
		}
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var name string
			var total int64
			if err := row.Get(5, &name); err != nil {
				t.Fatal(err)
			}
			if err := row.Get(2, &total); err != nil {
				t.Fatal(err)
			}
			results = append(results, struct {
				name  string
				total int64
			}{name, total})
		}

		// Expect: Alice (500), Charlie (1000) -- both gold tier with orders > 200
		if len(results) != 2 {
			t.Errorf("Expected 2 results for gold tier orders > 200, got %d: %v", len(results), results)
		}
	})

	// Test: Orders between 100 and 400 for silver customers
	t.Run("JoinWithRangeBetween", func(t *testing.T) {
		seq, err := tx.Select(joinedQ,
			Condition{Field: 6, Operator: EQ, Value: "silver"},
			Condition{Field: 2, Operator: GTE, Value: int64(100)},
			Condition{Field: 2, Operator: LTE, Value: int64(400)},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		// Expect: Bob's order of 300 (75 is too low)
		if count != 1 {
			t.Errorf("Expected 1 silver tier order between 100-400, got %d", count)
		}
	})
}

// TestQuery_NEQConditions tests NEQ (not equal) filtering
func TestQuery_NEQConditions(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: tasks(id, title, status, priority)
	tasksRel := "tasks"
	err = tx.CreateStorage(tasksRel, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // status
		{ReferencedCols: []int{3}, IsUnique: false}, // priority
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	tasks := []map[int]any{
		{0: "t1", 1: "Task 1", 2: "open", 3: int64(1)},
		{0: "t2", 1: "Task 2", 2: "closed", 3: int64(2)},
		{0: "t3", 1: "Task 3", 2: "open", 3: int64(3)},
		{0: "t4", 1: "Task 4", 2: "in_progress", 3: int64(1)},
		{0: "t5", 1: "Task 5", 2: "closed", 3: int64(2)},
		{0: "t6", 1: "Task 6", 2: "open", 3: int64(1)},
	}
	for _, task := range tasks {
		if err := tx.Insert(tasksRel, task); err != nil {
			t.Fatalf("Failed to insert task: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	tasksQ, err := tx.StoredQuery(tasksRel)
	if err != nil {
		t.Fatal(err)
	}

	// Test: status != "closed"
	t.Run("NotEqualString", func(t *testing.T) {
		seq, err := tx.Select(tasksQ,
			Condition{Field: 2, Operator: NEQ, Value: "closed"},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		// Expect 4 tasks (all except the 2 closed ones)
		if count != 4 {
			t.Errorf("Expected 4 non-closed tasks, got %d", count)
		}
	})

	// Test: priority != 1 AND status != "closed"
	t.Run("MultipleNEQConditions", func(t *testing.T) {
		seq, err := tx.Select(tasksQ,
			Condition{Field: 3, Operator: NEQ, Value: int64(1)},
			Condition{Field: 2, Operator: NEQ, Value: "closed"},
		)
		if err != nil {
			t.Fatal(err)
		}

		var titles []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var title string
			if err := row.Get(1, &title); err != nil {
				t.Fatal(err)
			}
			titles = append(titles, title)
		}

		// Expect Task 3 (open, priority 3) only
		// Task 2 and 5 are closed, Task 1, 4, 6 have priority 1
		if len(titles) != 1 || titles[0] != "Task 3" {
			t.Errorf("Expected only 'Task 3', got: %v", titles)
		}
	})

	// Test: NEQ combined with range
	t.Run("NEQWithRange", func(t *testing.T) {
		seq, err := tx.Select(tasksQ,
			Condition{Field: 2, Operator: NEQ, Value: "closed"},
			Condition{Field: 3, Operator: LTE, Value: int64(2)},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		// Expect: Task 1 (open, 1), Task 4 (in_progress, 1), Task 6 (open, 1)
		// Not closed AND priority <= 2
		if count != 3 {
			t.Errorf("Expected 3 tasks (not closed, priority <= 2), got %d", count)
		}
	})
}

// TestQuery_MultiColumnIndexWithRanges tests composite indexes with range queries
func TestQuery_MultiColumnIndexWithRanges(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema: events(id, year, month, day, description)
	// Composite index on (year, month) for date range queries
	eventsRel := "events"
	err = tx.CreateStorage(eventsRel, 5, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},     // id
		{ReferencedCols: []int{1, 2}, IsUnique: false}, // (year, month) composite
		{ReferencedCols: []int{1}, IsUnique: false},    // year alone
		{ReferencedCols: []int{2}, IsUnique: false},    // month alone
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	events := []map[int]any{
		{0: "e1", 1: int64(2023), 2: int64(1), 3: int64(15), 4: "New Year Event"},
		{0: "e2", 1: int64(2023), 2: int64(6), 3: int64(1), 4: "Summer Start"},
		{0: "e3", 1: int64(2023), 2: int64(12), 3: int64(25), 4: "Holiday"},
		{0: "e4", 1: int64(2024), 2: int64(1), 3: int64(1), 4: "New Year 2024"},
		{0: "e5", 1: int64(2024), 2: int64(3), 3: int64(20), 4: "Spring Event"},
		{0: "e6", 1: int64(2024), 2: int64(6), 3: int64(15), 4: "Summer 2024"},
		{0: "e7", 1: int64(2024), 2: int64(9), 3: int64(1), 4: "Fall Event"},
	}
	for _, e := range events {
		if err := tx.Insert(eventsRel, e); err != nil {
			t.Fatalf("Failed to insert event: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	eventsQ, err := tx.StoredQuery(eventsRel)
	if err != nil {
		t.Fatal(err)
	}

	// Test: year = 2024 AND month >= 3 AND month <= 9
	t.Run("CompositeIndexWithYearEqMonthRange", func(t *testing.T) {
		seq, err := tx.Select(eventsQ,
			Condition{Field: 1, Operator: EQ, Value: int64(2024)},
			Condition{Field: 2, Operator: GTE, Value: int64(3)},
			Condition{Field: 2, Operator: LTE, Value: int64(9)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var descriptions []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var desc string
			if err := row.Get(4, &desc); err != nil {
				t.Fatal(err)
			}
			descriptions = append(descriptions, desc)
		}

		// Expect: Spring Event (Mar), Summer 2024 (Jun), Fall Event (Sep)
		if len(descriptions) != 3 {
			t.Errorf("Expected 3 events in 2024 (months 3-9), got %d: %v", len(descriptions), descriptions)
		}
	})

	// Test: year range with month filter
	t.Run("YearRangeWithMonthEq", func(t *testing.T) {
		seq, err := tx.Select(eventsQ,
			Condition{Field: 1, Operator: GTE, Value: int64(2023)},
			Condition{Field: 1, Operator: LTE, Value: int64(2024)},
			Condition{Field: 2, Operator: EQ, Value: int64(6)},
		)
		if err != nil {
			t.Fatal(err)
		}

		var descriptions []string
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var desc string
			if err := row.Get(4, &desc); err != nil {
				t.Fatal(err)
			}
			descriptions = append(descriptions, desc)
		}

		// Expect: Summer Start (2023/6), Summer 2024 (2024/6)
		if len(descriptions) != 2 {
			t.Errorf("Expected 2 June events (2023-2024), got %d: %v", len(descriptions), descriptions)
		}
	})

	// Test: only year range (using partial composite index)
	t.Run("YearRangeOnly", func(t *testing.T) {
		seq, err := tx.Select(eventsQ,
			Condition{Field: 1, Operator: GT, Value: int64(2023)},
		)
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}

		// Expect: 4 events in 2024
		if count != 4 {
			t.Errorf("Expected 4 events after 2023, got %d", count)
		}
	})
}

// TestQuery_MergeJoin tests that merge join is used when both sides have appropriate indexes
func TestQuery_MergeJoin(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create two tables with indexes on join columns
	// Table A: id, value_a, join_key
	tableA := "table_a"
	err = tx.CreateStorage(tableA, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // join_key for merge join
	})
	if err != nil {
		t.Fatal(err)
	}

	// Table B: id, value_b, join_key
	tableB := "table_b"
	err = tx.CreateStorage(tableB, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // join_key for merge join
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data with various join keys
	testDataA := []map[int]any{
		{0: "a1", 1: "value_a1", 2: "key1"},
		{0: "a2", 1: "value_a2", 2: "key2"},
		{0: "a3", 1: "value_a3", 2: "key2"}, // duplicate key
		{0: "a4", 1: "value_a4", 2: "key3"},
		{0: "a5", 1: "value_a5", 2: "key5"}, // no match in B
	}
	for _, data := range testDataA {
		if err := tx.Insert(tableA, data); err != nil {
			t.Fatalf("Failed to insert into table A: %v", err)
		}
	}

	testDataB := []map[int]any{
		{0: "b1", 1: "value_b1", 2: "key1"},
		{0: "b2", 1: "value_b2", 2: "key2"},
		{0: "b3", 1: "value_b3", 2: "key2"}, // duplicate key
		{0: "b4", 1: "value_b4", 2: "key4"}, // no match in A
	}
	for _, data := range testDataB {
		if err := tx.Insert(tableB, data); err != nil {
			t.Fatalf("Failed to insert into table B: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Query with merge join
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	tableAQ, err := tx.StoredQuery(tableA)
	if err != nil {
		t.Fatal(err)
	}
	tableBQ, err := tx.StoredQuery(tableB)
	if err != nil {
		t.Fatal(err)
	}

	// Join on join_key: A.col2 == B.col2
	joinedQ, err := tableAQ.Join(tableBQ, JoinOn{LeftField: 2, RightField: 2, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Execute join (should use merge join internally)
	seq, err := tx.Select(joinedQ)
	if err != nil {
		t.Fatal(err)
	}

	// Collect results
	results := make([]struct {
		aValue  string
		bValue  string
		joinKey string
	}, 0)

	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		var aValue, bValue, joinKey string
		if err := row.Get(1, &aValue); err != nil {
			t.Fatal(err)
		}
		if err := row.Get(4, &bValue); err != nil {
			t.Fatal(err)
		}
		if err := row.Get(2, &joinKey); err != nil {
			t.Fatal(err)
		}
		results = append(results, struct {
			aValue  string
			bValue  string
			joinKey string
		}{aValue, bValue, joinKey})
	}

	// Verify results
	// Expected matches:
	// - key1: a1 x b1 = 1 match
	// - key2: (a2, a3) x (b2, b3) = 4 matches
	// Total: 5 matches
	if len(results) != 5 {
		t.Errorf("Expected 5 join results, got %d", len(results))
	}

	// Count matches by key
	keyCount := make(map[string]int)
	for _, r := range results {
		keyCount[r.joinKey]++
	}

	if keyCount["key1"] != 1 {
		t.Errorf("Expected 1 match for key1, got %d", keyCount["key1"])
	}
	if keyCount["key2"] != 4 {
		t.Errorf("Expected 4 matches for key2, got %d", keyCount["key2"])
	}
}

// TestQuery_MergeJoinComposite tests merge join with composite indexes
func TestQuery_MergeJoinComposite(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create tables with composite indexes
	// Table A: id, category, subcategory, value_a
	tableA := "orders"
	err = tx.CreateStorage(tableA, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},     // id
		{ReferencedCols: []int{1, 2}, IsUnique: false}, // (category, subcategory) composite
	})
	if err != nil {
		t.Fatal(err)
	}

	// Table B: id, category, subcategory, value_b
	tableB := "inventory"
	err = tx.CreateStorage(tableB, 4, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},     // id
		{ReferencedCols: []int{1, 2}, IsUnique: false}, // (category, subcategory) composite
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	ordersData := []map[int]any{
		{0: "o1", 1: "electronics", 2: "phones", 3: int64(100)},
		{0: "o2", 1: "electronics", 2: "laptops", 3: int64(200)},
		{0: "o3", 1: "clothing", 2: "shirts", 3: int64(50)},
	}
	for _, data := range ordersData {
		if err := tx.Insert(tableA, data); err != nil {
			t.Fatal(err)
		}
	}

	inventoryData := []map[int]any{
		{0: "i1", 1: "electronics", 2: "phones", 3: int64(500)},
		{0: "i2", 1: "electronics", 2: "laptops", 3: int64(300)},
		{0: "i3", 1: "clothing", 2: "pants", 3: int64(100)}, // no match
	}
	for _, data := range inventoryData {
		if err := tx.Insert(tableB, data); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Query
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	ordersQ, err := tx.StoredQuery(tableA)
	if err != nil {
		t.Fatal(err)
	}
	inventoryQ, err := tx.StoredQuery(tableB)
	if err != nil {
		t.Fatal(err)
	}

	// Join on both category and subcategory
	joinedQ, err := ordersQ.Join(inventoryQ,
		JoinOn{LeftField: 1, RightField: 1, Operator: EQ}, // category
		JoinOn{LeftField: 2, RightField: 2, Operator: EQ}, // subcategory
	)
	if err != nil {
		t.Fatal(err)
	}

	seq, err := tx.Select(joinedQ)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	matches := make(map[string]bool)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		var category, subcategory string
		if err := row.Get(1, &category); err != nil {
			t.Fatal(err)
		}
		if err := row.Get(2, &subcategory); err != nil {
			t.Fatal(err)
		}
		matches[category+":"+subcategory] = true
		count++
	}

	// Expected: 2 matches (electronics:phones, electronics:laptops)
	if count != 2 {
		t.Errorf("Expected 2 composite key matches, got %d", count)
	}

	if !matches["electronics:phones"] {
		t.Error("Expected match for electronics:phones")
	}
	if !matches["electronics:laptops"] {
		t.Error("Expected match for electronics:laptops")
	}
}

// TestQuery_MergeJoinFallback tests that merge join correctly falls back to nested loop
// when indexes are not available or conditions are not suitable
func TestQuery_MergeJoinFallback(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create tables where left has index but right doesn't
	tableA := "table_a"
	err = tx.CreateStorage(tableA, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{2}, IsUnique: false}, // join_key indexed
	})
	if err != nil {
		t.Fatal(err)
	}

	tableB := "table_b"
	err = tx.CreateStorage(tableB, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true}, // id only (no index on join_key)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	if err := tx.Insert(tableA, map[int]any{0: "a1", 1: "val_a1", 2: "key1"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(tableB, map[int]any{0: "b1", 1: "val_b1", 2: "key1"}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Query - should fall back to nested loop since right table lacks index
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	tableAQ, err := tx.StoredQuery(tableA)
	if err != nil {
		t.Fatal(err)
	}
	tableBQ, err := tx.StoredQuery(tableB)
	if err != nil {
		t.Fatal(err)
	}

	joinedQ, err := tableAQ.Join(tableBQ, JoinOn{LeftField: 2, RightField: 2, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	seq, err := tx.Select(joinedQ)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	// Should still get correct result via nested loop
	if count != 1 {
		t.Errorf("Expected 1 result from fallback join, got %d", count)
	}
}

// TestQuery_MergeJoinNonEQOperator tests that merge join is not used when ONLY non-EQ operators exist
func TestQuery_MergeJoinNonEQOperator(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create tables with indexes
	tableA := "table_a"
	err = tx.CreateStorage(tableA, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},
		{ReferencedCols: []int{2}, IsUnique: false},
	})
	if err != nil {
		t.Fatal(err)
	}

	tableB := "table_b"
	err = tx.CreateStorage(tableB, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},
		{ReferencedCols: []int{2}, IsUnique: false},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	if err := tx.Insert(tableA, map[int]any{0: "a1", 1: "val_a1", 2: int64(10)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(tableB, map[int]any{0: "b1", 1: "val_b1", 2: int64(5)}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Query with GT operator - should fall back to nested loop
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	tableAQ, err := tx.StoredQuery(tableA)
	if err != nil {
		t.Fatal(err)
	}
	tableBQ, err := tx.StoredQuery(tableB)
	if err != nil {
		t.Fatal(err)
	}

	// Join with GT (greater than) operator
	joinedQ, err := tableAQ.Join(tableBQ, JoinOn{LeftField: 2, RightField: 2, Operator: GT})
	if err != nil {
		t.Fatal(err)
	}

	seq, err := tx.Select(joinedQ)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	// Should get correct result via nested loop (10 > 5 = true)
	if count != 1 {
		t.Errorf("Expected 1 result from GT join, got %d", count)
	}
}

// TestQuery_MergeJoinWithMixedOperators tests that merge join can handle mixed EQ and non-EQ operators.
// It should use merge join for the EQ condition and apply non-EQ conditions as post-filters.
func TestQuery_MergeJoinWithMixedOperators(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create orders table: id, category, price
	orders := "orders"
	err = tx.CreateStorage(orders, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{1}, IsUnique: false}, // category (indexed for merge join)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create products table: id, category, min_price
	products := "products"
	err = tx.CreateStorage(products, 3, []IndexInfo{
		{ReferencedCols: []int{0}, IsUnique: true},  // id
		{ReferencedCols: []int{1}, IsUnique: false}, // category (indexed for merge join)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert orders data
	// Order 1: Electronics, price 1000
	// Order 2: Electronics, price 500
	// Order 3: Books, price 30
	// Order 4: Books, price 50
	if err := tx.Insert(orders, map[int]any{0: "o1", 1: "Electronics", 2: int64(1000)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(orders, map[int]any{0: "o2", 1: "Electronics", 2: int64(500)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(orders, map[int]any{0: "o3", 1: "Books", 2: int64(30)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(orders, map[int]any{0: "o4", 1: "Books", 2: int64(50)}); err != nil {
		t.Fatal(err)
	}

	// Insert products data
	// Product 1: Electronics, min_price 600
	// Product 2: Books, min_price 40
	if err := tx.Insert(products, map[int]any{0: "p1", 1: "Electronics", 2: int64(600)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(products, map[int]any{0: "p2", 1: "Books", 2: int64(40)}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Test 1: EQ on category AND GT on price
	// Should use merge join on category, then filter by price > min_price
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	ordersQ, err := tx.StoredQuery(orders)
	if err != nil {
		t.Fatal(err)
	}
	productsQ, err := tx.StoredQuery(products)
	if err != nil {
		t.Fatal(err)
	}

	// Join: orders.category = products.category AND orders.price > products.min_price
	joinedQ, err := ordersQ.Join(productsQ,
		JoinOn{LeftField: 1, RightField: 1, Operator: EQ}, // category match
		JoinOn{LeftField: 2, RightField: 2, Operator: GT}, // price > min_price
	)
	if err != nil {
		t.Fatal(err)
	}

	seq, err := tx.Select(joinedQ)
	if err != nil {
		t.Fatal(err)
	}

	// Expected matches:
	// o1 (Electronics, 1000) + p1 (Electronics, 600): 1000 > 600 = TRUE ✓
	// o2 (Electronics, 500)  + p1 (Electronics, 600): 500 > 600 = FALSE ✗
	// o3 (Books, 30)         + p2 (Books, 40):        30 > 40 = FALSE ✗
	// o4 (Books, 50)         + p2 (Books, 40):        50 > 40 = TRUE ✓
	expectedMatches := map[string]bool{
		"o1": false, // Will be set to true when we see it
		"o4": false,
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++

		// Get order ID
		var orderID string
		if err := row.Get(0, &orderID); err != nil {
			t.Fatal(err)
		}

		if _, expected := expectedMatches[orderID]; !expected {
			t.Errorf("Unexpected order ID in results: %s", orderID)
		}
		expectedMatches[orderID] = true
	}

	// Should get 2 matches (o1 and o4)
	if count != 2 {
		t.Errorf("Expected 2 results from mixed operator join, got %d", count)
	}

	// Verify we saw all expected matches
	for id, seen := range expectedMatches {
		if !seen {
			t.Errorf("Expected to see order %s but didn't", id)
		}
	}

	// Test 2: Multiple non-EQ operators with EQ
	// Join: orders.category = products.category AND orders.price >= products.min_price
	joinedQ2, err := ordersQ.Join(productsQ,
		JoinOn{LeftField: 1, RightField: 1, Operator: EQ},  // category match
		JoinOn{LeftField: 2, RightField: 2, Operator: GTE}, // price >= min_price
	)
	if err != nil {
		t.Fatal(err)
	}

	seq2, err := tx.Select(joinedQ2)
	if err != nil {
		t.Fatal(err)
	}

	// Expected matches with GTE:
	// o1 (Electronics, 1000) + p1 (Electronics, 600): 1000 >= 600 = TRUE ✓
	// o2 (Electronics, 500)  + p1 (Electronics, 600): 500 >= 600 = FALSE ✗
	// o3 (Books, 30)         + p2 (Books, 40):        30 >= 40 = FALSE ✗
	// o4 (Books, 50)         + p2 (Books, 40):        50 >= 40 = TRUE ✓
	count2 := 0
	for _, err := range seq2 {
		if err != nil {
			t.Fatal(err)
		}
		count2++
	}

	// Should still get 2 matches (o1 and o4)
	if count2 != 2 {
		t.Errorf("Expected 2 results from GTE join, got %d", count2)
	}
}

func testQuery_MutualRecursion_Body(t *testing.T) {
	db, cleanup := setupTestDBForQuery(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 1. Setup Schema: Graph with bidirectional relationships
	// edges: from, to (directed edges)
	edgesRel := "edges"
	// 0: from, 1: to
	err = tx.CreateStorage(edgesRel, 2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true}, // (from, to) unique
		{ReferencedCols: []int{0}, IsUnique: false},   // index on from
		{ReferencedCols: []int{1}, IsUnique: false},   // index on to
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Insert Data (Graph with cycles)
	// A -> B, B -> C, C -> A (cycle)
	// A -> D, D -> E (chain)
	if err := tx.Insert(edgesRel, map[int]any{0: "A", 1: "B"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(edgesRel, map[int]any{0: "B", 1: "C"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(edgesRel, map[int]any{0: "C", 1: "A"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(edgesRel, map[int]any{0: "A", 1: "D"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Insert(edgesRel, map[int]any{0: "D", 1: "E"}); err != nil {
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

	edges, err := tx.StoredQuery(edgesRel)
	if err != nil {
		t.Fatal(err)
	}

	// 3. Define Mutually Recursive Queries
	// even_reach(X, Y) :- edges(X, Y).
	// even_reach(X, Z) :- odd_reach(X, Y), edges(Y, Z).
	// odd_reach(X, Y) :- edges(X, Z), even_reach(Z, Y).

	// Schema: from(0), to(1)
	qEvenReach, err := tx.ClosureQuery(2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	qOddReach, err := tx.ClosureQuery(2, []IndexInfo{
		{ReferencedCols: []int{0, 1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// even_reach base case: even_reach(X, Y) :- edges(X, Y)
	evenBase, err := edges.Project(0, 1)
	if err != nil {
		t.Fatal(err)
	}

	// even_reach recursive case: even_reach(X, Z) :- odd_reach(X, Y), edges(Y, Z)
	// Join on Y: odd_reach.to (1) == edges.from (0)
	evenRecJoin, err := qOddReach.Join(edges, JoinOn{LeftField: 1, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Project odd_reach.from (0) and edges.to (3) -> even_reach(X, Z)
	evenRecProj, err := evenRecJoin.Project(0, 3)
	if err != nil {
		t.Fatal(err)
	}

	// odd_reach case: odd_reach(X, Y) :- edges(X, Z), even_reach(Z, Y)
	// Join on Z: edges.to (1) == even_reach.from (0)
	oddRecJoin, err := edges.Join(qEvenReach, JoinOn{LeftField: 1, RightField: 0, Operator: EQ})
	if err != nil {
		t.Fatal(err)
	}

	// Project edges.from (0) and even_reach.to (3) -> odd_reach(X, Y)
	oddRecProj, err := oddRecJoin.Project(0, 3)
	if err != nil {
		t.Fatal(err)
	}

	// Bind bodies to create mutual recursion
	if err := qEvenReach.ClosedUnder(evenBase, evenRecProj); err != nil {
		t.Fatal(err)
	}
	if err := qOddReach.ClosedUnder(oddRecProj); err != nil {
		t.Fatal(err)
	}

	// 4. Execution
	// Find all nodes reachable from A in even number of steps
	// Expected: B (1 step), E (2 steps: A->D->E), A (3 steps: A->B->C->A)
	seq, err := tx.Select(qEvenReach, Condition{Field: 0, Operator: EQ, Value: "A"})
	if err != nil {
		t.Fatal(err)
	}

	results := make(map[string]bool)
	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++

		var from string
		if err := row.Get(0, &from); err != nil {
			t.Fatalf("failed to get from: %v", err)
		}
		if from != "A" {
			t.Errorf("Expected from=A, got %v", from)
		}

		var to string
		if err := row.Get(1, &to); err != nil {
			t.Fatalf("failed to get to: %v", err)
		}
		results[to] = true
	}

	// Should find B (direct), E (A->D->E), and A (A->B->C->A)
	if count < 3 {
		t.Errorf("Expected at least 3 even-reachable nodes, got %d. Results: %v", count, results)
	}

	expectedNodes := []string{"B", "E", "A"}
	for _, node := range expectedNodes {
		if !results[node] {
			t.Errorf("Expected node %s not found in even-reachable results", node)
		}
	}

	// 5. Test odd_reach as well
	// Find all nodes reachable from A in odd number of steps
	// Expected: D (1 step), C (2 steps: A->B->C)
	seqOdd, err := tx.Select(qOddReach, Condition{Field: 0, Operator: EQ, Value: "A"})
	if err != nil {
		t.Fatal(err)
	}

	oddResults := make(map[string]bool)
	oddCount := 0
	for row, err := range seqOdd {
		if err != nil {
			t.Fatal(err)
		}
		oddCount++

		var to string
		if err := row.Get(1, &to); err != nil {
			t.Fatalf("failed to get to: %v", err)
		}
		oddResults[to] = true
	}

	if oddCount < 2 {
		t.Errorf("Expected at least 2 odd-reachable nodes, got %d. Results: %v", oddCount, oddResults)
	}

	expectedOddNodes := []string{"D", "C"}
	for _, node := range expectedOddNodes {
		if !oddResults[node] {
			t.Errorf("Expected node %s not found in odd-reachable results", node)
		}
	}
}
