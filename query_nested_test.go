package thunderdb

import (
	"testing"
)

func TestQuery_DeeplyNestedAndMultipleBodies(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// 1. Setup Data
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Schema
	// users: u_id(0), u_name(1), group_id(2)
	err = tx.CreateStorage("users", []ColumnSpec{{},{},{}}, nil)
	if err != nil { t.Fatal(err) }
	
	// admins: u_id(0), u_name(1), group_id(2)
	err = tx.CreateStorage("admins", []ColumnSpec{{},{},{}}, nil)
	if err != nil { t.Fatal(err) }
	
	// groups: group_id(0), g_name(1), org_id(2)
	err = tx.CreateStorage("groups", []ColumnSpec{{},{},{}}, nil)
	if err != nil { t.Fatal(err) }
	
	// orgs: org_id(0), o_name(1), region(2)
	err = tx.CreateStorage("orgs", []ColumnSpec{{},{},{}}, nil)
	if err != nil { t.Fatal(err) }

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
	qGroupsOrgs := groups.Join(orgs, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})
	// Result Cols: 0-2 (groups), 3-5 (orgs). 
	// Org Region is at index 3+2 = 5.

	// Branch 1: Users + qGroupsOrgs
	// Users: 0:u_id, 1:u_name, 2:group_id
	// qGroupsOrgs: 0-2 (groups), 3-5 (orgs) -> will become 3-8 in final
	// Join condition: users.group_id (2) == groups.group_id (0 from right side)
	branch1 := users.Join(qGroupsOrgs, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})
	
	// Branch 1 Schema indices:
	// Users: 0, 1, 2
	// Groups: 3, 4, 5
	// Orgs: 6, 7, 8
	// Region is at 8.

	// Branch 2: Admins + qGroupsOrgs
	// Admins: 0:u_id, 1:u_name, 2:group_id
	// Same structure.
	branch2 := admins.Join(qGroupsOrgs, []JoinOn{
		{leftField: 2, rightField: 0, operator: EQ},
	})

	// 3. Select Region="North"
	// Should return Alice (user) and Charlie (admin)
	key, err := ToKey("North")
	if err != nil {
		t.Fatal(err)
	}
	f := map[int]*BytesRange{
		8: NewBytesRange(key, key, true, true, nil), // Region is at index 8
	}
	
	seq, err := tx.Query(branch1, f)
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

	seq2, err := tx.Query(branch2, f)
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
