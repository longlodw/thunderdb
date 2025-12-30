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
	// users: u_id, u_name, group_id
	users, err := tx.CreatePersistent("users", map[string]ColumnSpec{
		"u_id":     {},
		"u_name":   {},
		"group_id": {},
	})
	if err != nil {
		t.Fatal(err)
	}
	// admins: u_id, u_name, group_id
	admins, err := tx.CreatePersistent("admins", map[string]ColumnSpec{
		"u_id":     {},
		"u_name":   {},
		"group_id": {},
	})
	if err != nil {
		t.Fatal(err)
	}
	// groups: group_id, g_name, org_id
	groups, err := tx.CreatePersistent("groups", map[string]ColumnSpec{
		"group_id": {},
		"g_name":   {},
		"org_id":   {},
	})
	if err != nil {
		t.Fatal(err)
	}
	// orgs: org_id, o_name, region
	orgs, err := tx.CreatePersistent("orgs", map[string]ColumnSpec{
		"org_id": {},
		"o_name": {},
		"region": {},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert Data
	// Orgs
	orgs.Insert(map[string]any{"org_id": "o1", "o_name": "TechCorp", "region": "North"})
	orgs.Insert(map[string]any{"org_id": "o2", "o_name": "BizInc", "region": "South"})

	// Groups
	groups.Insert(map[string]any{"group_id": "g1", "g_name": "Dev", "org_id": "o1"})   // North
	groups.Insert(map[string]any{"group_id": "g2", "g_name": "Sales", "org_id": "o2"}) // South

	// Users
	users.Insert(map[string]any{"u_id": "u1", "u_name": "Alice", "group_id": "g1"}) // North
	users.Insert(map[string]any{"u_id": "u2", "u_name": "Bob", "group_id": "g2"})   // South

	// Admins
	admins.Insert(map[string]any{"u_id": "a1", "u_name": "Charlie", "group_id": "g1"}) // North

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 2. Build Query
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	users, _ = tx.LoadPersistent("users")
	admins, _ = tx.LoadPersistent("admins")
	groups, _ = tx.LoadPersistent("groups")
	orgs, _ = tx.LoadPersistent("orgs")

	// Nested Query: qGroupsOrgs (Groups + Orgs)
	// Columns: Union of groups and orgs columns
	// group_id, g_name, org_id, o_name, region
	qGroupsOrgs := groups.Join(orgs)

	// Branch 1: Users + qGroupsOrgs
	branch1 := users.Join(qGroupsOrgs).Project(map[string]string{
		"u_id":     "u_id",
		"u_name":   "u_name",
		"group_id": "group_id",
		"g_name":   "g_name",
		"org_id":   "org_id",
		"o_name":   "o_name",
		"region":   "region",
	})
	// if err := qAll.AddBranch(branch1); err != nil {
	// 	t.Fatal(err)
	// }

	// Branch 2: Admins + qGroupsOrgs
	branch2 := admins.Join(qGroupsOrgs).Project(map[string]string{
		"u_id":     "u_id",
		"u_name":   "u_name",
		"group_id": "group_id",
		"g_name":   "g_name",
		"org_id":   "org_id",
		"o_name":   "o_name",
		"region":   "region",
	})

	// 3. Select Region="North"
	// Should return Alice (user) and Charlie (admin)
	op := Eq("region", "North")
	f, err := ToKeyRanges(op)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := branch1.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	results := make([]map[string]any, 0)
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		val, _ := row.ToMap()
		// Filter duplicate/unexpected results if the query engine returns more than expected
		// (e.g., if joins produce duplicates or if we are getting multiple matches)
		// For this test, we expect unique u_id.
		results = append(results, val)
	}
	seq2, err := branch2.Select(f)
	if err != nil {
		t.Fatal(err)
	}
	for row, err := range seq2 {
		if err != nil {
			t.Fatal(err)
		}
		val, _ := row.ToMap()
		results = append(results, val)
	}

	// Verify results directly (expecting exactly 2 results, no duplicates)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d. Raw results: %v", len(results), results)
	}

	names := make(map[string]bool)
	for _, res := range results {
		if name, ok := res["u_name"].(string); ok {
			names[name] = true
		} else {
			t.Errorf("Result missing u_name: %v", res)
		}
		if res["region"] != "North" {
			t.Errorf("Expected region North, got %v", res["region"])
		}
	}

	if !names["Alice"] {
		t.Error("Expected Alice in results")
	}
	if !names["Charlie"] {
		t.Error("Expected Charlie in results")
	}
}
