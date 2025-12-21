package thunder

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
	users, err := tx.CreatePersistent("users", []string{"u_id", "u_name", "group_id"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// admins: u_id, u_name, group_id
	admins, err := tx.CreatePersistent("admins", []string{"u_id", "u_name", "group_id"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// groups: group_id, g_name, org_id
	groups, err := tx.CreatePersistent("groups", []string{"group_id", "g_name", "org_id"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// orgs: org_id, o_name, region
	orgs, err := tx.CreatePersistent("orgs", []string{"org_id", "o_name", "region"}, nil, nil)
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
	qGroupsOrgsCols := []string{"group_id", "g_name", "org_id", "o_name", "region"}
	qGroupsOrgs, err := tx.CreateQuery("groups_orgs", qGroupsOrgsCols, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := qGroupsOrgs.AddBody(groups, orgs); err != nil {
		t.Fatal(err)
	}

	// Top Query: qAll (Users/Admins + qGroupsOrgs)
	// Columns: Union of users/admins and qGroupsOrgs
	// u_id, u_name, group_id, g_name, org_id, o_name, region
	qAllCols := []string{"u_id", "u_name", "group_id", "g_name", "org_id", "o_name", "region"}
	qAll, err := tx.CreateQuery("all_users", qAllCols, false)
	if err != nil {
		t.Fatal(err)
	}

	// Body 1: Users + qGroupsOrgs
	if err := qAll.AddBody(users, qGroupsOrgs); err != nil {
		t.Fatal(err)
	}

	// Body 2: Admins + qGroupsOrgs
	if err := qAll.AddBody(admins, qGroupsOrgs); err != nil {
		t.Fatal(err)
	}

	// 3. Select Region="North"
	// Should return Alice (user) and Charlie (admin)
	op := Eq("region", "North")
	seq, err := qAll.Select(op)
	if err != nil {
		t.Fatal(err)
	}

	results := make([]map[string]any, 0)
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, val)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
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
