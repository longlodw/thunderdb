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

	// Top Query: qAll (Users/Admins + qGroupsOrgs)
	// Columns: Union of users/admins and qGroupsOrgs
	// u_id, u_name, group_id, g_name, org_id, o_name, region

	// Body 1: Users + qGroupsOrgs
	// Body 2: Admins + qGroupsOrgs
	// Since we cannot add multiple bodies to a joining directly in the new API (it creates a new one),
	// we will run two separate queries and combine results or check individually.
	// For this test, let's verify each path individually or assume the user wants a single interface.
	// The original test tested adding multiple bodies to a single query.
	// To replicate "Union" behavior (Body 1 OR Body 2), we might not have a direct "Union" selector yet
	// unless Joining supports it or we use recursion.
	// However, looking at previous tests, "AddBody" on a Query added alternative paths (OR semantics).
	// Joining struct has `bodies []linkedSelector`.
	// The new `Join` method creates a `Joining` with multiple bodies, which implies AND semantics (joining them).
	// Wait, `Joining` struct has `bodies` which are joined together.
	// The old `Query` had `bodies` which were alternatives?
	// Let's check `Joining.Select`.
	// `func (jr *Joining) Select` iterates `jr.bodies[seedIdx].Select` then calls `jr.join`.
	// This implies AND semantics (Join).

	// The original test `qAll.AddBody(users, qGroupsOrgs)` and `qAll.AddBody(admins, qGroupsOrgs)`
	// implies qAll = (users JOIN qGroupsOrgs) UNION (admins JOIN qGroupsOrgs).
	// The new API seems to move towards explicit Join selectors.
	// If we want Union, we might need a different approach or just test the two joins separately.
	// For now, let's test the two joins separately as the "Union" might not be directly supported
	// without a "Union" selector or using Recursion (which supports multiple branches).

	// Let's use Recursion to simulate Union if needed, or just test separately.
	// Given the context "query no longer exists", and we have `Joining` and `Recursion`.
	// `Recursion` has `AddBranch`. Each branch is an alternative (OR).
	// So we can use `Recursion` to simulate the Union.

	qAll, err := tx.CreateRecursion("all_users_union", map[string]ColumnSpec{
		"u_id":     {},
		"u_name":   {},
		"group_id": {},
		"g_name":   {},
		"org_id":   {},
		"o_name":   {},
		"region":   {},
	})
	if err != nil {
		t.Fatal(err)
	}

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
	if err := qAll.AddBranch(branch1); err != nil {
		t.Fatal(err)
	}

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
	if err := qAll.AddBranch(branch2); err != nil {
		t.Fatal(err)
	}

	// 3. Select Region="North"
	// Should return Alice (user) and Charlie (admin)
	op := Eq("region", "North")
	f, err := ToKeyRanges(op)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := qAll.Select(f)
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
