package thunder

import (
	"os"
	"testing"
)

func setupTestDB(t *testing.T) (*DB, func()) {
	tmpfile, err := os.CreateTemp("", "thunder_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()

	db, err := OpenDB(&JsonMaUn, dbPath, 0600, nil)
	if err != nil {
		os.Remove(dbPath)
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}
	return db, cleanup
}

func TestPersistent_BasicCRUD(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	basicCRUD_Insert(t, db)
	basicCRUD_SelectAlice(t, db)
	basicCRUD_DeleteBob(t, db)
	basicCRUD_VerifyDeleteBob(t, db)
}

func basicCRUD_Insert(t *testing.T, db *DB) {
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "users"
	columns := []string{"id", "username", "age"}
	indexes := map[string][]string{
		"username": {"username"},
	}

	p, err := tx.CreatePersistent(relation, columns, indexes)
	if err != nil {
		t.Fatal(err)
	}

	user1 := map[string]any{"id": "1", "username": "alice", "age": 30.0}
	if err := p.Insert(user1); err != nil {
		t.Fatal(err)
	}

	user2 := map[string]any{"id": "2", "username": "bob", "age": 25.0}
	if err := p.Insert(user2); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func basicCRUD_SelectAlice(t *testing.T, db *DB) {
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.LoadPersistent("users")
	if err != nil {
		t.Fatal(err)
	}

	op := Eq([]any{"alice"})
	op.Field = "username"
	seq, err := p.Select(op)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["username"] != "alice" {
			t.Errorf("Expected username alice, got %v", val["username"])
		}
		if val["age"] != 30.0 {
			t.Errorf("Expected age 30, got %v", val["age"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for alice, got %d", count)
	}
}

func basicCRUD_DeleteBob(t *testing.T, db *DB) {
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.LoadPersistent("users")
	if err != nil {
		t.Fatal(err)
	}

	op := Eq([]any{"bob"})
	op.Field = "username"
	if err := p.Delete(op); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func basicCRUD_VerifyDeleteBob(t *testing.T, db *DB) {
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.LoadPersistent("users")
	if err != nil {
		t.Fatal(err)
	}

	op := Eq([]any{"bob"})
	op.Field = "username"
	seq, err := p.Select(op)
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
	if count != 0 {
		t.Errorf("Expected 0 results for bob, got %d", count)
	}
}

func TestPersistent_NonIndexedSelect(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	nonIndexed_Insert(t, db)
	nonIndexed_Select(t, db)
}

func nonIndexed_Insert(t *testing.T, db *DB) {
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "items"
	columns := []string{"id", "price"}
	indexes := map[string][]string{}

	p, err := tx.CreatePersistent(relation, columns, indexes)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Insert(map[string]any{"id": "A", "price": 10.0}); err != nil {
		t.Fatal(err)
	}
	if err := p.Insert(map[string]any{"id": "B", "price": 20.0}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func nonIndexed_Select(t *testing.T, db *DB) {
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.LoadPersistent("items")
	if err != nil {
		t.Fatal(err)
	}

	op := Eq(20.0)
	op.Field = "price"
	seq, err := p.Select(op)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["id"] != "B" {
			t.Errorf("Expected item B, got %v", val["id"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for price 20, got %d", count)
	}
}

func TestPersistent_Projection(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	projection_Insert(t, db)
	projection_Select(t, db)
}

func projection_Insert(t *testing.T, db *DB) {
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "users"
	columns := []string{"id", "username", "age"}
	indexes := map[string][]string{
		"username": {"username"},
	}

	p, err := tx.CreatePersistent(relation, columns, indexes)
	if err != nil {
		t.Fatal(err)
	}

	user1 := map[string]any{"id": "1", "username": "alice", "age": 30.0}
	if err := p.Insert(user1); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func projection_Select(t *testing.T, db *DB) {
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.LoadPersistent("users")
	if err != nil {
		t.Fatal(err)
	}

	mapping := map[string]string{
		"id":       "user_id",
		"username": "login_name",
		"age":      "user_age",
	}
	proj, err := p.Project(mapping)
	if err != nil {
		t.Fatal(err)
	}

	op := Eq([]any{"alice"})
	op.Field = "login_name"
	seq, err := proj.Select(op)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["login_name"] != "alice" {
			t.Errorf("Expected login_name alice, got %v", val["login_name"])
		}
		if val["user_age"] != 30.0 {
			t.Errorf("Expected user_age 30, got %v", val["user_age"])
		}
		if _, ok := val["username"]; ok {
			t.Errorf("Did not expect original field username")
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}
