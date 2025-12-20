package thunder

import (
	"os"
	"testing"
)

func setupTestDB(t *testing.T) (*DB, func()) {
	return setupTestDBWithMaUn(t, &MsgpackMaUn)
}

func setupTestDBWithMaUn(t *testing.T, maUn MarshalUnmarshaler) (*DB, func()) {
	tmpfile, err := os.CreateTemp("", "thunder_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()

	db, err := OpenDB(maUn, dbPath, 0600, nil)
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

	op := Eq("username", []any{"alice"})
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

	op := Eq("username", []any{"bob"})
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

	op := Eq("username", []any{"bob"})
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

	op := Eq("price", 20.0)
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

	op := Eq("login_name", []any{"alice"})
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

func TestPersistent_DifferentOperators(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "products"
	columns := []string{"id", "price", "stock"}
	indexes := map[string][]string{} // Non-indexed for operator tests

	p, err := tx.CreatePersistent(relation, columns, indexes)
	if err != nil {
		t.Fatal(err)
	}

	products := []map[string]any{
		{"id": "A", "price": 10.0, "stock": 100.0},
		{"id": "B", "price": 20.0, "stock": 50.0},
		{"id": "C", "price": 30.0, "stock": 0.0},
		{"id": "D", "price": 15.0, "stock": 20.0},
	}

	for _, prod := range products {
		if err := p.Insert(prod); err != nil {
			t.Fatal(err)
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
	p, err = tx.LoadPersistent(relation)
	if err != nil {
		t.Fatal(err)
	}

	// Test Greater Than
	opGt := Gt("price", 15.0)
	seqGt, err := p.Select(opGt)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for val, err := range seqGt {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["price"].(float64) <= 15.0 {
			t.Errorf("Expected price > 15, got %v", val["price"])
		}
	}
	if count != 2 { // B and C
		t.Errorf("Expected 2 items with price > 15, got %d", count)
	}

	// Test Less Than or Equal
	// A: stock 100
	// B: stock 50
	// C: stock 0
	// D: stock 20

	// Le(20.0) -> Expect C (0) and D (20)
	opLe := Le("stock", 20.0)
	seqLe, err := p.Select(opLe)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for val, err := range seqLe {
		if err != nil {
			t.Fatal(err)
		}

		v := val["stock"].(float64)
		if v > 20.0 {
			t.Errorf("Expected stock <= 20, got %v", v)
		} else {
			count++
		}
	}
	if count != 2 { // C (0) and D (20)
		t.Errorf("Expected 2 items with stock <= 20, got %d", count)
	}

	// Test Multiple Operators (AND)
	// Price > 10 AND Stock > 0
	op1 := Gt("price", 10.0)
	op2 := Gt("stock", 0.0)

	seqMulti, err := p.Select(op1, op2)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for val, err := range seqMulti {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["price"].(float64) <= 10.0 || val["stock"].(float64) <= 0.0 {
			t.Errorf("Expected price > 10 and stock > 0, got %v", val)
		}
	}
	if count != 2 { // B (20, 50) and D (15, 20). A is price 10 (not > 10). C is stock 0.
		t.Errorf("Expected 2 items with conditions, got %d", count)
	}
}

func TestPersistent_ComplexTypes(t *testing.T) {
	db, cleanup := setupTestDBWithMaUn(t, &JsonMaUn)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "complex"
	columns := []string{"id", "data", "tags"}
	indexes := map[string][]string{}

	p, err := tx.CreatePersistent(relation, columns, indexes)
	if err != nil {
		t.Fatal(err)
	}

	// Map and Slice
	item1 := map[string]any{
		"id": "1",
		"data": map[string]any{
			"key1": "value1",
			"key2": 42.0,
		},
		"tags": []any{"tag1", "tag2"},
	}
	if err := p.Insert(item1); err != nil {
		t.Fatal(err)
	}

	// Another item for comparison tests
	item2 := map[string]any{
		"id": "2",
		"data": map[string]any{
			"key1": "value2", // Different value
		},
		"tags": []any{"tag3"},
	}
	if err := p.Insert(item2); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	p, err = tx.LoadPersistent(relation)
	if err != nil {
		t.Fatal(err)
	}

	// Test Equality on Map
	// Comparison needs strict structural equality or byte comparison if non-comparable.
	targetMap := map[string]any{
		"key1": "value1",
		"key2": 42.0,
	}
	opMap := Eq("data", targetMap)

	seqMap, err := p.Select(opMap)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for val, err := range seqMap {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["id"] != "1" {
			t.Errorf("Expected id 1, got %v", val["id"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for map equality, got %d", count)
	}

	// Test Equality on Slice
	// Order matters for byte-based comparison of slices
	targetSlice := []any{"tag1", "tag2"}
	opSlice := Eq("tags", targetSlice)

	seqSlice, err := p.Select(opSlice)
	if err != nil {
		t.Fatal(err)
	}

	count = 0
	for val, err := range seqSlice {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["id"] != "1" {
			t.Errorf("Expected id 1, got %v", val["id"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for slice equality, got %d", count)
	}

	// Test Inequality (Byte order comparison for maps)
	// We expect item2 > item1 because "value2" > "value1" in the JSON string representation
	// item1 data: {"key1":"value1",...}
	// item2 data: {"key1":"value2"}
	// This depends on map key ordering in JSON marshal, which is usually sorted by key.

	// Let's verify sort order.
	opGt := Gt("data", targetMap) // data > {"key1":"value1"...}

	seqGt, err := p.Select(opGt)
	if err != nil {
		t.Fatal(err)
	}

	count = 0
	for val, err := range seqGt {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["id"] != "2" {
			t.Errorf("Expected id 2 (value2 > value1), got %v", val["id"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for map Gt comparison, got %d", count)
	}
}

func TestPersistent_CompositeIndex(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "users"
	columns := []string{"id", "first", "last", "age"}
	// Composite index on (first, last)
	indexes := map[string][]string{
		"name": {"first", "last"},
	}

	p, err := tx.CreatePersistent(relation, columns, indexes)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	users := []struct {
		id    string
		first string
		last  string
		age   float64
	}{
		{"1", "John", "Doe", 30},
		{"2", "Jane", "Doe", 25},
		{"3", "John", "Smith", 40},
		{"4", "Alice", "Wonder", 20},
	}

	for _, u := range users {
		err := p.Insert(map[string]any{
			"id":    u.id,
			"first": u.first,
			"last":  u.last,
			"age":   u.age,
		})
		if err != nil {
			t.Fatal(err)
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
	p, err = tx.LoadPersistent(relation)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Exact match on composite index
	op := Eq("name", []any{"John", "Doe"})
	seq, err := p.Select(op)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	count := 0
	for val, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["first"] != "John" || val["last"] != "Doe" {
			t.Errorf("Expected John Doe, got %v %v", val["first"], val["last"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for John Doe, got %d", count)
	}

	// Test 2: Composite index AND non-indexed filter
	op1 := Eq("name", []any{"John", "Doe"})
	op2 := Gt("age", 20.0)

	seq2, err := p.Select(op1, op2)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	count = 0
	for val, err := range seq2 {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["first"] != "John" {
			t.Errorf("Expected John, got %v", val["first"])
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}

	// Test 3: Range scan on composite index
	opGe := Ge("name", []any{"John"})
	opLt := Lt("name", []any{"Joho"})

	seq3, err := p.Select(opGe, opLt)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	count = 0
	foundDoe := false
	foundSmith := false
	for val, err := range seq3 {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if val["first"] != "John" {
			t.Errorf("Expected first name John, got %v", val["first"])
		}
		last := val["last"].(string)
		if last == "Doe" {
			foundDoe = true
		}
		if last == "Smith" {
			foundSmith = true
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 Johns, got %d", count)
	}
	if !foundDoe || !foundSmith {
		t.Errorf("Expected Doe and Smith, got Doe=%v Smith=%v", foundDoe, foundSmith)
	}
}
