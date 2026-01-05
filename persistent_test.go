package thunderdb

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

	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":       {},
		"username": {Indexed: true},
		"age":      {},
	})
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

	// Use simple value "alice"
	key, err := ToKey("alice")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"username": NewBytesRange(key, key, true, true, nil),
	}
	seq, err := p.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		username, _ := row.Get("username")
		if username != "alice" {
			t.Errorf("Expected username alice, got %v", username)
		}
		age, _ := row.Get("age")
		if age != 30.0 {
			t.Errorf("Expected age 30, got %v", age)
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

	// Use simple value "bob"
	key, err := ToKey("bob")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"username": NewBytesRange(key, key, true, true, nil),
	}

	if err := p.Delete(f); err != nil {
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

	key, err := ToKey("bob")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"username": NewBytesRange(key, key, true, true, nil),
	}
	seq, err := p.Select(f)
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

	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":    {},
		"price": {},
	})
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

	key, err := ToKey(20.0)
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"price": NewBytesRange(key, key, true, true, nil),
	}

	seq, err := p.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		id, _ := row.Get("id")
		if id != "B" {
			t.Errorf("Expected item B, got %v", id)
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

	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":       {},
		"username": {Indexed: true},
		"age":      {},
	})
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
		"user_id":    "id",
		"login_name": "username",
		"user_age":   "age",
	}
	proj := p.Project(mapping)

	key, err := ToKey("alice")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"login_name": NewBytesRange(key, key, true, true, nil),
	}

	seq, err := proj.Select(f)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		loginName, _ := row.Get("login_name")
		if loginName != "alice" {
			t.Errorf("Expected login_name alice, got %v", loginName)
		}
		userAge, _ := row.Get("user_age")
		if userAge != 30.0 {
			t.Errorf("Expected user_age 30, got %v", userAge)
		}
		_, err := row.Get("username")
		if err == nil {
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

	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":    {},
		"price": {},
		"stock": {},
	})
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
	key, err := ToKey(15.0)
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"price": NewBytesRange(key, nil, false, true, nil),
	}

	seqGt, err := p.Select(f)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for row, err := range seqGt {
		if err != nil {
			t.Fatal(err)
		}
		count++
		val, _ := row.Get("price")
		if val.(float64) <= 15.0 {
			t.Errorf("Expected price > 15, got %v", val)
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
	key, err = ToKey(20.0)
	if err != nil {
		t.Fatal(err)
	}
	f = map[string]*BytesRange{
		"stock": NewBytesRange(nil, key, true, true, nil),
	}

	seqLe, err := p.Select(f)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for row, err := range seqLe {
		if err != nil {
			t.Fatal(err)
		}

		val, _ := row.Get("stock")
		v := val.(float64)
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
	key1, err := ToKey(10.0)
	if err != nil {
		t.Fatal(err)
	}
	key2, err := ToKey(0.0)
	if err != nil {
		t.Fatal(err)
	}
	f = map[string]*BytesRange{
		"price": NewBytesRange(key1, nil, false, true, nil),
		"stock": NewBytesRange(key2, nil, false, true, nil),
	}

	seqMulti, err := p.Select(f)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for row, err := range seqMulti {
		if err != nil {
			t.Fatal(err)
		}
		count++
		price, _ := row.Get("price")
		stock, _ := row.Get("stock")
		if price.(float64) <= 10.0 || stock.(float64) <= 0.0 {
			t.Errorf("Expected price > 10 and stock > 0, got %v %v", price, stock)
		}
	}
	if count != 2 { // B (20, 50) and D (15, 20). A is price 10 (not > 10). C is stock 0.
		t.Errorf("Expected 2 items with conditions, got %d", count)
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
	// Composite index on (first, last)
	// Old: map[string][]string{"name": {"first", "last"}}
	// New: "name" is a key in ColumnSpec, with ReferenceCols = ["first", "last"] and Indexed = true
	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":    {},
		"first": {},
		"last":  {},
		"age":   {},
		"name": {
			ReferenceCols: []string{"first", "last"},
			Indexed:       true,
		},
	})
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
	// For composite index, we DO need a slice of values
	key, err := ToKey("John", "Doe")
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]*BytesRange{
		"name": NewBytesRange(key, key, true, true, nil),
	}
	seq, err := p.Select(f)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		first, _ := row.Get("first")
		last, _ := row.Get("last")
		if first != "John" || last != "Doe" {
			t.Errorf("Expected John Doe, got %v %v", first, last)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for John Doe, got %d", count)
	}

	// Test 2: Composite index AND non-indexed filter
	keyName, err := ToKey("John", "Doe")
	if err != nil {
		t.Fatal(err)
	}
	keyAge, err := ToKey(20.0)
	if err != nil {
		t.Fatal(err)
	}
	f = map[string]*BytesRange{
		"name": NewBytesRange(keyName, keyName, true, true, nil),
		"age":  NewBytesRange(keyAge, nil, false, true, nil),
	}
	seq2, err := p.Select(f)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	count = 0
	for row, err := range seq2 {
		if err != nil {
			t.Fatal(err)
		}
		count++
		first, _ := row.Get("first")
		if first != "John" {
			t.Errorf("Expected John, got %v", first)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}

	// Range on name between John Doae and John Smitz
	keyStart, err := ToKey("John", "Doae")
	if err != nil {
		t.Fatal(err)
	}
	keyEnd, err := ToKey("John", "Smitz")
	if err != nil {
		t.Fatal(err)
	}

	f = map[string]*BytesRange{
		"name": NewBytesRange(keyStart, keyEnd, true, false, nil),
	}

	seq3, err := p.Select(f)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	count = 0
	foundDoe := false
	foundSmith := false
	for row, err := range seq3 {
		if err != nil {
			t.Fatal(err)
		}
		count++
		first, _ := row.Get("first")
		if first != "John" {
			t.Errorf("Expected first name John, got %v", first)
		}
		val, _ := row.Get("last")
		last := val.(string)
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
