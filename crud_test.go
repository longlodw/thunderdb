package thunderdb

import (
	"os"
	"testing"
)

// setupTestDBFromPersistent is a helper that mimics setupTestDB from original persistent_test.go
func setupTestDBFromPersistent(t *testing.T) (*DB, func()) {
	tmpfile, err := os.CreateTemp("", "thunder_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()

	db, err := OpenDB(dbPath, 0600, nil)
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

func TestBasicCRUD_Insert(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "users"
	// users: id(0), username(1), age(2)
	// index on username(1)
	err = tx.CreateStorage(relation, 3, []IndexInfo{
		{ReferencedCols: []int{1}, IsUnique: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	user1 := map[int]any{0: "1", 1: "alice", 2: 30.0}
	if err := tx.Insert(relation, user1); err != nil {
		t.Fatal(err)
	}

	user2 := map[int]any{0: "2", 1: "bob", 2: 25.0}
	if err := tx.Insert(relation, user2); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestBasicCRUD_SelectAlice(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	// Pre-populate data
	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		relation := "users"
		// users: id(0), username(1), age(2)
		// index on username(1)
		err = tx.CreateStorage(relation, 3, []IndexInfo{
			{ReferencedCols: []int{1}, IsUnique: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		tx.Insert(relation, map[int]any{0: "1", 1: "alice", 2: 30.0})
		tx.Insert(relation, map[int]any{0: "2", 1: "bob", 2: 25.0})
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.StoredQuery("users")
	if err != nil {
		t.Fatal(err)
	}

	// Select where username(1) == "alice"
	seq, err := tx.Select(p, SelectCondition{Col: 1, Operator: EQ, Value: "alice"})
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
			t.Fatal(err)
		}
		if username != "alice" {
			t.Errorf("Expected username alice, got %v", username)
		}
		var age float64
		if err := row.Get(2, &age); err != nil {
			t.Fatal(err)
		}
		if age != 30.0 {
			t.Errorf("Expected age 30, got %v", age)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for alice, got %d", count)
	}
}

func TestBasicCRUD_DeleteBob(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	// Pre-populate data
	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		relation := "users"
		// users: id(0), username(1), age(2)
		// index on username(1)
		err = tx.CreateStorage(relation, 3, []IndexInfo{
			{ReferencedCols: []int{1}, IsUnique: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		tx.Insert(relation, map[int]any{0: "1", 1: "alice", 2: 30.0})
		tx.Insert(relation, map[int]any{0: "2", 1: "bob", 2: 25.0})
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	// Delete
	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		// Delete where username(1) == "bob"
		if err := tx.Delete("users", SelectCondition{Col: 1, Operator: EQ, Value: "bob"}); err != nil {
			t.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	// Verify Delete
	func() {
		tx, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		p, err := tx.StoredQuery("users")
		if err != nil {
			t.Fatal(err)
		}

		seq, err := tx.Select(p, SelectCondition{Col: 1, Operator: EQ, Value: "bob"})
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
	}()
}

func TestNonIndexedSelect(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()
	relation := "items"

	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		// items: id(0), price(1)
		// No indexes provided -> scan
		err = tx.CreateStorage(relation, 2, nil)
		if err != nil {
			t.Fatal(err)
		}

		tx.Insert(relation, map[int]any{0: "A", 1: 10.0})
		tx.Insert(relation, map[int]any{0: "B", 1: 20.0})

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.StoredQuery("items")
	if err != nil {
		t.Fatal(err)
	}

	// price is column 1
	seq, err := tx.Select(p, SelectCondition{Col: 1, Operator: EQ, Value: 20.0})
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		var id string
		if err := row.Get(0, &id); err != nil {
			t.Fatal(err)
		}
		if id != "B" {
			t.Errorf("Expected item B, got %v", id)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for price 20, got %d", count)
	}
}

func TestProjection(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	// Insert
	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		relation := "users"
		// users: id(0), username(1), age(2)
		err = tx.CreateStorage(relation, 3, []IndexInfo{
			{ReferencedCols: []int{1}, IsUnique: true},
		})
		if err != nil {
			t.Fatal(err)
		}
		tx.Insert(relation, map[int]any{0: "1", 1: "alice", 2: 30.0})
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	// Select
	func() {
		tx, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		p, err := tx.StoredQuery("users")
		if err != nil {
			t.Fatal(err)
		}

		// Projection mapping:
		// original: id(0), username(1), age(2)
		// target: user_id(0) -> id(0), login_name(1) -> username(1), user_age(2) -> age(2)
		proj, err := p.Project(0, 2, 1)
		if err != nil {
			t.Fatal(err)
		}

		// Filter: username is now at index 2 in projected relation.
		// username is at index 2.
		seq, err := tx.Select(proj, SelectCondition{Col: 2, Operator: EQ, Value: "alice"})
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
			// Projected: 0->id, 1->age, 2->username
			var loginName string
			if err := row.Get(2, &loginName); err != nil {
				t.Fatalf("failed to get loginName (idx 2): %v", err)
			}
			if loginName != "alice" {
				t.Errorf("Expected login_name alice, got %v", loginName)
			}
			var userAge float64
			if err := row.Get(1, &userAge); err != nil {
				t.Fatalf("failed to get userAge (idx 1): %v", err)
			}
			if userAge != 30.0 {
				t.Errorf("Expected user_age 30, got %v", userAge)
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 result, got %d", count)
		}
	}()
}

func TestDifferentOperators(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	// Insert
	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		relation := "products"
		// products: id(0), price(1), stock(2)
		err = tx.CreateStorage(relation, 3, nil)
		if err != nil {
			t.Fatal(err)
		}

		tx.Insert(relation, map[int]any{0: "A", 1: 10.0, 2: 100.0})
		tx.Insert(relation, map[int]any{0: "B", 1: 20.0, 2: 50.0})
		tx.Insert(relation, map[int]any{0: "C", 1: 30.0, 2: 0.0})
		tx.Insert(relation, map[int]any{0: "D", 1: 15.0, 2: 20.0})

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.StoredQuery("products")
	if err != nil {
		t.Fatal(err)
	}

	// Test Greater Than: price(1) > 15.0
	seqGt, err := tx.Select(p, SelectCondition{Col: 1, Operator: GT, Value: 15.0})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for row, err := range seqGt {
		if err != nil {
			t.Fatal(err)
		}
		count++
		var val float64
		row.Get(1, &val)
		if val <= 15.0 {
			t.Errorf("Expected price > 15, got %v", val)
		}
	}
	if count != 2 { // B(20) and C(30)
		t.Errorf("Expected 2 items with price > 15, got %d", count)
	}

	// Test Less Than or Equal: stock(2) <= 20.0
	seqLe, err := tx.Select(p, SelectCondition{Col: 2, Operator: LTE, Value: 20.0})
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for row, err := range seqLe {
		if err != nil {
			t.Fatal(err)
		}
		var val float64
		row.Get(2, &val)
		if val > 20.0 {
			t.Errorf("Expected stock <= 20, got %v", val)
		} else {
			count++
		}
	}
	if count != 2 { // C (0) and D (20)
		t.Errorf("Expected 2 items with stock <= 20, got %d", count)
	}

	// Test Multiple Operators (AND): price(1) > 10 AND stock(2) > 0
	seqMulti, err := tx.Select(p,
		SelectCondition{Col: 1, Operator: GT, Value: 10.0},
		SelectCondition{Col: 2, Operator: GT, Value: 0.0},
	)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for row, err := range seqMulti {
		if err != nil {
			t.Fatal(err)
		}
		count++
		var price float64
		row.Get(1, &price)
		var stock float64
		row.Get(2, &stock)
		if price <= 10.0 || stock <= 0.0 {
			t.Errorf("Expected price > 10 and stock > 0, got %v %v", price, stock)
		}
	}
	if count != 2 { // B (20, 50) and D (15, 20). A(10, 100) fail price. C(30, 0) fail stock.
		t.Errorf("Expected 2 items with conditions, got %d", count)
	}
}

func TestCompositeIndex(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	// Insert
	func() {
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		relation := "users"
		// users: id(0), first(1), last(2), age(3)
		// Composite index on (first, last) -> (1, 2)
		err = tx.CreateStorage(relation, 4, []IndexInfo{
			{ReferencedCols: []int{1, 2}, IsUnique: true},
		})
		if err != nil {
			t.Fatal(err)
		}

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
			tx.Insert(relation, map[int]any{
				0: u.id,
				1: u.first,
				2: u.last,
				3: u.age,
			})
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	p, err := tx.StoredQuery("users")
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Exact match on composite index
	// We check if providing values for first(1) and last(2) hits the index
	seq, err := tx.Select(p,
		SelectCondition{Col: 1, Operator: EQ, Value: "John"},
		SelectCondition{Col: 2, Operator: EQ, Value: "Doe"},
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	count := 0
	for row, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		count++
		var first, last string
		row.Get(1, &first)
		row.Get(2, &last)
		if first != "John" || last != "Doe" {
			t.Errorf("Expected John Doe, got %v %v", first, last)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for John Doe, got %d", count)
	}

	// Test 3: Composite + non-indexed filter
	// John Doe (1, 2) AND Age=30(3) (Age is 30 in data)
	seq2, err := tx.Select(p,
		SelectCondition{Col: 1, Operator: EQ, Value: "John"},
		SelectCondition{Col: 2, Operator: EQ, Value: "Doe"},
		SelectCondition{Col: 3, Operator: EQ, Value: 30.0},
	)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	count = 0
	for row, err := range seq2 {
		if err != nil {
			t.Fatal(err)
		}
		count++
		var first string
		row.Get(1, &first)
		if first != "John" {
			t.Errorf("Expected John, got %v", first)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 result for John Doe Age 30, got %d", count)
	}
}
