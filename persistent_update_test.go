package thunderdb

import (
	"errors"
	"testing"
)

func TestPersistent_Update(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	users, err := tx.CreatePersistent("users", map[string]ColumnSpec{
		"id":    {Indexed: true, Unique: true},
		"name":  {},
		"email": {Unique: true},
		"age":   {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial data
	initialUsers := []map[string]any{
		{"id": "1", "name": "Alice", "email": "alice@example.com", "age": int64(30)},
		{"id": "2", "name": "Bob", "email": "bob@example.com", "age": int64(25)},
		{"id": "3", "name": "Charlie", "email": "charlie@example.com", "age": int64(35)},
	}
	for _, u := range initialUsers {
		if err := users.Insert(u); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// 1. Basic Update
	t.Run("Basic Update", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()
		users, _ := tx.LoadPersistent("users")

		// Update Bob's age to 26
		op := Eq("id", "2")
		filter, _ := ToKeyRanges(op)
		updates := map[string]any{"age": int64(26)}
		if err := users.Update(filter, updates); err != nil {
			t.Fatal(err)
		}

		// Verify update
		seq, _ := users.Select(filter)
		count := 0
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			val, _ := row.Get("age")
			if val != int64(26) {
				t.Errorf("Expected age 26, got %v", val)
			}
			count++
		}
		if count != 1 {
			t.Errorf("Expected 1 updated row, got %d", count)
		}
	})

	// 2. Update causing Unique Constraint Violation (on email)
	t.Run("Update Unique Constraint Violation", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()
		users, _ := tx.LoadPersistent("users")

		// Try to update Alice's email to Bob's email
		op := Eq("id", "1")
		filter, _ := ToKeyRanges(op)
		updates := map[string]any{"email": "bob@example.com"}

		err := users.Update(filter, updates)
		if err == nil {
			t.Fatal("Expected unique constraint violation error, got nil")
		}

		// Check error type or message if possible, though strict type check might depend on implementation
		// Assuming ErrUniqueConstraint is returned
		var thunderErr *ThunderError
		if !errors.As(err, &thunderErr) {
			t.Errorf("Expected ThunderError, got %T: %v", err, err)
		} else if thunderErr.Code != ErrCodeUniqueConstraint {
			t.Errorf("Expected ErrCodeUniqueConstraint, got code %d: %v", thunderErr.Code, err)
		}
	})

	// 3. Update with Index Changes
	t.Run("Update Index Changes", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()
		users, _ := tx.LoadPersistent("users")

		// Update Charlie's age (indexed)
		op := Eq("id", "3")
		filter, _ := ToKeyRanges(op)
		updates := map[string]any{"age": 36}
		if err := users.Update(filter, updates); err != nil {
			t.Fatal(err)
		}

		// Verify using the index
		opNew := Eq("age", 36)
		filterNew, _ := ToKeyRanges(opNew)
		seq, _ := users.Select(filterNew)
		count := 0
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			count++
		}
		if count != 1 {
			t.Errorf("Expected to find row via new index value, got %d results", count)
		}

		// Verify old index value is gone
		opOld := Eq("age", 35)
		filterOld, _ := ToKeyRanges(opOld)
		seqOld, _ := users.Select(filterOld)
		countOld := 0
		for range seqOld {
			countOld++
		}
		if countOld != 0 {
			t.Errorf("Expected 0 results for old index value, got %d", countOld)
		}
	})

	// 4. Update Multiple Rows
	t.Run("Update Multiple Rows", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()
		users, _ := tx.LoadPersistent("users")

		// Update all users with age > 20 to have "active" status (adding a new field effectively if schema allowed,
		// but here we just update 'name' to append suffix)
		// Note: Schema is fixed in this test setup, so we update 'name'
		op := Gt("age", 20)
		filter, _ := ToKeyRanges(op)
		updates := map[string]any{"name": "Updated"} // sets all names to "Updated"

		if err := users.Update(filter, updates); err != nil {
			t.Fatal(err)
		}

		// Verify
		seq, _ := users.Select(filter)
		for row := range seq {
			val, _ := row.Get("name")
			if val != "Updated" {
				t.Errorf("Expected name 'Updated', got %v", val)
			}
		}
	})

	// 5. Update causing Unique Constraint on itself (No Change) -> Should pass
	t.Run("Update Unique Field to Same Value", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()
		users, _ := tx.LoadPersistent("users")

		op := Eq("id", "1")
		filter, _ := ToKeyRanges(op)
		updates := map[string]any{"email": "alice@example.com"} // Same email

		if err := users.Update(filter, updates); err != nil {
			t.Fatalf("Update with same unique value failed: %v", err)
		}
	})
}
