package thunderdb

import (
	"errors"
	"testing"
)

func TestPersistent_Update(t *testing.T) {
	db, cleanup := setupTestDBFromPersistent(t)
	defer cleanup()

	// 0. Setup: Create schema and initial data
	// Schema: id(0), name(1), email(2), age(3)
	// Indexes: id (unique), email (unique), age
	relation := "users"
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	err = tx.CreateStorage(relation, 4,
		IndexInfo{ReferencedCols: []int{0}, IsUnique: true},  // id
		IndexInfo{ReferencedCols: []int{2}, IsUnique: true},  // email
		IndexInfo{ReferencedCols: []int{3}, IsUnique: false}, // age
	)
	if err != nil {
		t.Fatal(err)
	}

	// Insert initial data
	initialUsers := []map[int]any{
		{0: "1", 1: "Alice", 2: "alice@example.com", 3: int64(30)},
		{0: "2", 1: "Bob", 2: "bob@example.com", 3: int64(25)},
		{0: "3", 1: "Charlie", 2: "charlie@example.com", 3: int64(35)},
	}
	for _, u := range initialUsers {
		if err := tx.Insert(relation, u); err != nil {
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

		// Update Bob's age to 26
		updates := map[int]any{3: int64(26)}
		if err := tx.Update(relation, updates, SelectCondition{Col: 0, Operator: EQ, Value: "2"}); err != nil {
			t.Fatal(err)
		}

		// Verify update
		p, _ := tx.StoredQuery(relation)
		seq, _ := tx.Select(p, SelectCondition{Col: 0, Operator: EQ, Value: "2"})
		count := 0
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var val int64
			if err := row.Get(3, &val); err != nil {
				t.Fatal(err)
			}
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

		// Try to update Alice's email to Bob's email
		updates := map[int]any{2: "bob@example.com"}

		err := tx.Update(relation, updates, SelectCondition{Col: 0, Operator: EQ, Value: "1"})
		if err == nil {
			t.Fatal("Expected unique constraint violation error, got nil")
		}

		if !errors.Is(err, ErrCodeUniqueConstraint) {
			t.Errorf("Expected ErrCodeUniqueConstraint, got: %v", err)
		}
	})

	// 3. Update with Index Changes
	t.Run("Update Index Changes", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()

		// Update Charlie's age (indexed)
		updates := map[int]any{3: int64(36)}
		if err := tx.Update(relation, updates, SelectCondition{Col: 0, Operator: EQ, Value: "3"}); err != nil {
			t.Fatal(err)
		}

		// Verify using the index
		p, _ := tx.StoredQuery(relation)
		seq, _ := tx.Select(p, SelectCondition{Col: 3, Operator: EQ, Value: int64(36)})
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
		seqOld, _ := tx.Select(p, SelectCondition{Col: 3, Operator: EQ, Value: int64(35)})
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

		// Update all users with age > 20 to have "Updated" name
		updates := map[int]any{1: "Updated"} // set name (col 1)

		if err := tx.Update(relation, updates, SelectCondition{Col: 3, Operator: GT, Value: int64(20)}); err != nil {
			t.Fatal(err)
		}

		// Verify
		p, _ := tx.StoredQuery(relation)
		// Select using same range to check results
		seq, _ := tx.Select(p, SelectCondition{Col: 3, Operator: GT, Value: int64(20)})
		for row, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			var val string
			if err := row.Get(1, &val); err != nil {
				t.Fatal(err)
			}
			if val != "Updated" {
				t.Errorf("Expected name 'Updated', got %v", val)
			}
		}
	})

	// 5. Update causing Unique Constraint on itself (No Change) -> Should pass
	t.Run("Update Unique Field to Same Value", func(t *testing.T) {
		tx, _ := db.Begin(true)
		defer tx.Rollback()

		updates := map[int]any{2: "alice@example.com"} // Same email

		if err := tx.Update(relation, updates, SelectCondition{Col: 0, Operator: EQ, Value: "1"}); err != nil {
			t.Fatalf("Update with same unique value failed: %v", err)
		}
	})

}
