package thunderdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// setupBatchTestDB creates a temporary database for testing
func setupBatchTestDB(t testing.TB) (*DB, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "thunderdb-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(dir, "test.db")
	db, err := OpenDB(&JsonMaUn, dbPath, 0600, nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open db: %v", err)
	}

	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestDB_View(t *testing.T) {
	db, cleanup := setupBatchTestDB(t)
	defer cleanup()

	// Prepare data
	err := db.Update(func(tx *Tx) error {
		// id=0, name=1
		err := tx.CreateStorage("users", []ColumnSpec{
			{IsUnique: true}, // id (0)
			{},               // name (1)
		}, nil)
		if err != nil {
			return err
		}
		return tx.Insert("users", map[int]any{0: "1", 1: "Alice"})
	})
	if err != nil {
		t.Fatalf("failed to prepare data: %v", err)
	}

	// Test View
	err = db.View(func(tx *Tx) error {
		p, err := tx.LoadStoredBody("users")
		if err != nil {
			return err
		}
		iter, err := tx.Query(p, nil)
		if err != nil {
			return err
		}
		count := 0
		for row, err := range iter {
			if err != nil {
				return err
			}
			var val string
			if err := row.Get(1, &val); err != nil {
				return err
			}
			if val != "Alice" {
				t.Errorf("expected Alice, got %v", val)
			}
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 row, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Errorf("View failed: %v", err)
	}
}

func TestDB_Update(t *testing.T) {
	db, cleanup := setupBatchTestDB(t)
	defer cleanup()

	err := db.Update(func(tx *Tx) error {
		// id=0
		err := tx.CreateStorage("items", []ColumnSpec{
			{IsUnique: true}, // id (0)
		}, nil)
		if err != nil {
			return err
		}
		return tx.Insert("items", map[int]any{0: 100})
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	db.View(func(tx *Tx) error {
		p, err := tx.LoadStoredBody("items")
		if err != nil {
			return err
		}
		iter, err := tx.Query(p, nil)
		if err != nil {
			return err
		}
		found := false
		for _, err := range iter {
			if err != nil {
				return err
			}
			found = true
			break
		}
		if !found {
			t.Error("expected item to be found")
		}
		return nil
	})
}

func TestDB_Batch(t *testing.T) {
	db, cleanup := setupBatchTestDB(t)
	defer cleanup()

	// Initialize schema first
	err := db.Update(func(tx *Tx) error {
		// ts=0, msg=1
		return tx.CreateStorage("logs", []ColumnSpec{
			{IsUnique: true}, // ts (0)
			{},               // msg (1)
		}, nil)
	})
	if err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}

	var wg sync.WaitGroup
	workers := 10
	errCh := make(chan error, workers)

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := db.Batch(func(tx *Tx) error {
				// We don't need to load anything to insert anymore, just insert directly by name
				// Use a unique timestamp based on ID to avoid collisions in this simple test
				return tx.Insert("logs", map[int]any{
					0: id,                        // ts
					1: fmt.Sprintf("worker %d", id), // msg
				})
			})
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("Batch worker failed: %v", err)
	}

	// Verify all writes succeeded
	db.View(func(tx *Tx) error {
		p, err := tx.LoadStoredBody("logs")
		if err != nil {
			return err
		}
		iter, err := tx.Query(p, nil)
		if err != nil {
			return err
		}
		count := 0
		for _, err := range iter {
			if err != nil {
				return err
			}
			count++
		}
		if count != workers {
			t.Errorf("expected %d logs, got %d", workers, count)
		}
		return nil
	})
}

func TestDB_Batch_PanicProtection(t *testing.T) {
	db, cleanup := setupBatchTestDB(t)
	defer cleanup()

	// This test ensures that a panic in one batch item doesn't crash the whole process
	// and allows other batch items to succeed (eventually, via retry).
	// Note: boltdb catches panics in Batch, returns error, and retries others solo.

	// 1. Setup Schema
	db.Update(func(tx *Tx) error {
		// val=0
		return tx.CreateStorage("data", []ColumnSpec{
			{}, // val
		}, nil)
	})

	var wg sync.WaitGroup
	wg.Add(2)

	// Worker 1: Panics
	go func() {
		defer wg.Done()
		defer func() { recover() }() // We expect a panic
		db.Batch(func(tx *Tx) error {
			panic("something went wrong")
		})
	}()

	// Worker 2: Succeeds
	go func() {
		defer wg.Done()
		// Sleep slightly to let them likely group together
		time.Sleep(time.Millisecond)
		db.Batch(func(tx *Tx) error {
			return tx.Insert("data", map[int]any{0: 123})
		})
	}()

	wg.Wait()

	// Check if Worker 2's data made it
	db.View(func(tx *Tx) error {
		p, _ := tx.LoadStoredBody("data")
		iter, _ := tx.Query(p, nil)
		count := 0
		for _, err := range iter {
			if err == nil {
				count++
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 row from successful worker, got %d", count)
		}
		return nil
	})
}

// Benchmark comparing Update (Sequential) vs Batch (Concurrent)

func TestDB_BatchOptions(t *testing.T) {
	db, cleanup := setupBatchTestDB(t)
	defer cleanup()

	// Get initial values
	initialBatchSize := db.MaxBatchSize()
	initialBatchDelay := db.MaxBatchDelay()

	// Set new values
	newBatchSize := initialBatchSize + 500
	newBatchDelay := initialBatchDelay + 100*time.Millisecond

	db.SetMaxBatchSize(newBatchSize)
	db.SetMaxBatchDelay(newBatchDelay)

	// Verify the new values match what was set
	if got := db.MaxBatchSize(); got != newBatchSize {
		t.Errorf("expected MaxBatchSize %d, got %d", newBatchSize, got)
	}
	if got := db.MaxBatchDelay(); got != newBatchDelay {
		t.Errorf("expected MaxBatchDelay %v, got %v", newBatchDelay, got)
	}
}
