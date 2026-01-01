package thunderdb

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestConcurrentReadWrite simulates high concurrency with 100 readers and 200 writers.
// It verifies that:
// 1. Reads do not crash.
// 2. Reads return valid data (consistent with snapshot isolation).
// 3. All writes are successfully persisted.
func TestConcurrentReadWrite(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Initialize the schema
	initTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	// Create a persistent table "data"
	_, err = initTx.CreatePersistent("data", map[string]ColumnSpec{
		"key": {Indexed: true, Unique: true},
		"val": {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := initTx.Commit(); err != nil {
		t.Fatal(err)
	}

	const numWriters = 200
	const numReaders = 100
	const writeIterations = 50 // Each writer updates its key 50 times
	const readIterations = 50  // Each reader performs 50 reads

	var wg sync.WaitGroup
	wg.Add(numWriters + numReaders)

	// Writers
	// Each writer 'i' owns "key-i" and updates it from 0 to writeIterations-1
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < writeIterations; j++ {
				tx, err := db.Begin(true)
				if err != nil {
					t.Errorf("Writer %d begin failed: %v", writerID, err)
					return
				}

				p, err := tx.LoadPersistent("data")
				if err != nil {
					t.Errorf("Writer %d load failed: %v", writerID, err)
					tx.Rollback()
					return
				}

				key := fmt.Sprintf("key-%d", writerID)

				// Upsert logic: Delete if exists, then Insert
				op := Eq("key", key)
				f, err := ToKeyRanges(op)
				if err != nil {
					t.Errorf("Writer %d filter error: %v", writerID, err)
					tx.Rollback()
					return
				}

				// Delete existing entries for this key (if any)
				if err := p.Delete(f); err != nil {
					t.Errorf("Writer %d delete failed: %v", writerID, err)
					tx.Rollback()
					return
				}

				// Insert new value
				err = p.Insert(map[string]any{
					"key": key,
					"val": float64(j),
				})
				if err != nil {
					t.Errorf("Writer %d insert failed: %v", writerID, err)
					tx.Rollback()
					return
				}

				if err := tx.Commit(); err != nil {
					t.Errorf("Writer %d commit failed: %v", writerID, err)
					return
				}
			}
		}(i)
	}

	// Readers
	// Readers pick a random writer's key (0 to numWriters-1) and read it.
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()
			// Create a local random source to avoid global lock contention in math/rand
			rng := rand.New(rand.NewSource(int64(readerID)))

			for j := 0; j < readIterations; j++ {
				tx, err := db.Begin(false)
				if err != nil {
					t.Errorf("Reader %d begin failed: %v", readerID, err)
					return
				}

				p, err := tx.LoadPersistent("data")
				if err != nil {
					t.Errorf("Reader %d load failed: %v", readerID, err)
					tx.Rollback()
					return
				}

				targetID := rng.Intn(numWriters)
				targetKey := fmt.Sprintf("key-%d", targetID)

				op := Eq("key", targetKey)
				f, err := ToKeyRanges(op)
				if err != nil {
					t.Errorf("Reader %d filter error: %v", readerID, err)
					tx.Rollback()
					return
				}

				seq, err := p.Select(f)
				if err != nil {
					t.Errorf("Reader %d select failed: %v", readerID, err)
					tx.Rollback()
					return
				}

				count := 0
				for row, err := range seq {
					if err != nil {
						t.Errorf("Reader %d row error: %v", readerID, err)
					}
					count++

					// Validate value
					val, _ := row.Get("val")
					if val == nil {
						t.Errorf("Reader %d found nil value for %s", readerID, targetKey)
					}
					// Value should be between 0 and writeIterations-1
					vFloat, ok := val.(float64)
					if !ok {
						t.Errorf("Reader %d expected float val, got %T", readerID, val)
					} else if vFloat < 0 || vFloat >= float64(writeIterations) {
						// Note: This check assumes strictly 0..49.
						// Since writers start at 0 and go up, this is safe.
						t.Errorf("Reader %d read invalid value %v for %s", readerID, vFloat, targetKey)
					}
				}

				// If key hasn't been written yet, count is 0. That's valid (old value = nonexistent).
				// If key has been written, count is 1.
				if count > 1 {
					t.Errorf("Reader %d found duplicate keys for %s: count %d", readerID, targetKey, count)
				}

				tx.Rollback()
			}
		}(i)
	}

	wg.Wait()

	// Verification Phase
	// Verify that all 200 keys exist and have the final value (writeIterations - 1)
	verifyTx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer verifyTx.Rollback()

	p, err := verifyTx.LoadPersistent("data")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < numWriters; i++ {
		targetKey := fmt.Sprintf("key-%d", i)
		op := Eq("key", targetKey)
		f, _ := ToKeyRanges(op)
		seq, _ := p.Select(f)

		count := 0
		var finalVal any
		for row, _ := range seq {
			count++
			finalVal, _ = row.Get("val")
		}

		if count != 1 {
			t.Errorf("Verification failed: key %s not found or duplicate (count=%d)", targetKey, count)
		} else {
			expected := float64(writeIterations - 1)
			if finalVal != expected {
				t.Errorf("Verification failed: key %s has value %v, expected %v", targetKey, finalVal, expected)
			}
		}
	}
}
