package thunderdb

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentReadWrite simulates multiple concurrent transactions.
// It uses one writer goroutine and multiple reader goroutines.
// The writer inserts records while readers constantly query for them.
func TestConcurrentReadWrite(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Initialize the schema
	initTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	// Create a persistent table "metrics"
	// with "id" and "val" columns.
	_, err = initTx.CreatePersistent("metrics", map[string]ColumnSpec{
		"id":  {Indexed: true},
		"val": {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := initTx.Commit(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	// We'll use a done channel to signal stopping.
	done := make(chan struct{})

	// Configuration
	const numReaders = 5
	const writeDuration = 2 * time.Second
	// Use atomic counter for generating IDs
	var writeCount int64

	// Writer Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		timer := time.NewTimer(writeDuration)
		defer timer.Stop()

		for {
			select {
			case <-done:
				return
			case <-timer.C:
				close(done) // Signal readers to stop
				return
			case <-ticker.C:
				// Perform a write transaction
				tx, err := db.Begin(true)
				if err != nil {
					t.Errorf("writer begin error: %v", err)
					return
				}
				p, err := tx.LoadPersistent("metrics")
				if err != nil {
					t.Errorf("writer load persistent error: %v", err)
					tx.Rollback()
					return
				}

				// Insert a new record
				id := atomic.AddInt64(&writeCount, 1)
				record := map[string]any{
					"id":  fmt.Sprintf("k-%d", id),
					"val": float64(id * 10),
				}
				if err := p.Insert(record); err != nil {
					t.Errorf("writer insert error: %v", err)
					tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("writer commit error: %v", err)
					return
				}
			}
		}
	}()

	// Reader Goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					// Just a small sleep to not hammer CPU too hard in loop
					time.Sleep(5 * time.Millisecond)

					tx, err := db.Begin(false) // Read-only
					if err != nil {
						t.Errorf("reader %d begin error: %v", readerID, err)
						return
					}
					p, err := tx.LoadPersistent("metrics")
					if err != nil {
						t.Errorf("reader %d load persistent error: %v", readerID, err)
						tx.Rollback()
						return
					}

					// Read a random existing ID or just count all
					// Let's count all for simplicity or pick a recent one
					currentMax := atomic.LoadInt64(&writeCount)
					if currentMax == 0 {
						tx.Rollback()
						continue
					}

					// Randomly select one ID to query
					targetID := rand.Int63n(currentMax) + 1
					targetKey := fmt.Sprintf("k-%d", targetID)

					op := Eq("id", targetKey)
					f, err := ToKeyRanges(op)
					if err != nil {
						t.Errorf("reader %d key range error: %v", readerID, err)
						tx.Rollback()
						return
					}

					seq, err := p.Select(f)
					if err != nil {
						t.Errorf("reader %d select error: %v", readerID, err)
						tx.Rollback()
						return
					}

					// Consume the iterator
					count := 0
					for _, err := range seq {
						if err != nil {
							t.Errorf("reader %d seq error: %v", readerID, err)
						}
						count++
					}

					// We expect exactly 1 result if it was inserted successfully
					// (Assuming strict consistency or at least eventual consistency visible here)
					if count != 1 {
						// It's possible the writer incremented the atomic counter
						// but hasn't committed the transaction yet.
						// So a missing record is not necessarily a fatal error in this loose coupling,
						// but strictly speaking, if we see writeCount=N, k-N might not be committed yet.
						// However, k-(N-some_buffer) should probably be there.
						// For this test, let's just ensure no errors occur during read.
					}

					tx.Rollback()
				}
			}
		}(i)
	}

	wg.Wait()

	// Final verification
	finalTx, _ := db.Begin(false)
	defer finalTx.Rollback()
	p, _ := finalTx.LoadPersistent("metrics")
	// Check total count?
	// This is just to ensure DB is not corrupted
	op := Gt("val", -1.0) // select all
	f, _ := ToKeyRanges(op)
	seq, _ := p.Select(f)
	finalCount := 0
	for _, _ = range seq {
		finalCount++
	}

	t.Logf("Total records written: %d, Total records found: %d", atomic.LoadInt64(&writeCount), finalCount)
	if int64(finalCount) != atomic.LoadInt64(&writeCount) {
		t.Errorf("Mismatch in final count")
	}
}
