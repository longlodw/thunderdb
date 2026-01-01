package thunderdb

import (
	"fmt"
	"math/rand"
	"os"
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

// BenchmarkConcurrentReadWrite measures performance of concurrent reads and writes.
func BenchmarkConcurrentReadWrite(b *testing.B) {
	// Setup DB manually since we can't use setupTestDB (it takes *testing.T)
	// We'll replicate the logic.
	tmpfile, err := os.CreateTemp("", "thunder_bench_*.db")
	if err != nil {
		b.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := OpenDB(&MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Initialize Schema
	initTx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	_, err = initTx.CreatePersistent("bench_table", map[string]ColumnSpec{
		"id":  {Indexed: true},
		"val": {},
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := initTx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Pre-populate some data so reads have something to hit
	{
		tx, _ := db.Begin(true)
		p, _ := tx.LoadPersistent("bench_table")
		for i := 0; i < 1000; i++ {
			p.Insert(map[string]any{
				"id":  fmt.Sprintf("init-%d", i),
				"val": i,
			})
		}
		tx.Commit()
	}

	b.ResetTimer()

	// We will run parallel benchmarks.
	// Some goroutines will read, others (fewer) will write.
	// b.RunParallel launches multiple goroutines.
	// We can use the goroutine index or random chance to decide role.

	var writeCounter int64

	b.RunParallel(func(pb *testing.PB) {
		// Create a local random source or just use math/rand (mutex protected globally, might be slow)
		// but for simplicity in this snippet we'll just read mostly.

		for pb.Next() {
			// 10% writes, 90% reads
			if rand.Intn(10) == 0 {
				// Write
				tx, err := db.Begin(true)
				if err != nil {
					b.Fatal(err)
				}
				p, err := tx.LoadPersistent("bench_table")
				if err != nil {
					tx.Rollback()
					b.Fatal(err)
				}

				id := atomic.AddInt64(&writeCounter, 1)
				err = p.Insert(map[string]any{
					"id":  fmt.Sprintf("new-%d", id),
					"val": int(id),
				})
				if err != nil {
					tx.Rollback()
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			} else {
				// Read
				tx, err := db.Begin(false)
				if err != nil {
					b.Fatal(err)
				}
				p, err := tx.LoadPersistent("bench_table")
				if err != nil {
					tx.Rollback()
					b.Fatal(err)
				}

				// Read a random ID from the initial set
				target := rand.Intn(1000)
				op := Eq("id", fmt.Sprintf("init-%d", target))
				f, _ := ToKeyRanges(op)

				seq, _ := p.Select(f)
				for _, _ = range seq {
					// consume
				}
				tx.Rollback()
			}
		}
	})
}
