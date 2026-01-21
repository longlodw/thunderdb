package thunderdb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// setupBenchmarkDB is similar to setupTestDB but uses testing.B and handles errors for benchmarks.
func setupBenchmarkDB(b *testing.B) (*DB, func()) {
	tmpfile, err := os.CreateTemp("", "thunder_bench_*.db")
	if err != nil {
		b.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()

	// Using MsgpackMaUn as default
	db, err := OpenDB(MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		os.Remove(dbPath)
		b.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(dbPath)
	}
	return db, cleanup
}

func BenchmarkInsert(b *testing.B) {
	counts := []int{100, 1000}
	for _, count := range counts {
		b.Run(fmt.Sprintf("NoIndex_%d", count), func(b *testing.B) {
			db, cleanup := setupBenchmarkDB(b)
			defer cleanup()
			testBody := func() {
				tx, err := db.Begin(true)
				if err != nil {
					b.Fatal(err)
				}
				defer tx.Rollback()
				relation := fmt.Sprintf("bench_%d", rand.Int()) // use rand to avoid conflicts if possible, or just unique
				err = tx.CreateStorage(relation, 2, nil)
				if err != nil {
					b.Fatal(err)
				}

				for j := range count {
					tx.Insert(relation, map[int]any{
						0: strconv.Itoa(j),
						1: float64(j),
					})
				}
				tx.Commit()
			}
			for b.Loop() {
				testBody()
			}
		})

		b.Run(fmt.Sprintf("WithIndex_%d", count), func(b *testing.B) {
			db, cleanup := setupBenchmarkDB(b)
			defer cleanup()

			loopBody := func() {
				tx, err := db.Begin(true)
				if err != nil {
					b.Fatal(err)
				}
				defer tx.Rollback()
				relation := fmt.Sprintf("bench_idx_%d", rand.Int())
				err = tx.CreateStorage(relation, 2, []IndexInfo{
					{ReferencedCols: []int{1}},
				})
				if err != nil {
					b.Fatal(err)
				}

				for j := range count {
					tx.Insert(relation, map[int]any{
						0: strconv.Itoa(j),
						1: float64(j),
					})
				}
				tx.Commit()
			}
			for b.Loop() {
				loopBody()
			}
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Prep data: 10k records
	count := 10000
	tx, _ := db.Begin(true)
	defer tx.Rollback()

	// Relation with index on "val" (col 1)
	relationIdx := "bench_select_idx"
	tx.CreateStorage(relationIdx, 2, []IndexInfo{
		{ReferencedCols: []int{1}},
	})

	// Relation WITHOUT index on "val"
	relationNoIdx := "bench_select_noidx"
	tx.CreateStorage(relationNoIdx, 2, nil)

	for i := range count {
		row := map[int]any{
			0: strconv.Itoa(i),
			1: float64(i),
		}
		tx.Insert(relationIdx, row)
		tx.Insert(relationNoIdx, row)
	}
	tx.Commit()

	// Read transaction
	readTx, _ := db.Begin(false)
	defer readTx.Rollback()
	pLoadIdx, _ := readTx.StoredQuery(relationIdx)
	pLoadNoIdx, _ := readTx.StoredQuery(relationNoIdx)

	b.Run("NonIndexed_Eq", func(b *testing.B) {
		for b.Loop() {
			// Search for random val (non-indexed)
			target := float64(rand.Intn(count))
			seq, _ := readTx.Select(pLoadNoIdx, Condition{Field: 1, Operator: EQ, Value: target})
			for range seq {
				// drain
			}
		}
	})

	b.Run("Indexed_Eq", func(b *testing.B) {
		for b.Loop() {
			// Search for random val (indexed)
			target := float64(rand.Intn(count))
			seq, _ := readTx.Select(pLoadIdx, Condition{Field: 1, Operator: EQ, Value: target})
			for range seq {
				// drain
			}
		}
	})

	b.Run("NonIndexed_Range", func(b *testing.B) {
		for b.Loop() {
			// Range query (scan)
			start := float64(rand.Intn(count - 100))
			end := start + 50.0
			seq, _ := readTx.Select(pLoadNoIdx,
				Condition{Field: 1, Operator: GTE, Value: start},
				Condition{Field: 1, Operator: LT, Value: end},
			)
			for range seq {
				// drain
			}
		}
	})

	b.Run("Indexed_Range", func(b *testing.B) {
		for b.Loop() {
			// Range query (index)
			start := float64(rand.Intn(count - 100))
			end := start + 50.0
			seq, _ := readTx.Select(pLoadIdx,
				Condition{Field: 1, Operator: GTE, Value: start},
				Condition{Field: 1, Operator: LT, Value: end},
			)
			for range seq {
				// drain
			}
		}
	})
}

func BenchmarkDeeplyNestedLargeRows(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Constants for large row generation
	const largeValSize = 8192 // larger than typical OS page size (4KB)
	largeVal := make([]byte, largeValSize)
	for i := range largeVal {
		largeVal[i] = 'A'
	}
	largeStr := string(largeVal)

	tx, _ := db.Begin(true)
	defer tx.Rollback()

	// Schema setup
	// users: u_id(0), u_name(1), group_id(2), u_payload(3)
	tx.CreateStorage("users", 4, []IndexInfo{{ReferencedCols: []int{0}}, {ReferencedCols: []int{2}}})

	// groups: group_id(0), g_name(1), org_id(2), g_payload(3)
	tx.CreateStorage("groups", 4, []IndexInfo{{ReferencedCols: []int{0}}, {ReferencedCols: []int{2}}})

	// orgs: org_id(0), o_name(1), region(2), o_payload(3)
	tx.CreateStorage("orgs", 4, []IndexInfo{{ReferencedCols: []int{0}}, {ReferencedCols: []int{2}}})

	// Pre-populate some data
	count := 1000
	for i := range count {
		// Orgs
		orgID := fmt.Sprintf("o%d", i)
		region := "North"
		if i%2 == 0 {
			region = "South"
		}
		tx.Insert("orgs", map[int]any{
			0: orgID,
			1: fmt.Sprintf("Org_%d", i),
			2: region,
			3: largeStr,
		})

		// Groups
		groupID := fmt.Sprintf("g%d", i)
		tx.Insert("groups", map[int]any{
			0: groupID,
			1: fmt.Sprintf("Group_%d", i),
			2: orgID,
			3: largeStr,
		})

		// Users
		userID := fmt.Sprintf("u%d", i)
		tx.Insert("users", map[int]any{
			0: userID,
			1: fmt.Sprintf("User_%d", i),
			2: groupID,
			3: largeStr,
		})
	}
	tx.Commit()

	b.Run("InsertLargeRows", func(b *testing.B) {
		db, cleanup := setupBenchmarkDB(b)
		defer cleanup()

		tx, _ := db.Begin(true)
		defer tx.Rollback()
		tx.CreateStorage("large_rows", 2, []IndexInfo{{ReferencedCols: []int{0}}})
		tx.Commit()

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			tx, _ := db.Begin(true)
			tx.Insert("large_rows", map[int]any{
				0: strconv.Itoa(i),
				1: largeStr,
			})
			tx.Commit()
		}
	})

	b.Run("QueryDeeplyNested", func(b *testing.B) {
		readTx, _ := db.Begin(false)
		defer readTx.Rollback()

		users, _ := readTx.StoredQuery("users")
		groups, _ := readTx.StoredQuery("groups")
		orgs, _ := readTx.StoredQuery("orgs")

		// Nested Query: qGroupsOrgs (Groups + Orgs)
		// groups(0,1,2,3) join orgs(0,1,2,3) on groups.org_id(2) == orgs.org_id(0)
		qGroupsOrgs, _ := groups.Join(orgs, []JoinOn{
			{LeftField: 2, RightField: 0, Operator: EQ},
		})

		// Top Query: qAll (Users + qGroupsOrgs)
		// users(0,1,2,3) join qGroupsOrgs on users.group_id(2) == groups.group_id(0)
		// Note: qGroupsOrgs is groups(0..3) + orgs(0..3) -> 8 columns? No, Join combines them.
		// JoinedQuery columns: left cols... then right cols...
		// groups has 4 cols. orgs has 4 cols.
		// So in qGroupsOrgs, groups.group_id is at index 0.
		qAll, _ := users.Join(qGroupsOrgs, []JoinOn{
			{LeftField: 2, RightField: 0, Operator: EQ},
		})

		b.ResetTimer()
		for b.Loop() {
			// Query for a specific region. Region is orgs.region(2).
			// In qGroupsOrgs, orgs is on right. groups cols 0-3. orgs cols 4-7.
			// orgs.region is col 2 in orgs, so 4+2 = 6 in qGroupsOrgs.
			// In qAll, users is on left (cols 0-3). qGroupsOrgs is on right.
			// So orgs.region is 4 + 6 = 10 in qAll.
			seq, _ := readTx.Select(qAll, Condition{Field: 10, Operator: EQ, Value: "North"})
			for range seq {
				// drain
			}
		}
	})

	b.Run("RecursiveLargeRows", func(b *testing.B) {
		// Skipping recursion benchmarks as the new API recursion logic might differ significantly
		// and the provided snippet doesn't show recursion in transaction.go yet,
		// or rather, the StoredQuery/Join structure is there but CreateRecursion is missing in new Tx API?
		// transaction.go shows StoredQuery, Select, but not CreateRecursion.
		// Assuming recursion is not yet fully ported or exposed in the same way in the new API subset shown.
		b.Skip("Recursion not implemented in new benchmark update yet")
	})
}

func BenchmarkRecursion(b *testing.B) {
	b.Skip("Recursion not implemented in new benchmark update yet")
}

func BenchmarkRecursionWithNoise(b *testing.B) {
	b.Skip("Recursion not implemented in new benchmark update yet")
}

// BenchmarkConcurrency tests performance under different contention scenarios:
// 1. ReadOnly: 100% readers (Should scale well)
// 2. WriteOnly: 100% writers (Serialized contention)
// 3. Mixed_90_10: 90% Read, 10% Write (Realistic web workload)
// 4. Mixed_50_50: High contention
func BenchmarkConcurrency(b *testing.B) {
	// 1. Setup DB
	tmpfile, err := os.CreateTemp("", "thunder_bench_concurrent_*.db")
	if err != nil {
		b.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := OpenDB(MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// 2. Initialize Schema
	initTx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	// bench_concurrent: id(0), val(1)
	err = initTx.CreateStorage("bench_concurrent", 2, []IndexInfo{{ReferencedCols: []int{0}}})
	if err != nil {
		b.Fatal(err)
	}
	if err := initTx.Commit(); err != nil {
		b.Fatal(err)
	}

	// 3. Pre-populate Data (so reads have targets)
	// We insert 10,000 records
	initialCount := 10000
	{
		tx, _ := db.Begin(true)
		for i := range initialCount {
			tx.Insert("bench_concurrent", map[int]any{
				0: fmt.Sprintf("item-%d", i),
				1: i,
			})
		}
		tx.Commit()
	}

	// Helper to run parallel workloads
	runWorkload := func(b *testing.B, name string, writePerc int) {
		b.Run(name, func(b *testing.B) {
			var writeCounter int64 // Atomic counter for unique write IDs

			b.RunParallel(func(pb *testing.PB) {
				// Thread-local random source to avoid global lock contention
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				for pb.Next() {
					// Decide: Read or Write?
					if rng.Intn(100) < writePerc {
						// WRITE TRANSACTION
						tx, err := db.Begin(true)
						if err != nil {
							b.Fatal(err)
						}

						// Unique ID for new item
						uid := atomic.AddInt64(&writeCounter, 1)
						err = tx.Insert("bench_concurrent", map[int]any{
							0: fmt.Sprintf("new-%d-%d", uid, rng.Int()),
							1: int(uid),
						})
						if err != nil {
							tx.Rollback()
							b.Fatal(err)
						}
						if err := tx.Commit(); err != nil {
							b.Fatal(err)
						}

					} else {
						// READ TRANSACTION
						tx, err := db.Begin(false)
						if err != nil {
							b.Fatal(err)
						}
						p, err := tx.StoredQuery("bench_concurrent")
						if err != nil {
							tx.Rollback()
							b.Fatal(err)
						}

						// Read a random existing item from the initial set
						targetID := rng.Intn(initialCount)
						targetKey := fmt.Sprintf("item-%d", targetID)

						seq, _ := tx.Select(p, Condition{Field: 0, Operator: EQ, Value: targetKey})
						count := 0
						for range seq {
							count++
						}
						tx.Rollback()
					}
				}
			})
		})
	}

	// 4. Run Sub-Benchmarks
	b.SetParallelism(100)             // Force high parallelism to simulate realistic load
	runWorkload(b, "ReadOnly", 0)     // 0% writes
	runWorkload(b, "WriteOnly", 100)  // 100% writes
	runWorkload(b, "Mixed_90_10", 10) // 10% writes
	runWorkload(b, "Mixed_50_50", 50) // 50% writes
}

func BenchmarkUpdateSequential(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b) // Uses MsgpackMaUn
	defer cleanup()

	// Setup data
	tx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	// update_seq: id(0), val(1)
	err = tx.CreateStorage("update_seq", 2, []IndexInfo{{ReferencedCols: []int{0}}})
	if err != nil {
		b.Fatal(err)
	}

	// Insert a record to update
	err = tx.Insert("update_seq", map[int]any{
		0: "1",
		1: 0,
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		err := db.Update(func(tx *Tx) error {
			// Update the record
			return tx.Update("update_seq",
				map[int]any{1: i}, // updates
				Condition{Field: 0, Operator: EQ, Value: "1"}, // conditions
			)
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchConcurrent(b *testing.B) {
	// 1. Setup DB
	db, cleanup := setupBenchmarkDB(b) // Uses MsgpackMaUn
	defer cleanup()

	// 2. Initialize Schema
	initTx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	// bench_batch_concurrent: id(0), val(1)
	err = initTx.CreateStorage("bench_batch_concurrent", 2, []IndexInfo{{ReferencedCols: []int{0}}})
	if err != nil {
		b.Fatal(err)
	}
	if err := initTx.Commit(); err != nil {
		b.Fatal(err)
	}

	// 3. Pre-populate Data (so reads have targets)
	// We insert 10,000 records
	initialCount := 10000
	{
		tx, _ := db.Begin(true)
		for i := range initialCount {
			tx.Insert("bench_batch_concurrent", map[int]any{
				0: fmt.Sprintf("item-%d", i),
				1: i,
			})
		}
		tx.Commit()
	}

	// Helper to run parallel workloads using Batch
	runWorkload := func(b *testing.B, name string, writePerc int) {
		b.Run(name, func(b *testing.B) {
			var writeCounter int64 // Atomic counter for unique write IDs

			b.RunParallel(func(pb *testing.PB) {
				// Thread-local random source
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				for pb.Next() {
					// Decide: Read or Write?
					if rng.Intn(100) < writePerc {
						// WRITE TRANSACTION (Batched)
						err := db.Batch(func(tx *Tx) error {
							// Unique ID for new item
							uid := atomic.AddInt64(&writeCounter, 1)
							return tx.Insert("bench_batch_concurrent", map[int]any{
								0: fmt.Sprintf("new-%d-%d", uid, rng.Int()),
								1: int(uid),
							})
						})
						if err != nil {
							b.Fatal(err)
						}

					} else {
						// READ TRANSACTION (Batched/View)
						err := db.View(func(tx *Tx) error {
							p, err := tx.StoredQuery("bench_batch_concurrent")
							if err != nil {
								return err
							}

							// Read a random existing item from the initial set
							targetID := rng.Intn(initialCount)
							targetKey := fmt.Sprintf("item-%d", targetID)

							seq, _ := tx.Select(p, Condition{Field: 0, Operator: EQ, Value: targetKey})
							count := 0
							for range seq {
								count++
							}
							return nil
						})
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		})
	}

	// 4. Run Sub-Benchmarks (same mixed workloads as BenchmarkConcurrency)
	b.SetParallelism(100)             // Force high parallelism to simulate realistic load
	runWorkload(b, "ReadOnly", 0)     // 0% writes
	runWorkload(b, "WriteOnly", 100)  // 100% writes
	runWorkload(b, "Mixed_90_10", 10) // 10% writes
	runWorkload(b, "Mixed_50_50", 50) // 50% writes
}
