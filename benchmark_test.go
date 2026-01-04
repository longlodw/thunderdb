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
	db, err := OpenDB(&MsgpackMaUn, dbPath, 0600, nil)
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
				p, _ := tx.CreatePersistent(relation, map[string]ColumnSpec{
					"id":  {},
					"val": {},
				})

				for j := range count {
					p.Insert(map[string]any{
						"id":  strconv.Itoa(j),
						"val": float64(j),
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
				p, _ := tx.CreatePersistent(relation, map[string]ColumnSpec{
					"id":  {},
					"val": {Indexed: true},
				})

				for j := range count {
					p.Insert(map[string]any{
						"id":  strconv.Itoa(j),
						"val": float64(j),
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

	// Relation with index on "val"
	relationIdx := "bench_select_idx"
	pIdx, _ := tx.CreatePersistent(relationIdx, map[string]ColumnSpec{
		"id":  {},
		"val": {Indexed: true},
	})

	// Relation WITHOUT index on "val"
	relationNoIdx := "bench_select_noidx"
	pNoIdx, _ := tx.CreatePersistent(relationNoIdx, map[string]ColumnSpec{
		"id":  {},
		"val": {},
	})

	for i := range count {
		row := map[string]any{
			"id":  strconv.Itoa(i),
			"val": float64(i),
		}
		pIdx.Insert(row)
		pNoIdx.Insert(row)
	}
	tx.Commit()

	// Read transaction
	readTx, _ := db.Begin(false)
	defer readTx.Rollback()
	pLoadIdx, _ := readTx.LoadPersistent(relationIdx)
	pLoadNoIdx, _ := readTx.LoadPersistent(relationNoIdx)

	b.Run("NonIndexed_Eq", func(b *testing.B) {
		for b.Loop() {
			// Search for random val (non-indexed)
			target := float64(rand.Intn(count))
			key, err := ToKey(target)
			if err != nil {
				b.Fatal(err)
			}
			f := map[string]*BytesRange{
				"val": NewBytesRange(key, key, true, true, nil),
			}
			seq, _ := pLoadNoIdx.Select(f, nil)
			for range seq {
				// drain
			}
		}
	})

	b.Run("Indexed_Eq", func(b *testing.B) {
		for b.Loop() {
			// Search for random val (indexed)
			target := float64(rand.Intn(count))
			key, err := ToKey(target)
			if err != nil {
				b.Fatal(err)
			}
			f := map[string]*BytesRange{
				"val": NewBytesRange(key, key, true, true, nil),
			}
			seq, _ := pLoadIdx.Select(f, nil)
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
			keyStart, err := ToKey(start)
			if err != nil {
				b.Fatal(err)
			}
			keyEnd, err := ToKey(end)
			if err != nil {
				b.Fatal(err)
			}
			f := map[string]*BytesRange{
				"val": NewBytesRange(keyStart, keyEnd, true, false, nil),
			}
			seq, _ := pLoadNoIdx.Select(f, nil)
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
			keyStart, err := ToKey(start)
			if err != nil {
				b.Fatal(err)
			}
			keyEnd, err := ToKey(end)
			if err != nil {
				b.Fatal(err)
			}
			f := map[string]*BytesRange{
				"val": NewBytesRange(keyStart, keyEnd, true, false, nil),
			}
			seq, _ := pLoadIdx.Select(f, nil)
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

	// Schema setup similar to query_nested_test.go
	users, _ := tx.CreatePersistent("users", map[string]ColumnSpec{
		"u_id":      {Indexed: true},
		"u_name":    {},
		"group_id":  {Indexed: true},
		"u_payload": {},
	})
	groups, _ := tx.CreatePersistent("groups", map[string]ColumnSpec{
		"group_id":  {Indexed: true},
		"g_name":    {},
		"org_id":    {Indexed: true},
		"g_payload": {},
	})
	orgs, _ := tx.CreatePersistent("orgs", map[string]ColumnSpec{
		"org_id":    {Indexed: true},
		"o_name":    {},
		"region":    {Indexed: true},
		"o_payload": {},
	})

	// Pre-populate some data
	count := 1000
	for i := range count {
		// Orgs
		orgID := fmt.Sprintf("o%d", i)
		region := "North"
		if i%2 == 0 {
			region = "South"
		}
		orgs.Insert(map[string]any{
			"org_id":    orgID,
			"o_name":    fmt.Sprintf("Org_%d", i),
			"region":    region,
			"o_payload": largeStr,
		})

		// Groups
		groupID := fmt.Sprintf("g%d", i)
		groups.Insert(map[string]any{
			"group_id":  groupID,
			"g_name":    fmt.Sprintf("Group_%d", i),
			"org_id":    orgID,
			"g_payload": largeStr,
		})

		// Users
		userID := fmt.Sprintf("u%d", i)
		users.Insert(map[string]any{
			"u_id":      userID,
			"u_name":    fmt.Sprintf("User_%d", i),
			"group_id":  groupID,
			"u_payload": largeStr,
		})
	}
	tx.Commit()

	b.Run("InsertLargeRows", func(b *testing.B) {
		db, cleanup := setupBenchmarkDB(b)
		defer cleanup()

		tx, _ := db.Begin(true)
		defer tx.Rollback()
		p, _ := tx.CreatePersistent("large_rows", map[string]ColumnSpec{
			"id":    {Indexed: true},
			"large": {},
		})
		tx.Commit()

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			tx, _ := db.Begin(true)
			p, _ = tx.LoadPersistent("large_rows")
			p.Insert(map[string]any{
				"id":    strconv.Itoa(i),
				"large": largeStr,
			})
			tx.Commit()
		}
	})

	b.Run("QueryDeeplyNested", func(b *testing.B) {
		readTx, _ := db.Begin(false)
		defer readTx.Rollback()

		users, _ = readTx.LoadPersistent("users")
		groups, _ = readTx.LoadPersistent("groups")
		orgs, _ = readTx.LoadPersistent("orgs")

		// Nested Query: qGroupsOrgs (Groups + Orgs)
		groups, _ = readTx.LoadPersistent("groups")
		orgs, _ = readTx.LoadPersistent("orgs")
		qGroupsOrgs := groups.Join(orgs)

		// Top Query: qAll (Users + qGroupsOrgs)
		users, _ = readTx.LoadPersistent("users")
		qAll := users.Join(qGroupsOrgs)

		b.ResetTimer()
		for b.Loop() {
			// Query for a specific region
			key, err := ToKey("North")
			if err != nil {
				b.Fatal(err)
			}
			f := map[string]*BytesRange{
				"region": NewBytesRange(key, key, true, true, nil),
			}
			seq, _ := qAll.Select(f, nil)
			for range seq {
				// drain
			}
		}
	})

	b.Run("RecursiveLargeRows", func(b *testing.B) {
		db, cleanup := setupBenchmarkDB(b)
		defer cleanup()

		depth := 100
		tx, _ := db.Begin(true)
		defer tx.Rollback()
		relation := "graph_large"
		p, _ := tx.CreatePersistent(relation, map[string]ColumnSpec{
			"source":    {Indexed: true},
			"target":    {Indexed: true},
			"g_payload": {},
		})

		for i := range depth {
			p.Insert(map[string]any{
				"source":    fmt.Sprintf("node_%d", i),
				"target":    fmt.Sprintf("node_%d", i+1),
				"g_payload": largeStr,
			})
		}
		tx.Commit()

		recursiveLoopBody := func() {
			rtx, _ := db.Begin(true)
			defer rtx.Rollback()

			q, _ := rtx.CreateRecursion("descendants_large", map[string]ColumnSpec{
				"target":    {},
				"g_payload": {},
			})

			// Base case
			baseP, _ := rtx.LoadPersistent(relation)
			baseProj := baseP.Project(map[string]string{"target": "target", "g_payload": "g_payload"})

			startNodeRel := "start_node_large"
			startNodeP, _ := rtx.CreatePersistent(startNodeRel, map[string]ColumnSpec{
				"source": {Indexed: true},
			})
			startNodeP.Insert(map[string]any{"source": "node_0"})

			q.AddBranch(baseProj.Join(startNodeP))

			// Recursive step
			recP, _ := rtx.LoadPersistent(relation)
			recProj := recP.Project(map[string]string{"target": "target", "g_payload": "g_payload"})
			q.AddBranch(q.Join(recProj))

			seq, _ := q.Select(nil, nil)
			for range seq {
			}
		}

		b.ResetTimer()
		for b.Loop() {
			recursiveLoopBody()
		}
	})
}

func BenchmarkRecursion(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Create a graph: A -> B -> C -> D ...
	// Hierarchy depth 100
	depth := 100
	tx, _ := db.Begin(true)
	defer tx.Rollback()
	relation := "graph"
	// We need indexes for efficient recursion (joins)
	p, _ := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"source": {Indexed: true},
		"target": {Indexed: true},
	})

	for i := range depth {
		p.Insert(map[string]any{
			"source": fmt.Sprintf("node_%d", i),
			"target": fmt.Sprintf("node_%d", i+1),
		})
	}
	// Create cycle
	p.Insert(map[string]any{
		"source": fmt.Sprintf("node_%d", depth),
		"target": "node_0",
	})
	tx.Commit()

	// Recursive Query Setup
	// Find all descendants of node_0
	readTx, _ := db.Begin(false)
	defer readTx.Rollback()

	recursiveLoopBody := func() {
		rtx, _ := db.Begin(true)
		defer rtx.Rollback()

		q, _ := rtx.CreateRecursion("descendants", map[string]ColumnSpec{
			"source": {
				Indexed: true,
			},
			"target": {},
		})
		edge, _ := rtx.LoadPersistent(relation)
		q.AddBranch(edge)
		q.AddBranch(q.Project(map[string]string{
			"source": "source",
			"join":   "target",
		}).Join(edge.Project(map[string]string{
			"join":   "source",
			"target": "target",
		})).Project(map[string]string{
			"source": "source",
			"target": "target",
		}))

		// Execute
		startKey, _ := ToKey("node_0")
		seq, err := q.Select(map[string]*BytesRange{
			"source": NewBytesRange(startKey, startKey, true, true, nil),
		}, nil)
		if err != nil {
			b.Fatal(err)
		}
		for range seq {
		}
	}
	b.Run("Recursive_Engine", func(b *testing.B) {
		for b.Loop() {
			recursiveLoopBody()
		}
	})

	iterativeLoopBody := func() {
		rtx, _ := db.Begin(false)
		defer rtx.Rollback()
		pLoad, _ := rtx.LoadPersistent(relation)

		currentNodes := []string{"node_0"}
		visited := map[string]bool{"node_0": true}

		// Iterate until no new nodes
		for len(currentNodes) > 0 {
			var nextNodes []string
			for _, node := range currentNodes {
				// Find children
				nodeKey, err := ToKey(node)
				if err != nil {
					b.Fatal(err)
				}

				seq, _ := pLoad.Select(map[string]*BytesRange{
					"source": NewBytesRange(nodeKey, nodeKey, true, true, nil),
				}, nil)
				for row := range seq {
					val, _ := row.Get("target")
					target := val.(string)
					if !visited[target] {
						visited[target] = true
						nextNodes = append(nextNodes, target)
					}
				}
			}
			currentNodes = nextNodes
		}
	}
	b.Run("Iterative_ClientSide", func(b *testing.B) {
		for b.Loop() {
			iterativeLoopBody()
		}
	})
}

func BenchmarkRecursionWithNoise(b *testing.B) {
	noiseLevels := []int{0, 1000, 10000, 100000}

	for _, noiseCount := range noiseLevels {
		b.Run(fmt.Sprintf("Noise_%d", noiseCount), func(b *testing.B) {
			db, cleanup := setupBenchmarkDB(b)
			defer cleanup()

			// 1. Setup Data: Graph + Noise
			depth := 100
			tx, _ := db.Begin(true)
			defer tx.Rollback()

			relation := "graph"
			p, _ := tx.CreatePersistent(relation, map[string]ColumnSpec{
				"source": {Indexed: true},
				"target": {Indexed: true},
				"type":   {Indexed: true},
			})

			// Insert "Relevant" Graph Path: node_0 -> node_1 ... -> node_100
			for i := range depth {
				p.Insert(map[string]any{
					"source": fmt.Sprintf("node_%d", i),
					"target": fmt.Sprintf("node_%d", i+1),
					"type":   "path",
				})
			}

			// Insert "Noise" Data
			for i := range noiseCount {
				p.Insert(map[string]any{
					"source": fmt.Sprintf("noise_%d", i),
					"target": fmt.Sprintf("noise_%d_end", i),
					"type":   "noise",
				})
			}
			// Create cycle to match BenchmarkRecursion logic
			p.Insert(map[string]any{
				"source": fmt.Sprintf("node_%d", depth),
				"target": "node_0",
			})
			tx.Commit()

			// 2. Define Benchmark Body
			recursiveLoopBody := func() {
				rtx, _ := db.Begin(true)
				defer rtx.Rollback()

				q, _ := rtx.CreateRecursion("descendants", map[string]ColumnSpec{
					"target": {},
					"source": {Indexed: true},
					"type":   {Indexed: true},
				})

				// Base case: direct children of node_0
				edge, _ := rtx.LoadPersistent(relation)
				q.AddBranch(edge)

				// Recursive step: children of discovered targets
				q.AddBranch(q.Project(map[string]string{
					"source": "source",
					"join":   "target",
					"type":   "type",
				}).Join(edge.Project(map[string]string{
					"join":   "source",
					"target": "target",
					"type":   "type",
				})).Project(map[string]string{
					"source": "source",
					"target": "target",
					"type":   "type",
				}))

				// Filter to only follow "path" type edges
				startKey, _ := ToKey("node_0")
				filters := map[string]*BytesRange{
					"source": NewBytesRange(startKey, startKey, true, true, nil),
					"type":   NewBytesRange([]byte("path"), []byte("path"), true, true, nil),
				}

				// Execute
				seq, _ := q.Select(filters, nil)
				for range seq {
				}
			}

			b.ResetTimer()
			for b.Loop() {
				recursiveLoopBody()
			}
		})
	}
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

	db, err := OpenDB(&MsgpackMaUn, dbPath, 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// 2. Initialize Schema
	initTx, err := db.Begin(true)
	if err != nil {
		b.Fatal(err)
	}
	_, err = initTx.CreatePersistent("bench_concurrent", map[string]ColumnSpec{
		"id":  {Indexed: true},
		"val": {},
	})
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
		p, _ := tx.LoadPersistent("bench_concurrent")
		for i := range initialCount {
			p.Insert(map[string]any{
				"id":  fmt.Sprintf("item-%d", i),
				"val": i,
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
				// Use current nanotime + pointer address (or just atomic increment) to seed
				// Here we just use time because exact determinism isn't required for load gen
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				for pb.Next() {
					// Decide: Read or Write?
					if rng.Intn(100) < writePerc {
						// WRITE TRANSACTION
						tx, err := db.Begin(true)
						if err != nil {
							b.Fatal(err)
						}
						p, err := tx.LoadPersistent("bench_concurrent")
						if err != nil {
							tx.Rollback()
							b.Fatal(err)
						}

						// Unique ID for new item
						uid := atomic.AddInt64(&writeCounter, 1)
						err = p.Insert(map[string]any{
							"id":  fmt.Sprintf("new-%d-%d", uid, rng.Int()),
							"val": int(uid),
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
						p, err := tx.LoadPersistent("bench_concurrent")
						if err != nil {
							tx.Rollback()
							b.Fatal(err)
						}

						// Read a random existing item from the initial set
						targetID := rng.Intn(initialCount)
						key, _ := ToKey(fmt.Sprintf("item-%d", targetID))
						f := map[string]*BytesRange{
							"id": NewBytesRange(key, key, true, true, nil),
						}

						seq, _ := p.Select(f, nil)
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
	p, err := tx.CreatePersistent("update_seq", map[string]ColumnSpec{
		"id":  {Indexed: true},
		"val": {},
	})
	if err != nil {
		b.Fatal(err)
	}

	// Insert a record to update
	err = p.Insert(map[string]any{
		"id":  "1",
		"val": 0,
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
			p, err := tx.LoadPersistent("update_seq")
			if err != nil {
				return err
			}

			// Update the record
			key, err := ToKey("1")
			if err != nil {
				return err
			}
			ranges := map[string]*BytesRange{
				"id": NewBytesRange(key, key, true, true, nil),
			}

			return p.Update(ranges, nil, map[string]any{
				"val": i,
			})
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
	_, err = initTx.CreatePersistent("bench_batch_concurrent", map[string]ColumnSpec{
		"id":  {Indexed: true},
		"val": {},
	})
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
		p, _ := tx.LoadPersistent("bench_batch_concurrent")
		for i := range initialCount {
			p.Insert(map[string]any{
				"id":  fmt.Sprintf("item-%d", i),
				"val": i,
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
							p, err := tx.LoadPersistent("bench_batch_concurrent")
							if err != nil {
								return err
							}

							// Unique ID for new item
							uid := atomic.AddInt64(&writeCounter, 1)
							return p.Insert(map[string]any{
								"id":  fmt.Sprintf("new-%d-%d", uid, rng.Int()),
								"val": int(uid),
							})
						})
						if err != nil {
							b.Fatal(err)
						}

					} else {
						// READ TRANSACTION (Batched/View)
						// Reads are typically NOT batched in BoltDB in the same way,
						// but usually just separate Views.
						// However, if we want to use Batch for everything, we can.
						// Typically we use View for reads.
						err := db.View(func(tx *Tx) error {
							p, err := tx.LoadPersistent("bench_batch_concurrent")
							if err != nil {
								return err
							}

							// Read a random existing item from the initial set
							targetID := rng.Intn(initialCount)
							key, _ := ToKey(fmt.Sprintf("item-%d", targetID))
							f := map[string]*BytesRange{
								"id": NewBytesRange(key, key, true, true, nil),
							}

							seq, _ := p.Select(f, nil)
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
	// Note: ReadOnly isn't interesting for Batch comparisons usually, but good for baseline.
	// WriteOnly is where we expect Batch to shine vs UpdateSequential (if concurrent).
	b.SetParallelism(100)             // Force high parallelism to simulate realistic load
	runWorkload(b, "ReadOnly", 0)     // 0% writes
	runWorkload(b, "WriteOnly", 100)  // 100% writes
	runWorkload(b, "Mixed_90_10", 10) // 10% writes
	runWorkload(b, "Mixed_50_50", 50) // 50% writes
}
