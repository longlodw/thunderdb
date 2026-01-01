package thunderdb

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
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
			op := Eq("val", target)
			f, err := ToKeyRanges(op)
			if err != nil {
				b.Fatal(err)
			}
			seq, _ := pLoadNoIdx.Select(f)
			for range seq {
				// drain
			}
		}
	})

	b.Run("Indexed_Eq", func(b *testing.B) {
		for b.Loop() {
			// Search for random val (indexed)
			target := float64(rand.Intn(count))
			op := Eq("val", target)
			f, err := ToKeyRanges(op)
			if err != nil {
				b.Fatal(err)
			}
			seq, _ := pLoadIdx.Select(f)
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
			op1 := Ge("val", start)
			op2 := Lt("val", end)
			f, err := ToKeyRanges(op1, op2)
			if err != nil {
				b.Fatal(err)
			}
			seq, _ := pLoadNoIdx.Select(f)
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
			op1 := Ge("val", start)
			op2 := Lt("val", end)
			f, err := ToKeyRanges(op1, op2)
			if err != nil {
				b.Fatal(err)
			}
			seq, _ := pLoadIdx.Select(f)
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
			op := Eq("region", "North")
			f, _ := ToKeyRanges(op)
			seq, _ := qAll.Select(f)
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

			seq, _ := q.Select(nil)
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
	readTx, _ := db.Begin(false) // Note: Recursive query might need write tx if it creates temp backing?
	defer readTx.Rollback()

	recursiveLoopBody := func() {
		rtx, _ := db.Begin(true)
		defer rtx.Rollback()

		q, _ := rtx.CreateRecursion("descendants", map[string]ColumnSpec{
			"target": {},
		})

		// Base case: direct children of node_0
		baseP, _ := rtx.LoadPersistent(relation)
		// Select target where source = node_0
		baseProj := baseP.Project(map[string]string{"target": "target"})

		// Creating a helper relation for the start node constraint
		startNodeRel := "start_node"
		startNodeP, _ := rtx.CreatePersistent(startNodeRel, map[string]ColumnSpec{
			"source": {Indexed: true},
		})
		startNodeP.Insert(map[string]any{"source": "node_0"})

		q.AddBranch(baseProj.Join(startNodeP))

		// Recursive step: children of discovered targets
		// Join graph G on G.source = descendant.target
		recP, _ := rtx.LoadPersistent(relation)
		recProj := recP.Project(map[string]string{"target": "target"})

		// We need to join q (source of truth for recursion) with recP
		// q(target) -> recP(source) -> output(target)
		q.AddBranch(q.Join(recProj))

		// Execute
		seq, _ := q.Select(nil)
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

				seq, _ := pLoad.Select(map[string]*keyRange{
					"source": KeyRange(nodeKey, nodeKey, true, true, nil),
				})
				for row, _ := range seq {
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
				})

				// Base case: direct children of node_0
				baseP, _ := rtx.LoadPersistent(relation)
				baseProj := baseP.Project(map[string]string{"target": "target"})

				startNodeRel := "start_node"
				startNodeP, _ := rtx.CreatePersistent(startNodeRel, map[string]ColumnSpec{
					"source": {Indexed: true},
				})
				startNodeP.Insert(map[string]any{"source": "node_0"})

				q.AddBranch(baseProj.Join(startNodeP))

				// Recursive step: children of discovered targets
				recP, _ := rtx.LoadPersistent(relation)
				recProj := recP.Project(map[string]string{"target": "target"})

				q.AddBranch(q.Join(recProj))

				// Execute
				seq, _ := q.Select(nil)
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
