package thunder

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
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

			for b.Loop() {
				// Re-create DB/Table for each iteration to prevent growth affecting timing?
				// Or typically we just insert N records in the loop.
				// Since we want to measure "Insert", usually we do one transaction per batch or per item.
				// For simplicity and speed, let's do a batch insert inside the loop,
				// but that means we are benchmarking N * count inserts.
				// Let's structure it so we measure the time to insert 'count' records.
				tx, _ := db.Begin(true)
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
		})

		b.Run(fmt.Sprintf("WithIndex_%d", count), func(b *testing.B) {
			db, cleanup := setupBenchmarkDB(b)
			defer cleanup()

			for b.Loop() {
				tx, _ := db.Begin(true)
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
		})
	}
}

func BenchmarkSelect(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b)
	defer cleanup()

	// Prep data: 10k records
	count := 10000
	tx, _ := db.Begin(true)

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
			seq, _ := pLoadNoIdx.Select(op)
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
			seq, _ := pLoadIdx.Select(op)
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
			seq, _ := pLoadNoIdx.Select(op1, op2)
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
			seq, _ := pLoadIdx.Select(op1, op2)
			for range seq {
				// drain
			}
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
	readTx.Rollback()

	b.Run("Recursive_Engine", func(b *testing.B) {
		for b.Loop() {
			// Transaction per op because CreatePersistent modifies DB
			rtx, _ := db.Begin(true)
			// Cleanup temp query backing is handled by Rollback

			q, _ := rtx.CreateQuery("descendants", []string{"target"}, true)

			// Base case: direct children of node_0
			baseP, _ := rtx.LoadPersistent(relation)
			// Select target where source = node_0
			baseProj, _ := baseP.Project(map[string]string{"target": "target"})

			// Creating a helper relation for the start node constraint
			startNodeRel := "start_node"
			startNodeP, _ := rtx.CreatePersistent(startNodeRel, map[string]ColumnSpec{
				"source": {Indexed: true},
			})
			startNodeP.Insert(map[string]any{"source": "node_0"})

			q.AddBody(baseProj, startNodeP)

			// Recursive step: children of discovered targets
			// Join graph G on G.source = descendant.target
			recP, _ := rtx.LoadPersistent(relation)
			recProj, _ := recP.Project(map[string]string{"target": "target"})

			// We need to join q (source of truth for recursion) with recP
			// q(target) -> recP(source) -> output(target)
			q.AddBody(q, recProj)

			// Execute
			seq, _ := q.Select()
			for range seq {
			}
			rtx.Rollback()
		}
	})

	b.Run("Iterative_ClientSide", func(b *testing.B) {
		for b.Loop() {
			rtx, _ := db.Begin(false)
			pLoad, _ := rtx.LoadPersistent(relation)

			currentNodes := []string{"node_0"}
			visited := map[string]bool{"node_0": true}

			// Iterate until no new nodes
			for len(currentNodes) > 0 {
				var nextNodes []string
				for _, node := range currentNodes {
					// Find children
					op := Eq("source", node)
					seq, _ := pLoad.Select(op)
					for row := range seq {
						target := row["target"].(string)
						if !visited[target] {
							visited[target] = true
							nextNodes = append(nextNodes, target)
						}
					}
				}
				currentNodes = nextNodes
			}
			rtx.Rollback()
		}
	})
}
