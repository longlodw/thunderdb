package thunder

import (
	"fmt"
	"testing"
)

func TestPersistent_AllOperators(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	relation := "numbers"
	// Create one indexed column and one non-indexed to test both paths
	// indexes := map[string][]string{"indexed_val": {"indexed_val"}}

	p, err := tx.CreatePersistent(relation, map[string]ColumnSpec{
		"id":          {},
		"val":         {},
		"indexed_val": {Indexed: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert values: 0, 10, 20, 30, 40, 50
	for i := 0; i <= 50; i += 10 {
		err := p.Insert(map[string]any{
			"id":          fmt.Sprintf("%d", i),
			"val":         float64(i),
			"indexed_val": float64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	p, err = tx.LoadPersistent(relation)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		ops      []Op
		expected int // expected count of results
	}{
		// Non-indexed column 'val'
		{"Eq val=20", []Op{Eq("val", 20.0)}, 1},
		{"Ne val=20", []Op{Ne("val", 20.0)}, 5}, // 0,10,30,40,50
		{"Gt val=20", []Op{Gt("val", 20.0)}, 3}, // 30,40,50
		{"Lt val=20", []Op{Lt("val", 20.0)}, 2}, // 0,10
		{"Ge val=20", []Op{Ge("val", 20.0)}, 4}, // 20,30,40,50
		{"Le val=20", []Op{Le("val", 20.0)}, 3}, // 0,10,20

		// Indexed column 'indexed_val'
		{"Eq idx=20", []Op{Eq("indexed_val", 20.0)}, 1},
		{"Ne idx=20", []Op{Ne("indexed_val", 20.0)}, 5},
		{"Gt idx=20", []Op{Gt("indexed_val", 20.0)}, 3},
		{"Lt idx=20", []Op{Lt("indexed_val", 20.0)}, 2},
		{"Ge idx=20", []Op{Ge("indexed_val", 20.0)}, 4},
		{"Le idx=20", []Op{Le("indexed_val", 20.0)}, 3},

		// Combinations
		{"val>10 AND val<40", []Op{Gt("val", 10.0), Lt("val", 40.0)}, 2},                 // 20, 30
		{"idx>10 AND idx<40", []Op{Gt("indexed_val", 10.0), Lt("indexed_val", 40.0)}, 2}, // 20, 30
		{"idx>10 AND val<40", []Op{Gt("indexed_val", 10.0), Lt("val", 40.0)}, 2},         // 20, 30 (Mixed)
		{"val=20 AND val=30", []Op{Eq("val", 20.0), Eq("val", 30.0)}, 0},                 // Impossible
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := Filter(tt.ops...)
			if err != nil {
				t.Fatalf("Filter creation failed: %v", err)
			}
			seq, err := p.Select(f)
			if err != nil {
				t.Fatalf("Select failed: %v", err)
			}
			count := 0
			for val, err := range seq {
				if err != nil {
					t.Fatal(err)
				}
				count++
				_ = val
			}
			if count != tt.expected {
				t.Errorf("Expected count %d, got %d", tt.expected, count)
			}
		})
	}
}
