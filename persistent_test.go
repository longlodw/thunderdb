package thunder

import (
	"os"
	"testing"

	"github.com/openkvlab/boltdb"
)

func TestPersistent_BasicCRUD(t *testing.T) {
	// Setup temporary DB
	tmpfile, err := os.CreateTemp("", "thunder_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := boltdb.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	relation := "users"
	columns := []string{"id", "username", "age"}
	// Index on username
	indexes := map[string][]string{
		"username": {"username"},
	}

	// 1. Create Schema & Insert Data
	err = db.Update(func(tx *boltdb.Tx) error {
		p, err := CreatePersistent(tx, relation, &JsonMaUn, columns, indexes)
		if err != nil {
			return err
		}

		// Insert user1
		// Use float64 for age because JSON unmarshalling will produce float64
		user1 := map[string]any{"id": "1", "username": "alice", "age": 30.0}
		if err := p.Insert(user1); err != nil {
			return err
		}

		// Insert user2
		user2 := map[string]any{"id": "2", "username": "bob", "age": 25.0}
		if err := p.Insert(user2); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// 2. Read Data (Select)
	err = db.View(func(tx *boltdb.Tx) error {
		p, err := LoadPersistent(tx, relation, &JsonMaUn)
		if err != nil {
			return err
		}

		// Test A: Indexed Select (Eq)
		// Should use the index on 'username'
		// Note: Persistent expects index values to be a slice []any for composite key support
		op := Eq([]any{"alice"})
		op.Field = "username" // Matches index name
		seq, err := p.Select(op)
		if err != nil {
			return err
		}

		count := 0
		for val, err := range seq {
			if err != nil {
				return err
			}
			count++
			if val["username"] != "alice" {
				t.Errorf("Expected username alice, got %v", val["username"])
			}
			// Verify other fields loaded
			if val["age"] != 30.0 {
				t.Errorf("Expected age 30, got %v", val["age"])
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 result for alice, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// 3. Delete Data
	err = db.Update(func(tx *boltdb.Tx) error {
		p, err := LoadPersistent(tx, relation, &JsonMaUn)
		if err != nil {
			return err
		}

		// Delete bob
		op := Eq([]any{"bob"})
		op.Field = "username"
		if err := p.Delete(op); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. Verify Deletion
	err = db.View(func(tx *boltdb.Tx) error {
		p, err := LoadPersistent(tx, relation, &JsonMaUn)
		if err != nil {
			return err
		}

		// Query bob
		op := Eq([]any{"bob"})
		op.Field = "username"
		seq, err := p.Select(op)
		if err != nil {
			return err
		}

		count := 0
		for _, err := range seq {
			if err != nil {
				return err
			}
			count++
		}
		if count != 0 {
			t.Errorf("Expected 0 results for bob, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Verify delete failed: %v", err)
	}
}

func TestPersistent_NonIndexedSelect(t *testing.T) {
	// Setup temporary DB
	tmpfile, err := os.CreateTemp("", "thunder_test_scan_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := boltdb.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	relation := "items"
	columns := []string{"id", "price"}
	// No indexes
	indexes := map[string][]string{}

	err = db.Update(func(tx *boltdb.Tx) error {
		p, err := CreatePersistent(tx, relation, &JsonMaUn, columns, indexes)
		if err != nil {
			return err
		}
		if err := p.Insert(map[string]any{"id": "A", "price": 10.0}); err != nil {
			return err
		}
		if err := p.Insert(map[string]any{"id": "B", "price": 20.0}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *boltdb.Tx) error {
		p, err := LoadPersistent(tx, relation, &JsonMaUn)
		if err != nil {
			return err
		}

		// Eq on non-indexed field
		op := Eq(20.0)
		op.Field = "price"
		seq, err := p.Select(op)
		if err != nil {
			return err
		}

		count := 0
		for val, err := range seq {
			if err != nil {
				return err
			}
			count++
			if val["id"] != "B" {
				t.Errorf("Expected item B, got %v", val["id"])
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 result for price 20, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPersistent_Projection(t *testing.T) {
	// Setup temporary DB
	tmpfile, err := os.CreateTemp("", "thunder_test_proj_*.db")
	if err != nil {
		t.Fatal(err)
	}
	dbPath := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(dbPath)

	db, err := boltdb.Open(dbPath, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	relation := "users"
	columns := []string{"id", "username", "age"}
	// Index on username
	indexes := map[string][]string{
		"username": {"username"},
	}

	// 1. Create Schema & Insert Data
	err = db.Update(func(tx *boltdb.Tx) error {
		p, err := CreatePersistent(tx, relation, &JsonMaUn, columns, indexes)
		if err != nil {
			return err
		}

		user1 := map[string]any{"id": "1", "username": "alice", "age": 30.0}
		if err := p.Insert(user1); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Test Projection
	err = db.View(func(tx *boltdb.Tx) error {
		p, err := LoadPersistent(tx, relation, &JsonMaUn)
		if err != nil {
			return err
		}

		// Create Projection: rename 'username' -> 'login_name'
		mapping := map[string]string{
			"id":       "user_id",
			"username": "login_name",
			"age":      "user_age",
		}
		proj, err := p.Project(mapping)
		if err != nil {
			return err
		}

		// Select using projected field name
		op := Eq([]any{"alice"})
		op.Field = "login_name" // Use projected name
		seq, err := proj.Select(op)
		if err != nil {
			return err
		}

		count := 0
		for val, err := range seq {
			if err != nil {
				return err
			}
			count++
			if val["login_name"] != "alice" {
				t.Errorf("Expected login_name alice, got %v", val["login_name"])
			}
			if val["user_age"] != 30.0 {
				t.Errorf("Expected user_age 30, got %v", val["user_age"])
			}
			if _, ok := val["username"]; ok {
				t.Errorf("Did not expect original field username")
			}
		}
		if count != 1 {
			t.Errorf("Expected 1 result, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
