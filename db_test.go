package thunderdb

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// setupSnapshotTestDB creates a temporary database and populates it with
// a simple schema and a few rows for snapshot tests.
func setupSnapshotTestDB(t *testing.T) (*DB, string, func()) {
	t.Helper()

	tmpfile, err := os.CreateTemp("", "thunder_snapshot_*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := tmpfile.Name()
	tmpfile.Close()

	db, err := OpenDB(path, 0600, nil)
	if err != nil {
		os.Remove(path)
		t.Fatal(err)
	}

	cleanup := func() {
		db.Close()
		os.Remove(path)
	}

	// Populate some data
	err = db.Update(func(tx *Tx) error {
		relation := "users"
		if err := tx.CreateStorage(relation, 3,
			IndexInfo{ReferencedCols: []int{1}, IsUnique: true},
		); err != nil {
			return err
		}
		if err := tx.Insert(relation, map[int]any{0: "1", 1: "alice", 2: 30.0}); err != nil {
			return err
		}
		if err := tx.Insert(relation, map[int]any{0: "2", 1: "bob", 2: 25.0}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		cleanup()
		t.Fatal(err)
	}

	return db, path, cleanup
}

func TestSnapshot_Basic(t *testing.T) {
	db, _, cleanup := setupSnapshotTestDB(t)
	defer cleanup()

	backupFile, err := os.CreateTemp("", "thunder_snapshot_backup_*.db")
	if err != nil {
		t.Fatal(err)
	}
	backupPath := backupFile.Name()
	backupFile.Close()
	defer os.Remove(backupPath)

	f, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	n, err := db.Snapshot(f)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected snapshot to write >0 bytes, got %d", n)
	}

	// Open the snapshot as a new database and verify data is present.
	backupDB, err := OpenDB(backupPath, 0600, &DBOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("opening snapshot DB failed: %v", err)
	}
	defer backupDB.Close()

	err = backupDB.View(func(tx *Tx) error {
		users, err := tx.StoredQuery("users")
		if err != nil {
			return err
		}
		rows, err := tx.Select(users)
		if err != nil {
			return err
		}
		count := 0
		for row, err := range rows {
			if err != nil {
				return err
			}
			if row == nil {
				continue
			}
			count++
		}
		if count != 2 {
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verifying snapshot data failed: %v", err)
	}
}

func TestSnapshot_EmptyDB(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "thunder_snapshot_empty_*.db")
	if err != nil {
		t.Fatal(err)
	}
	path := tmpfile.Name()
	tmpfile.Close()
	defer os.Remove(path)

	db, err := OpenDB(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var buf bytes.Buffer
	n, err := db.Snapshot(&buf)
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected snapshot to write >0 bytes, got %d", n)
	}

	// Ensure resulting bytes can be opened as a BoltDB file.
	backupPath := path + ".copy"
	if err := os.WriteFile(backupPath, buf.Bytes(), 0600); err != nil {
		t.Fatalf("writing snapshot copy failed: %v", err)
	}
	defer os.Remove(backupPath)

	backupDB, err := OpenDB(backupPath, 0600, &DBOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("opening snapshot copy failed: %v", err)
	}
	backupDB.Close()
}
