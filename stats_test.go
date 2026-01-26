package thunderdb

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStats_TransactionCounts(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_tx.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Initial stats
	stats := db.Stats()
	if stats.ReadTxTotal != 0 {
		t.Errorf("expected ReadTxTotal=0, got %d", stats.ReadTxTotal)
	}
	if stats.WriteTxTotal != 0 {
		t.Errorf("expected WriteTxTotal=0, got %d", stats.WriteTxTotal)
	}

	// Test View (read transaction)
	err = db.View(func(tx *Tx) error {
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	stats = db.Stats()
	if stats.ReadTxTotal != 1 {
		t.Errorf("expected ReadTxTotal=1 after View, got %d", stats.ReadTxTotal)
	}

	// Test Update (write transaction)
	err = db.Update(func(tx *Tx) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	stats = db.Stats()
	if stats.WriteTxTotal != 1 {
		t.Errorf("expected WriteTxTotal=1 after Update, got %d", stats.WriteTxTotal)
	}
	if stats.TxCommitTotal != 1 {
		t.Errorf("expected TxCommitTotal=1 after successful Update, got %d", stats.TxCommitTotal)
	}

	// Test manual transaction with commit
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	stats = db.Stats()
	if stats.TxOpenCount != 1 {
		t.Errorf("expected TxOpenCount=1 during manual tx, got %d", stats.TxOpenCount)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	stats = db.Stats()
	if stats.TxOpenCount != 0 {
		t.Errorf("expected TxOpenCount=0 after commit, got %d", stats.TxOpenCount)
	}
	if stats.TxCommitTotal != 2 {
		t.Errorf("expected TxCommitTotal=2, got %d", stats.TxCommitTotal)
	}

	// Test manual transaction with rollback
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	stats = db.Stats()
	if stats.TxRollbackTotal != 1 {
		t.Errorf("expected TxRollbackTotal=1, got %d", stats.TxRollbackTotal)
	}
}

func TestStats_OperationCounts(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_ops.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Create storage and insert rows
	err = db.Update(func(tx *Tx) error {
		err := tx.CreateStorage("users", 3, []IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},
		})
		if err != nil {
			return err
		}

		// Insert 5 rows
		for i := 0; i < 5; i++ {
			err = tx.Insert("users", map[int]any{0: i, 1: "user", 2: i * 10})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	stats := db.Stats()
	if stats.RowsInserted != 5 {
		t.Errorf("expected RowsInserted=5, got %d", stats.RowsInserted)
	}
	if stats.StoragesCreated != 1 {
		t.Errorf("expected StoragesCreated=1, got %d", stats.StoragesCreated)
	}

	// Update rows
	err = db.Update(func(tx *Tx) error {
		return tx.Update("users", map[int]any{1: "updated"}, Condition{Field: 2, Operator: GTE, Value: 20})
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	stats = db.Stats()
	if stats.RowsUpdated != 3 { // rows with col2 >= 20: 20, 30, 40
		t.Errorf("expected RowsUpdated=3, got %d", stats.RowsUpdated)
	}

	// Delete rows
	err = db.Update(func(tx *Tx) error {
		return tx.Delete("users", Condition{Field: 0, Operator: LT, Value: 2})
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	stats = db.Stats()
	if stats.RowsDeleted != 2 { // rows with col0 < 2: 0, 1
		t.Errorf("expected RowsDeleted=2, got %d", stats.RowsDeleted)
	}

	// Delete storage
	err = db.Update(func(tx *Tx) error {
		return tx.DeleteStorage("users")
	})
	if err != nil {
		t.Fatalf("delete storage failed: %v", err)
	}

	stats = db.Stats()
	if stats.StoragesDeleted != 1 {
		t.Errorf("expected StoragesDeleted=1, got %d", stats.StoragesDeleted)
	}
}

func TestStats_QueryCounts(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_query.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Setup: create storage with index and insert rows
	err = db.Update(func(tx *Tx) error {
		err := tx.CreateStorage("items", 2, []IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},
		})
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			err = tx.Insert("items", map[int]any{0: i, 1: "item"})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Reset stats to isolate query stats
	db.ResetStats()

	// Query with index (EQ condition on indexed column)
	err = db.View(func(tx *Tx) error {
		q, err := tx.StoredQuery("items")
		if err != nil {
			return err
		}
		results, err := tx.Select(q, Condition{Field: 0, Operator: EQ, Value: 5})
		if err != nil {
			return err
		}
		count := 0
		for _, err := range results {
			if err != nil {
				return err
			}
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 result, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	stats := db.Stats()
	if stats.QueriesTotal != 1 {
		t.Errorf("expected QueriesTotal=1, got %d", stats.QueriesTotal)
	}
	if stats.IndexScansTotal != 1 {
		t.Errorf("expected IndexScansTotal=1 for indexed query, got %d", stats.IndexScansTotal)
	}
	if stats.RowsRead != 1 {
		t.Errorf("expected RowsRead=1, got %d", stats.RowsRead)
	}

	// Query without index (full scan on non-indexed column)
	err = db.View(func(tx *Tx) error {
		q, err := tx.StoredQuery("items")
		if err != nil {
			return err
		}
		results, err := tx.Select(q, Condition{Field: 1, Operator: EQ, Value: "item"})
		if err != nil {
			return err
		}
		count := 0
		for _, err := range results {
			if err != nil {
				return err
			}
			count++
		}
		if count != 10 {
			t.Errorf("expected 10 results, got %d", count)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	stats = db.Stats()
	if stats.QueriesTotal != 2 {
		t.Errorf("expected QueriesTotal=2, got %d", stats.QueriesTotal)
	}
	if stats.FullScansTotal != 1 {
		t.Errorf("expected FullScansTotal=1 for non-indexed query, got %d", stats.FullScansTotal)
	}
	if stats.RowsRead != 11 { // 1 from first query + 10 from second
		t.Errorf("expected RowsRead=11, got %d", stats.RowsRead)
	}
}

func TestStats_JoinCounts(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_join.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Setup: create two storages
	err = db.Update(func(tx *Tx) error {
		err := tx.CreateStorage("users", 2, []IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: true},
		})
		if err != nil {
			return err
		}
		err = tx.CreateStorage("orders", 2, []IndexInfo{
			{ReferencedCols: []int{0}, IsUnique: false},
		})
		if err != nil {
			return err
		}
		// Insert some data
		tx.Insert("users", map[int]any{0: 1, 1: "alice"})
		tx.Insert("orders", map[int]any{0: 1, 1: "order1"})
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	db.ResetStats()

	// Perform a join query
	err = db.View(func(tx *Tx) error {
		users, err := tx.StoredQuery("users")
		if err != nil {
			return err
		}
		orders, err := tx.StoredQuery("orders")
		if err != nil {
			return err
		}
		joined, err := users.Join(orders, JoinOn{LeftField: 0, RightField: 0, Operator: EQ})
		if err != nil {
			return err
		}
		results, err := tx.Select(joined)
		if err != nil {
			return err
		}
		for _, err := range results {
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("join query failed: %v", err)
	}

	stats := db.Stats()
	if stats.JoinsTotal != 1 {
		t.Errorf("expected JoinsTotal=1, got %d", stats.JoinsTotal)
	}
}

func TestStats_Timing(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_timing.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Perform operations and verify timing is non-zero
	err = db.Update(func(tx *Tx) error {
		err := tx.CreateStorage("test", 2, nil)
		if err != nil {
			return err
		}
		return tx.Insert("test", map[int]any{0: 1, 1: "value"})
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	stats := db.Stats()
	if stats.TxDuration == 0 {
		t.Error("expected TxDuration > 0")
	}
	if stats.InsertDuration == 0 {
		t.Error("expected InsertDuration > 0")
	}

	// Query timing
	err = db.View(func(tx *Tx) error {
		q, _ := tx.StoredQuery("test")
		results, _ := tx.Select(q)
		for range results {
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	stats = db.Stats()
	if stats.QueryDuration == 0 {
		t.Error("expected QueryDuration > 0")
	}
}

func TestStats_Reset(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_reset.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Perform some operations
	err = db.Update(func(tx *Tx) error {
		tx.CreateStorage("test", 2, nil)
		tx.Insert("test", map[int]any{0: 1, 1: "value"})
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	stats := db.Stats()
	if stats.WriteTxTotal == 0 {
		t.Error("expected WriteTxTotal > 0 before reset")
	}

	// Reset and verify
	db.ResetStats()

	stats = db.Stats()
	if stats.WriteTxTotal != 0 {
		t.Errorf("expected WriteTxTotal=0 after reset, got %d", stats.WriteTxTotal)
	}
	if stats.RowsInserted != 0 {
		t.Errorf("expected RowsInserted=0 after reset, got %d", stats.RowsInserted)
	}
	if stats.TxDuration != 0 {
		t.Errorf("expected TxDuration=0 after reset, got %v", stats.TxDuration)
	}

	// OpenedAt should be preserved
	if stats.OpenedAt.IsZero() {
		t.Error("expected OpenedAt to be preserved after reset")
	}
}

func TestStats_String(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_string.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Perform some operations to have non-zero stats
	err = db.Update(func(tx *Tx) error {
		tx.CreateStorage("test", 2, nil)
		tx.Insert("test", map[int]any{0: 1, 1: "value"})
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	stats := db.Stats()
	str := stats.String()

	// Verify the string contains expected sections
	expectedSections := []string{
		"ThunderDB Stats",
		"Transactions:",
		"Operations:",
		"Queries:",
		"Storage:",
		"BoltDB:",
	}

	for _, section := range expectedSections {
		if !strings.Contains(str, section) {
			t.Errorf("expected stats string to contain %q", section)
		}
	}
}

func TestStats_Concurrent(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_concurrent.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Setup
	err = db.Update(func(tx *Tx) error {
		return tx.CreateStorage("test", 2, nil)
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	// Concurrent writes and reads
	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of reads and stats calls
				db.View(func(tx *Tx) error {
					return nil
				})
				db.Stats() // Concurrent stats access
			}
		}(i)
	}

	wg.Wait()

	stats := db.Stats()
	expectedReads := int64(numGoroutines * opsPerGoroutine)
	// Account for setup transaction
	if stats.ReadTxTotal != expectedReads {
		t.Errorf("expected ReadTxTotal=%d, got %d", expectedReads, stats.ReadTxTotal)
	}
}

func TestStats_OpenedAt(t *testing.T) {
	before := time.Now()

	dbPath := t.TempDir() + "/test_stats_opened.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	after := time.Now()

	stats := db.Stats()
	if stats.OpenedAt.Before(before) || stats.OpenedAt.After(after) {
		t.Errorf("OpenedAt %v should be between %v and %v", stats.OpenedAt, before, after)
	}
}

func TestStats_BoltDBPassthrough(t *testing.T) {
	dbPath := t.TempDir() + "/test_stats_bolt.db"
	db, err := OpenDB(dbPath, 0600, nil)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	// Perform some operations
	err = db.Update(func(tx *Tx) error {
		tx.CreateStorage("test", 2, nil)
		for i := 0; i < 100; i++ {
			tx.Insert("test", map[int]any{0: i, 1: "value"})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	stats := db.Stats()

	// BoltDB stats should be accessible (passthrough is working)
	// The exact values depend on BoltDB internals, so we just verify the struct is populated
	// FreePageN and other fields might be 0 for a fresh small DB, which is valid
	t.Logf("BoltDB Stats: TxN=%d, OpenTxN=%d, FreePageN=%d, FreeAlloc=%d",
		stats.BoltDB.TxN, stats.BoltDB.OpenTxN, stats.BoltDB.FreePageN, stats.BoltDB.FreeAlloc)

	// At minimum, verify we can access the BoltDB stats struct without panic
	_ = stats.BoltDB.TxStats
}

// Test helper functions
func TestFormatCount(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{-1234, "-1,234"},
	}

	for _, tt := range tests {
		result := formatCount(tt.input)
		if result != tt.expected {
			t.Errorf("formatCount(%d) = %s, expected %s", tt.input, result, tt.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input    time.Duration
		contains string
	}{
		{500 * time.Nanosecond, "ns"},
		{500 * time.Microsecond, "us"},
		{500 * time.Millisecond, "ms"},
		{2 * time.Second, "s"},
		{2 * time.Minute, "m"},
		{2 * time.Hour, "h"},
	}

	for _, tt := range tests {
		result := formatDuration(tt.input)
		if !strings.Contains(result, tt.contains) {
			t.Errorf("formatDuration(%v) = %s, expected to contain %s", tt.input, result, tt.contains)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int
		contains string
	}{
		{500, "B"},
		{2048, "KB"},
		{2 * 1024 * 1024, "MB"},
		{2 * 1024 * 1024 * 1024, "GB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.input)
		if !strings.Contains(result, tt.contains) {
			t.Errorf("formatBytes(%d) = %s, expected to contain %s", tt.input, result, tt.contains)
		}
	}
}
