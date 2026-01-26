package thunderdb

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/openkvlab/boltdb"
)

// Stats contains database statistics. All counters are cumulative since the
// database was opened (or since the last ResetStats call).
//
// Stats is safe for concurrent access - call db.Stats() to get a snapshot.
type Stats struct {
	// OpenedAt is when the database was opened.
	OpenedAt time.Time

	// Transaction counts
	ReadTxTotal     int64 // Total read-only transactions started
	WriteTxTotal    int64 // Total write transactions started
	TxCommitTotal   int64 // Successful commits
	TxRollbackTotal int64 // Rollbacks (explicit or implicit)
	TxOpenCount     int64 // Currently open transactions

	// Operation counts
	RowsInserted int64 // Total rows inserted
	RowsUpdated  int64 // Total rows updated
	RowsDeleted  int64 // Total rows deleted
	RowsRead     int64 // Total rows returned from queries

	// Query counts
	QueriesTotal    int64 // Total Select() calls
	JoinsTotal      int64 // Join operations created
	IndexScansTotal int64 // Queries that used an index
	FullScansTotal  int64 // Queries that did full table scans

	// Storage counts
	StoragesCreated int64 // Total relations created
	StoragesDeleted int64 // Total relations dropped

	// Timing (cumulative durations)
	TxDuration     time.Duration // Total time spent in transactions
	QueryDuration  time.Duration // Total time spent in Select()
	InsertDuration time.Duration // Total time spent in Insert()
	UpdateDuration time.Duration // Total time spent in Update()
	DeleteDuration time.Duration // Total time spent in Delete()

	// BoltDB is the underlying BoltDB statistics (passthrough).
	BoltDB boltdb.Stats
}

// String returns a human-readable multi-line summary of the statistics.
func (s Stats) String() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("ThunderDB Stats (opened %s)\n", s.OpenedAt.Format(time.RFC3339)))
	b.WriteString(strings.Repeat("-", 50) + "\n")

	// Transactions
	b.WriteString("Transactions:\n")
	b.WriteString(fmt.Sprintf("  Read:       %s    Write:      %s\n",
		formatCount(s.ReadTxTotal), formatCount(s.WriteTxTotal)))
	b.WriteString(fmt.Sprintf("  Commits:    %s    Rollbacks:  %s\n",
		formatCount(s.TxCommitTotal), formatCount(s.TxRollbackTotal)))
	b.WriteString(fmt.Sprintf("  Open:       %s\n", formatCount(s.TxOpenCount)))
	b.WriteString(fmt.Sprintf("  Total Time: %s\n", formatDuration(s.TxDuration)))

	// Operations
	b.WriteString("\nOperations:\n")
	b.WriteString(fmt.Sprintf("  Inserted:   %s    (total: %s)\n",
		formatCount(s.RowsInserted), formatDuration(s.InsertDuration)))
	b.WriteString(fmt.Sprintf("  Updated:    %s    (total: %s)\n",
		formatCount(s.RowsUpdated), formatDuration(s.UpdateDuration)))
	b.WriteString(fmt.Sprintf("  Deleted:    %s    (total: %s)\n",
		formatCount(s.RowsDeleted), formatDuration(s.DeleteDuration)))
	b.WriteString(fmt.Sprintf("  Rows Read:  %s\n", formatCount(s.RowsRead)))

	// Queries
	b.WriteString("\nQueries:\n")
	b.WriteString(fmt.Sprintf("  Total:      %s    (total: %s)\n",
		formatCount(s.QueriesTotal), formatDuration(s.QueryDuration)))
	b.WriteString(fmt.Sprintf("  Joins:      %s\n", formatCount(s.JoinsTotal)))
	b.WriteString(fmt.Sprintf("  Index Scans: %s   Full Scans: %s\n",
		formatCount(s.IndexScansTotal), formatCount(s.FullScansTotal)))

	// Storage
	b.WriteString("\nStorage:\n")
	b.WriteString(fmt.Sprintf("  Created:    %s    Deleted:    %s\n",
		formatCount(s.StoragesCreated), formatCount(s.StoragesDeleted)))

	// BoltDB stats
	b.WriteString("\nBoltDB:\n")
	b.WriteString(fmt.Sprintf("  Free Pages:    %d    Pending Pages: %d\n",
		s.BoltDB.FreePageN, s.BoltDB.PendingPageN))
	b.WriteString(fmt.Sprintf("  Free Alloc:    %s    Freelist Size: %s\n",
		formatBytes(s.BoltDB.FreeAlloc), formatBytes(s.BoltDB.FreelistInuse)))
	b.WriteString(fmt.Sprintf("  Open Read Tx:  %d    Total Tx:      %d\n",
		s.BoltDB.OpenTxN, s.BoltDB.TxN))

	return b.String()
}

// internalStats holds atomic counters for thread-safe statistics tracking.
// Durations are stored as int64 nanoseconds for atomic operations.
type internalStats struct {
	// Transaction counts
	readTx    int64
	writeTx   int64
	commits   int64
	rollbacks int64
	openTx    int64

	// Operation counts
	inserts int64
	updates int64
	deletes int64
	reads   int64

	// Query counts
	queries    int64
	joins      int64
	indexScans int64
	fullScans  int64

	// Storage counts
	storagesCreated int64
	storagesDeleted int64

	// Durations (nanoseconds)
	txDuration     int64
	queryDuration  int64
	insertDuration int64
	updateDuration int64
	deleteDuration int64
}

// snapshot returns a Stats struct with current values.
func (s *internalStats) snapshot(openedAt time.Time, boltStats boltdb.Stats) Stats {
	return Stats{
		OpenedAt: openedAt,

		ReadTxTotal:     atomic.LoadInt64(&s.readTx),
		WriteTxTotal:    atomic.LoadInt64(&s.writeTx),
		TxCommitTotal:   atomic.LoadInt64(&s.commits),
		TxRollbackTotal: atomic.LoadInt64(&s.rollbacks),
		TxOpenCount:     atomic.LoadInt64(&s.openTx),

		RowsInserted: atomic.LoadInt64(&s.inserts),
		RowsUpdated:  atomic.LoadInt64(&s.updates),
		RowsDeleted:  atomic.LoadInt64(&s.deletes),
		RowsRead:     atomic.LoadInt64(&s.reads),

		QueriesTotal:    atomic.LoadInt64(&s.queries),
		JoinsTotal:      atomic.LoadInt64(&s.joins),
		IndexScansTotal: atomic.LoadInt64(&s.indexScans),
		FullScansTotal:  atomic.LoadInt64(&s.fullScans),

		StoragesCreated: atomic.LoadInt64(&s.storagesCreated),
		StoragesDeleted: atomic.LoadInt64(&s.storagesDeleted),

		TxDuration:     time.Duration(atomic.LoadInt64(&s.txDuration)),
		QueryDuration:  time.Duration(atomic.LoadInt64(&s.queryDuration)),
		InsertDuration: time.Duration(atomic.LoadInt64(&s.insertDuration)),
		UpdateDuration: time.Duration(atomic.LoadInt64(&s.updateDuration)),
		DeleteDuration: time.Duration(atomic.LoadInt64(&s.deleteDuration)),

		BoltDB: boltStats,
	}
}

// reset zeros all counters.
func (s *internalStats) reset() {
	atomic.StoreInt64(&s.readTx, 0)
	atomic.StoreInt64(&s.writeTx, 0)
	atomic.StoreInt64(&s.commits, 0)
	atomic.StoreInt64(&s.rollbacks, 0)
	// Note: openTx is not reset as it represents current state

	atomic.StoreInt64(&s.inserts, 0)
	atomic.StoreInt64(&s.updates, 0)
	atomic.StoreInt64(&s.deletes, 0)
	atomic.StoreInt64(&s.reads, 0)

	atomic.StoreInt64(&s.queries, 0)
	atomic.StoreInt64(&s.joins, 0)
	atomic.StoreInt64(&s.indexScans, 0)
	atomic.StoreInt64(&s.fullScans, 0)

	atomic.StoreInt64(&s.storagesCreated, 0)
	atomic.StoreInt64(&s.storagesDeleted, 0)

	atomic.StoreInt64(&s.txDuration, 0)
	atomic.StoreInt64(&s.queryDuration, 0)
	atomic.StoreInt64(&s.insertDuration, 0)
	atomic.StoreInt64(&s.updateDuration, 0)
	atomic.StoreInt64(&s.deleteDuration, 0)
}

// formatCount formats an integer with comma separators for readability.
func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	s := fmt.Sprintf("%d", n)
	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

// formatDuration formats a duration in a human-readable way.
func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fus", float64(d.Nanoseconds())/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
	if d < time.Minute {
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.2fm", d.Minutes())
	}
	return fmt.Sprintf("%.2fh", d.Hours())
}

// formatBytes formats bytes in a human-readable way.
func formatBytes(b int) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/GB)
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/MB)
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/KB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}
