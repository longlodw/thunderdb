package thunderdb

import (
	"os"
	"time"

	"github.com/openkvlab/boltdb"
)

// DB represents an open ThunderDB database. It wraps an underlying bolt database
// and provides transaction management for persistent data storage.
//
// A DB is safe for concurrent use by multiple goroutines.
type DB struct {
	db *boltdb.DB
}

// DBOptions configures the database behavior. It is an alias for boltdb.Options.
// Common options include:
//   - Timeout: time to wait for a file lock
//   - NoSync: disable fsync after each commit (faster but less durable)
//   - ReadOnly: open database in read-only mode
type DBOptions = boltdb.Options

// OpenDB opens a ThunderDB database at the specified path.
// If the file does not exist, it will be created with the given file mode.
// Options can be nil for default settings.
//
// Example:
//
//	db, err := thunderdb.OpenDB("my.db", 0600, nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
func OpenDB(path string, mode os.FileMode, options *DBOptions) (*DB, error) {
	bdb, err := boltdb.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return &DB{db: bdb}, nil
}

// Close releases all database resources and closes the underlying file.
// All transactions must be closed before calling Close.
func (d *DB) Close() error {
	return d.db.Close()
}

// Begin starts a new transaction. If writable is true, the transaction can
// modify the database; otherwise it is read-only.
//
// For manually managed transactions, you must always call Rollback() to release
// resources. If you call Commit() successfully, the subsequent Rollback() will
// be a no-op.
//
// For most use cases, prefer the managed transaction methods: View, Update, or Batch.
//
// Example:
//
//	tx, err := db.Begin(true)
//	if err != nil {
//	    return err
//	}
//	defer tx.Rollback()
//
//	// ... perform operations ...
//
//	return tx.Commit()
func (d *DB) Begin(writable bool) (*Tx, error) {
	tx, err := d.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx: tx, stores: make(map[string]*storage),
	}, nil
}

// View executes a function within the context of a read-only transaction.
// The transaction is automatically rolled back after the function returns.
//
// Any error returned by the function is propagated to the caller.
//
// Example:
//
//	err := db.View(func(tx *thunderdb.Tx) error {
//	    users, err := tx.StoredQuery("users")
//	    if err != nil {
//	        return err
//	    }
//	    // ... read data ...
//	    return nil
//	})
func (d *DB) View(fn func(*Tx) error) error {
	return d.db.View(func(btx *boltdb.Tx) error {
		tx := &Tx{
			tx:      btx,
			managed: true,
			stores:  make(map[string]*storage),
		}
		defer tx.cleanupTempTx()
		return fn(tx)
	})
}

// Update executes a function within the context of a read-write transaction.
// The transaction is automatically committed if the function returns nil.
// If the function returns an error or panics, the transaction is rolled back.
//
// Example:
//
//	err := db.Update(func(tx *thunderdb.Tx) error {
//	    return tx.Insert("users", map[int]any{0: "1", 1: "alice"})
//	})
func (d *DB) Update(fn func(*Tx) error) error {
	return d.db.Update(func(btx *boltdb.Tx) error {
		tx := &Tx{
			tx:      btx,
			managed: true,
			stores:  make(map[string]*storage),
		}
		defer tx.cleanupTempTx()
		return fn(tx)
	})
}

// Batch executes a function within the context of a read-write managed transaction.
// If multiple goroutines call Batch simultaneously, the internal DB will attempt to
// coalesce them into a single disk sync (group commit).
//
// This is useful for high-concurrency write scenarios where throughput is more
// important than the latency of individual writes.
//
// The transaction is automatically committed if the function returns nil.
// If the function returns an error, the transaction is rolled back.
// If the function panics, the transaction is rolled back and the panic is propagated.
func (d *DB) Batch(fn func(*Tx) error) error {
	return d.db.Batch(func(btx *boltdb.Tx) error {
		tx := &Tx{
			tx:      btx,
			managed: true,
			stores:  make(map[string]*storage),
		}
		defer tx.cleanupTempTx()
		return fn(tx)
	})
}

// SetMaxBatchDelay sets the maximum amount of time that the batcher will wait
// for additional transactions before closing the batch.
//
// A higher delay can increase throughput by allowing more transactions to participate
// in a single batch, but it increases the latency of individual transactions.
func (d *DB) SetMaxBatchDelay(delay time.Duration) {
	d.db.MaxBatchDelay = delay
}

// SetMaxBatchSize sets the maximum size of a batch.
//
// If a batch exceeds this size, it is committed immediately, even if MaxBatchDelay
// has not been reached.
func (d *DB) SetMaxBatchSize(size int) {
	d.db.MaxBatchSize = size
}

// SetAllocSize sets the size of the initial memory allocation for the database.
func (d *DB) SetAllocSize(size int) {
	d.db.AllocSize = size
}

// MaxBatchDelay returns the current maximum batch delay.
func (d *DB) MaxBatchDelay() time.Duration {
	return d.db.MaxBatchDelay
}

// MaxBatchSize returns the current maximum batch size.
func (d *DB) MaxBatchSize() int {
	return d.db.MaxBatchSize
}

// AllocSize returns the current allocation size.
func (d *DB) AllocSize() int {
	return d.db.AllocSize
}
