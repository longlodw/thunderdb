package thunderdb

import (
	"encoding/gob"
	"os"
	"time"

	"github.com/openkvlab/boltdb"
)

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
	gob.Register(string(""))
	gob.Register(int(0))
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(bool(false))
	gob.Register(storageMetadata{})
	gob.Register(ColumnSpec{})
	gob.Register(computedColumnSpec{})
}

type DB struct {
	db   *boltdb.DB
	maUn MarshalUnmarshaler
}

type DBOptions = boltdb.Options

func OpenDB(maUn MarshalUnmarshaler, path string, mode os.FileMode, options *DBOptions) (*DB, error) {
	bdb, err := boltdb.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	return &DB{db: bdb, maUn: maUn}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}

func (d *DB) Begin(writable bool) (*Tx, error) {
	tx, err := d.db.Begin(writable)
	if err != nil {
		return nil, err
	}

	return &Tx{
		tx:   tx,
		maUn: d.maUn,
	}, nil
}

func (d *DB) View(fn func(*Tx) error) error {
	return d.db.View(func(btx *boltdb.Tx) error {
		tx := &Tx{
			tx:      btx,
			maUn:    d.maUn,
			managed: true,
		}
		defer tx.cleanupTempTx()
		return fn(tx)
	})
}

func (d *DB) Update(fn func(*Tx) error) error {
	return d.db.Update(func(btx *boltdb.Tx) error {
		tx := &Tx{
			tx:      btx,
			maUn:    d.maUn,
			managed: true,
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
			maUn:    d.maUn,
			managed: true,
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
