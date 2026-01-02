package thunderdb

import (
	"os"
	"time"

	"github.com/openkvlab/boltdb"
)

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

func (d *DB) SetMaxBatchDelay(delay time.Duration) {
	d.db.MaxBatchDelay = delay
}

func (d *DB) SetMaxBatchSize(size int) {
	d.db.MaxBatchSize = size
}

func (d *DB) SetAllocSize(size int) {
	d.db.AllocSize = size
}

func (d *DB) MaxBatchDelay() time.Duration {
	return d.db.MaxBatchDelay
}

func (d *DB) MaxBatchSize() int {
	return d.db.MaxBatchSize
}

func (d *DB) AllocSize() int {
	return d.db.AllocSize
}
