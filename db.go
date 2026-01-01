package thunderdb

import (
	"os"

	"github.com/openkvlab/boltdb"
)

type DB struct {
	db   *boltdb.DB
	maUn MarshalUnmarshaler
}

func OpenDB(maUn MarshalUnmarshaler, path string, mode os.FileMode, options *boltdb.Options) (*DB, error) {
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
