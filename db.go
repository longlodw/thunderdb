package thunder

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
	tempFile, err := os.CreateTemp("", "thunder_tempdb_*.db")
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()

	tempDb, err := boltdb.Open(tempFilePath, 0600, nil)
	if err != nil {
		tx.Rollback()
		os.Remove(tempFilePath)
		return nil, err
	}
	tempTx, err := tempDb.Begin(true)
	if err != nil {
		tx.Rollback()
		tempDb.Close()
		os.Remove(tempFilePath)
		return nil, err
	}

	return &Tx{
		tx:           tx,
		tempTx:       tempTx,
		tempDb:       tempDb,
		tempFilePath: tempFilePath,
		maUn:         d.maUn,
	}, nil
}
