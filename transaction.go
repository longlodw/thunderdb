package thunderdb

import (
	"errors"
	"os"

	"github.com/openkvlab/boltdb"
)

type Tx struct {
	tx           *boltdb.Tx
	tempTx       *boltdb.Tx
	tempDb       *boltdb.DB
	tempFilePath string
	maUn         MarshalUnmarshaler
}

func (tx *Tx) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	errs := []error{tx.tx.Rollback()}
	if tx.tempTx != nil {
		errs = append(errs, tx.tempTx.Rollback())
	}
	if tx.tempDb != nil {
		errs = append(errs, tx.tempDb.Close())
	}
	if tx.tempFilePath != "" {
		errs = append(errs, os.Remove(tx.tempFilePath))
	}
	return errors.Join(errs...)
}

func (tx *Tx) ensureTempTx() (*boltdb.Tx, error) {
	if tx.tempTx != nil {
		return tx.tempTx, nil
	}
	tempFile, err := os.CreateTemp("", "thunder_tempdb_*.db")
	if err != nil {
		return nil, err
	}
	tempFilePath := tempFile.Name()
	tempFile.Close()

	tempDb, err := boltdb.Open(tempFilePath, 0600, nil)
	if err != nil {
		os.Remove(tempFilePath)
		return nil, err
	}
	tempTx, err := tempDb.Begin(true)
	if err != nil {
		tempDb.Close()
		os.Remove(tempFilePath)
		return nil, err
	}
	tx.tempTx = tempTx
	tx.tempDb = tempDb
	tx.tempFilePath = tempFilePath
	return tempTx, nil
}

func (tx *Tx) ID() int {
	return tx.tx.ID()
}

func (tx *Tx) CreatePersistent(
	relation string,
	columnSpecs map[string]ColumnSpec,
) (*Persistent, error) {
	return newPersistent(tx, relation, columnSpecs, false)
}

func (tx *Tx) LoadPersistent(
	relation string,
) (*Persistent, error) {
	return loadPersistent(tx, relation)
}

func (tx *Tx) DeletePersistent(relation string) error {
	tnx := tx.tx
	if err := tnx.DeleteBucket([]byte(relation)); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) CreateRecursion(relation string, colColumnSpec map[string]ColumnSpec) (*Recursion, error) {
	return newRecursive(tx, relation, colColumnSpec)
}
