package thunder

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
	return errors.Join(
		tx.tx.Rollback(),
		tx.tempTx.Rollback(),
		tx.tempDb.Close(),
		os.Remove(tx.tempFilePath),
	)
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
