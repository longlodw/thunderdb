package thunder

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
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
	columns []string,
	indexes map[string][]string,
) (*Persistent, error) {
	tnx := tx.tx
	maUn := tx.maUn
	bucket, err := tnx.CreateBucketIfNotExists([]byte(relation))
	if err != nil {
		return nil, err
	}
	metaBucket, err := bucket.CreateBucketIfNotExists([]byte("meta"))
	if err != nil {
		return nil, err
	}

	columnsBytes, err := maUn.Marshal(columns)
	if err != nil {
		return nil, err
	}
	if err := metaBucket.Put([]byte("columns"), columnsBytes); err != nil {
		return nil, err
	}

	for _, cols := range indexes {
		for _, col := range cols {
			if !slices.Contains(columns, col) {
				return nil, fmt.Errorf("index column %s not found in columns", col)
			}
		}
	}
	indexesBytes, err := maUn.Marshal(indexes)
	if err != nil {
		return nil, err
	}
	if err := metaBucket.Put([]byte("indexes"), indexesBytes); err != nil {
		return nil, err
	}
	indexesStore, err := newIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	reverseIdxStore, err := newReverseIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	dataStore, err := newData(bucket, maUn)
	if err != nil {
		return nil, err
	}

	return &Persistent{
		data:        dataStore,
		indexes:     indexesStore,
		reverseIdx:  reverseIdxStore,
		indexesMeta: indexes,
		columns:     columns,
		relation:    relation,
		maUn:        maUn,
	}, nil
}

func (tx *Tx) LoadPersistent(
	relation string,
) (*Persistent, error) {
	tnx := tx.tx
	maUn := tx.maUn
	bucket := tnx.Bucket([]byte(relation))
	if bucket == nil {
		return nil, boltdb_errors.ErrBucketNotFound
	}

	metaBucket := bucket.Bucket([]byte("meta"))
	if metaBucket == nil {
		return nil, boltdb_errors.ErrBucketNotFound
	}

	columnsBytes := metaBucket.Get([]byte("columns"))
	var columns []string
	if err := maUn.Unmarshal(columnsBytes, &columns); err != nil {
		return nil, err
	}

	indexesBytes := metaBucket.Get([]byte("indexes"))
	var indexes map[string][]string
	if err := maUn.Unmarshal(indexesBytes, &indexes); err != nil {
		return nil, err
	}

	indexesStore, err := loadIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	reverseIdxStore, err := loadReverseIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	dataStore, err := loadData(bucket, maUn)
	if err != nil {
		return nil, err
	}

	return &Persistent{
		data:        dataStore,
		indexes:     indexesStore,
		reverseIdx:  reverseIdxStore,
		indexesMeta: indexes,
		columns:     columns,
		relation:    relation,
		maUn:        maUn,
	}, nil
}

func (tx *Tx) DeletePersistent(relation string) error {
	tnx := tx.tx
	if err := tnx.DeleteBucket([]byte(relation)); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) CreateQuery(columns []string) (*Query, error) {
	return newQuery(tx, columns), nil
}
