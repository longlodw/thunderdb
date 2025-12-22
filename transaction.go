package thunder

import (
	"errors"
	"maps"
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
	columnSpecs map[string]ColumnSpec,
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
	columns := make([]string, 0, len(columnSpecs))
	indexNames := make([]string, 0, len(columnSpecs))
	uniquesNames := make([]string, 0, len(columnSpecs))

	columnsBytes, err := maUn.Marshal(columnSpecs)
	if err != nil {
		return nil, err
	}
	if err := metaBucket.Put([]byte("columnSpecs"), columnsBytes); err != nil {
		return nil, err
	}
	for colName, colSpec := range columnSpecs {
		refCols := colSpec.ReferenceCols
		if len(refCols) == 0 {
			columns = append(columns, colName)
		}
	}
	for colName, colSpec := range columnSpecs {
		refCols := colSpec.ReferenceCols
		if colSpec.Indexed {
			indexNames = append(indexNames, colName)
		}
		if colSpec.Unique {
			uniquesNames = append(uniquesNames, colName)
			indexNames = append(indexNames, colName)
		}
		for _, refCol := range refCols {
			if !slices.Contains(columns, refCol) {
				return nil, ErrFieldNotFoundInColumns(refCol)
			}
		}
	}
	indexesStore, err := newIndex(bucket, indexNames)
	if err != nil {
		return nil, err
	}
	reverseIdxStore, err := newReverseIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	fields := slices.Collect(maps.Keys(columnSpecs))
	dataStore, err := newData(bucket, fields)
	if err != nil {
		return nil, err
	}

	return &Persistent{
		data:        dataStore,
		indexes:     indexesStore,
		reverseIdx:  reverseIdxStore,
		fields:      columnSpecs,
		relation:    relation,
		uniqueNames: uniquesNames,
		indexNames:  indexNames,
		columns:     columns,
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
	columnSpecsBytes := metaBucket.Get([]byte("columnSpecs"))
	if columnSpecsBytes == nil {
		return nil, ErrMetaDataNotFound
	}
	var columnSpecs map[string]ColumnSpec
	if err := maUn.Unmarshal(columnSpecsBytes, &columnSpecs); err != nil {
		return nil, err
	}
	columns := make([]string, 0, len(columnSpecs))
	indexNames := make([]string, 0, len(columnSpecs))
	uniquesNames := make([]string, 0, len(columnSpecs))
	for colName, colSpec := range columnSpecs {
		refCols := colSpec.ReferenceCols
		if len(refCols) == 0 {
			columns = append(columns, colName)
		}
	}
	for colName, colSpec := range columnSpecs {
		refCols := colSpec.ReferenceCols
		if colSpec.Indexed {
			indexNames = append(indexNames, colName)
		}
		if colSpec.Unique {
			uniquesNames = append(uniquesNames, colName)
			indexNames = append(indexNames, colName)
		}
		for _, refCol := range refCols {
			if !slices.Contains(columns, refCol) {
				return nil, ErrFieldNotFoundInColumns(refCol)
			}
		}
	}
	fields := slices.Collect(maps.Keys(columnSpecs))

	indexesStore, err := loadIndex(bucket)
	if err != nil {
		return nil, err
	}
	reverseIdxStore, err := loadReverseIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	dataStore, err := loadData(bucket, fields)
	if err != nil {
		return nil, err
	}

	return &Persistent{
		data:        dataStore,
		indexes:     indexesStore,
		reverseIdx:  reverseIdxStore,
		fields:      columnSpecs,
		relation:    relation,
		uniqueNames: uniquesNames,
		indexNames:  indexNames,
		columns:     columns,
	}, nil
}

func (tx *Tx) DeletePersistent(relation string) error {
	tnx := tx.tx
	if err := tnx.DeleteBucket([]byte(relation)); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) CreateQuery(name string, columns []string, recursive bool) (*Query, error) {
	return newQuery(tx, name, columns, recursive)
}
