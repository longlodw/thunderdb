package thunder

import (
	"bytes"
	"iter"
	"slices"

	boltdb_errors "github.com/openkvlab/boltdb/errors"
)

// Persistent represents an object relation in the database.
type Persistent struct {
	data        *dataStorage
	indexes     *indexStorage
	reverseIdx  *reverseIndexStorage
	fields      map[string]ColumnSpec
	relation    string
	uniqueNames []string
	indexNames  []string
	columns     []string
}

func newPersistent(tx *Tx, relation string, columnSpecs map[string]ColumnSpec) (*Persistent, error) {
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
	dataStore, err := newData(bucket, columns, maUn)
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

func loadPersistent(tx *Tx, relation string) (*Persistent, error) {
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

	indexesStore, err := loadIndex(bucket)
	if err != nil {
		return nil, err
	}
	reverseIdxStore, err := loadReverseIndex(bucket, maUn)
	if err != nil {
		return nil, err
	}
	dataStore, err := loadData(bucket, columns, maUn)
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

func (pr *Persistent) Insert(obj map[string]any) error {
	id, err := pr.data.insert(obj)
	if err != nil {
		return err
	}
	value := make(map[string][]byte)
	for k, v := range pr.fields {
		if !(v.Indexed || v.Unique) {
			continue
		}
		refs := v.ReferenceCols
		if len(refs) > 0 {
			key, err := pr.computeKey(obj, k)
			if err != nil {
				return err
			}
			value[k] = key
		} else {
			v, ok := obj[k]
			if !ok {
				return ErrObjectMissingField(k)
			}
			vBytes, err := orderedMa.Marshal([]any{v})
			if err != nil {
				return err
			}
			value[k] = vBytes
		}
	}
	// Check uniques
	for _, uniqueName := range pr.uniqueNames {
		idxRange := &keyRange{
			includeEnd:   true,
			includeStart: true,
			startKey:     value[uniqueName],
			endKey:       value[uniqueName],
		}
		exists, err := pr.indexes.get(uniqueName, idxRange)
		if err != nil {
			return err
		}
		for range exists {
			return ErrUniqueConstraint(uniqueName)
		}
	}

	// Update indexes
	revIdx := make(map[string][]byte)
	for _, idxName := range pr.indexNames {
		revIdxField, err := pr.indexes.insert(idxName, value[idxName], id)
		if err != nil {
			return err
		}
		revIdx[idxName] = revIdxField
	}
	if err := pr.reverseIdx.insert(id, revIdx); err != nil {
		return err
	}
	return nil
}

func (pr *Persistent) Delete(ranges map[string]*keyRange) error {
	iterEntries, err := pr.iter(ranges)
	if err != nil {
		return err
	}
	for e, err := range iterEntries {
		if err != nil {
			return err
		}
		// Delete from indexes
		revIdx, err := pr.reverseIdx.get(e.id)
		if err != nil {
			return err
		}
		for idxName, revIdxField := range revIdx {
			keyBytes, err := pr.computeKey(e.value, idxName)
			if err != nil {
				return err
			}
			if err := pr.indexes.delete(idxName, keyBytes, revIdxField); err != nil {
				return err
			}
		}
		if err := pr.reverseIdx.delete(e.id); err != nil {
			return err
		}
		// Delete from data
		if err := pr.data.delete(e.id); err != nil {
			return err
		}
	}
	return nil
}

func (pr *Persistent) Select(ranges map[string]*keyRange) (iter.Seq2[map[string]any, error], error) {
	iterEntries, err := pr.iter(ranges)
	if err != nil {
		return nil, err
	}
	return func(yield func(map[string]any, error) bool) {
		iterEntries(func(e entry, err error) bool {
			if err != nil {
				return yield(nil, err)
			}
			return yield(e.value, nil)
		})
	}, nil
}

func (pr *Persistent) Name() string {
	return pr.relation
}

func (pr *Persistent) Columns() []string {
	return slices.Clone(pr.columns)
}

func (pr *Persistent) Project(mapping map[string]string) (Selector, error) {
	return newProjection(pr, mapping)
}

func (pr *Persistent) iter(ranges map[string]*keyRange) (iter.Seq2[entry, error], error) {
	selectedIndexes := make([]string, 0, len(ranges))
	for _, idxName := range pr.indexNames {
		if _, ok := ranges[idxName]; ok {
			selectedIndexes = append(selectedIndexes, idxName)
		}
	}
	if len(selectedIndexes) == 0 {
		// No indexes defined, full scan
		entries, err := pr.data.get(&keyRange{
			includeEnd:   true,
			includeStart: true,
		})
		if err != nil {
			return nil, err
		}
		return func(yield func(entry, error) bool) {
			for e, err := range entries {
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				value := make(map[string][]byte)
				for k := range ranges {
					key, err := pr.computeKey(e.value, k)
					if err != nil {
						if !yield(entry{}, err) {
							return
						}
						continue
					}
					value[k] = key
				}
				matches, err := pr.matchOps(value, ranges, "")
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				if matches && !yield(e, nil) {
					return
				}
			}
		}, nil
	}
	shortestRangeIdxName := slices.MinFunc(selectedIndexes, func(a, b string) int {
		distA := ranges[a].distance()
		distB := ranges[b].distance()
		return bytes.Compare(distA, distB)
	})
	rangeIdx := ranges[shortestRangeIdxName]
	idxes, err := pr.indexes.get(shortestRangeIdxName, rangeIdx)
	if err != nil {
		return nil, err
	}
	return func(yield func(entry, error) bool) {
		for idBytes := range idxes {
			id := idBytes
			values, err := pr.data.get(&keyRange{
				includeEnd:   true,
				includeStart: true,
				startKey:     id,
				endKey:       id,
			})
			if err != nil {
				if !yield(entry{}, err) {
					return
				}
				continue
			}
			for e, err := range values {
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				// Match other ops
				value := make(map[string][]byte)
				for k := range ranges {
					if k == shortestRangeIdxName {
						continue
					}
					key, err := pr.computeKey(e.value, k)
					if err != nil {
						if !yield(entry{}, err) {
							return
						}
						continue
					}
					value[k] = key
				}
				matches, err := pr.matchOps(value, ranges, shortestRangeIdxName)
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				if matches && !yield(e, nil) {
					return
				}
			}
		}
	}, nil
}

func (pr *Persistent) computeKey(obj map[string]any, name string) ([]byte, error) {
	keySpec, ok := pr.fields[name]
	if !ok {
		return nil, ErrFieldNotFoundInColumns(name)
	}
	keyParts := []any{}
	if len(keySpec.ReferenceCols) > 0 {
		for _, refCol := range keySpec.ReferenceCols {
			v, ok := obj[refCol]
			if !ok {
				return nil, ErrObjectMissingField(refCol)
			}
			keyParts = append(keyParts, v)
		}
	} else {
		v, ok := obj[name]
		if !ok {
			return nil, ErrObjectMissingField(name)
		}
		keyParts = append(keyParts, v)
	}
	return orderedMa.Marshal(keyParts)
}

func (pr *Persistent) matchOps(value map[string][]byte, keyRanges map[string]*keyRange, skip string) (bool, error) {
	for name, r := range keyRanges {
		if name == skip {
			continue
		}
		v, ok := value[name]
		if !ok {
			return false, nil
		}
		if !r.contains(v) {
			return false, nil
		}
	}
	return true, nil
}
