package thunderdb

import (
	"bytes"
	"iter"
	"maps"
	"slices"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
)

// Persistent represents an object relation in the database.
type Persistent struct {
	data        *dataStorage
	indexes     *indexStorage
	fields      map[string]ColumnSpec
	relation    string
	uniqueNames []string
	indexNames  []string
	columns     []string
	parentsList []*queryParent
}

func newPersistent(tx *Tx, relation string, columnSpecs map[string]ColumnSpec, emepheral bool) (*Persistent, error) {
	tnx := tx.tx
	if emepheral {
		tempTx, err := tx.ensureTempTx()
		if err != nil {
			return nil, err
		}
		tnx = tempTx
	}
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
				return nil, ErrFieldNotFound(refCol)
			}
		}
	}
	indexesStore, err := newIndex(bucket, indexNames, maUn)
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
		fields:      columnSpecs,
		relation:    relation,
		uniqueNames: uniquesNames,
		indexNames:  indexNames,
		columns:     columns,
	}, nil
}

func (pr *Persistent) Fields() map[string]ColumnSpec {
	return pr.fields
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
		return nil, ErrMetaDataNotFound(relation)
	}
	columnSpecsBytes := metaBucket.Get([]byte("columnSpecs"))
	if columnSpecsBytes == nil {
		return nil, ErrCorruptedMetaDataEntry(relation, "columnSpecs")
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
				return nil, ErrFieldNotFound(refCol)
			}
		}
	}

	indexesStore, err := loadIndex(bucket, maUn)
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
		fields:      columnSpecs,
		relation:    relation,
		uniqueNames: uniquesNames,
		indexNames:  indexNames,
		columns:     columns,
	}, nil
}

func (pr *Persistent) IsRecursive() bool {
	return false
}

func (pr *Persistent) addParent(parent *queryParent) {
	pr.parentsList = append(pr.parentsList, parent)
}

func (pr *Persistent) parents() []*queryParent {
	return pr.parentsList
}

func (pr *Persistent) Join(bodies ...Selector) Selector {
	linkedBodies := make([]linkedSelector, 0, len(bodies)+1)
	linkedBodies = append(linkedBodies, pr)
	for _, body := range bodies {
		linkedBodies = append(linkedBodies, body.(linkedSelector))
	}
	return newJoining(linkedBodies)
}

func (pr *Persistent) Insert(obj map[string]any) error {
	if len(obj) != len(pr.columns) {
		return ErrFieldCountMismatch(len(pr.columns), len(obj))
	}

	// Check uniques before inserting data
	if err := pr.assertUnique(&persistentRow{
		value:  obj,
		fields: pr.columns,
	}); err != nil {
		return err
	}

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
			key, err := pr.computeKey(&persistentRow{
				value: obj,
			}, k)
			if err != nil {
				return err
			}
			value[k] = key
		} else {
			v, ok := obj[k]
			if !ok {
				return ErrFieldNotFound(k)
			}
			vBytes, err := ToKey(v)
			if err != nil {
				return err
			}
			value[k] = vBytes
		}
	}

	for _, idxName := range pr.indexNames {
		err := pr.indexes.insert(idxName, value[idxName], id[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (pr *Persistent) Delete(ranges map[string]*BytesRange) error {
	iterEntries, err := pr.iter(ranges)
	if err != nil {
		return err
	}
	for e, err := range iterEntries {
		if err != nil {
			return err
		}
		// Delete from indexes
		for _, idxName := range pr.indexNames {
			key, err := pr.computeKey(e, idxName)
			if err != nil {
				return err
			}
			if err := pr.indexes.delete(idxName, key, e.id[:]); err != nil {
				return err
			}
		}
		// Delete from data
		if err := pr.data.delete(e.id[:]); err != nil {
			return err
		}
	}
	return nil
}

func (pr *Persistent) Update(ranges map[string]*BytesRange, updates map[string]any) error {
	iterEntries, err := pr.iter(ranges)
	if err != nil {
		return err
	}
	for e, err := range iterEntries {
		if err != nil {
			return err
		}
		// Prepare old index keys
		oldIndexKeys := make(map[string][]byte)
		for _, idxName := range pr.indexNames {
			key, err := pr.computeKey(e, idxName)
			if err != nil {
				return err
			}
			oldIndexKeys[idxName] = key
		}
		// Apply updates
		maps.Copy(e.value, updates)
		// Check uniques
		if err := pr.assertUnique(e); err != nil {
			return err
		}
		// Prepare new index keys
		newIndexKeys := make(map[string][]byte)
		for _, idxName := range pr.indexNames {
			key, err := pr.computeKey(e, idxName)
			if err != nil {
				return err
			}
			newIndexKeys[idxName] = key
		}
		// Update indexes
		for _, idxName := range pr.indexNames {
			oldKey := oldIndexKeys[idxName]
			newKey := newIndexKeys[idxName]
			if !bytes.Equal(oldKey, newKey) {
				// Delete old index entry
				if err := pr.indexes.delete(idxName, oldKey, e.id[:]); err != nil {
					return err
				}
				// Insert new index entry
				if err := pr.indexes.insert(idxName, newKey, e.id[:]); err != nil {
					return err
				}
			}
		}
		// Update data storage
		if err := pr.data.update(e.id[:], updates); err != nil {
			return err
		}
	}
	return nil
}

func (pr *Persistent) assertUnique(r *persistentRow) error {
	for _, uniqueName := range pr.uniqueNames {
		key, err := pr.computeKey(r, uniqueName)
		if err != nil {
			return err
		}
		idxRange := &BytesRange{
			includeEnd:   true,
			includeStart: true,
			start:        key,
			end:          key,
		}
		exists, err := pr.indexes.get(uniqueName, idxRange)
		if err != nil {
			return err
		}
		for id, err := range exists {
			if err != nil {
				return err
			}
			if id == r.id {
				continue
			}
			return ErrUniqueConstraint(uniqueName, key)
		}
	}
	return nil
}

func (pr *Persistent) Select(ranges map[string]*BytesRange) (iter.Seq2[Row, error], error) {
	return pr.selectEval(ranges, false)
}

func (pr *Persistent) selectEval(ranges map[string]*BytesRange, _ bool) (iter.Seq2[Row, error], error) {
	iterEntries, err := pr.iter(ranges)
	if err != nil {
		return nil, err
	}
	return func(yield func(Row, error) bool) {
		iterEntries(func(item *persistentRow, err error) bool {
			return yield(item, err)
		})
	}, nil
}

func (pr *Persistent) Name() string {
	return pr.relation
}

func (pr *Persistent) Columns() []string {
	return pr.columns
}

func (pr *Persistent) Project(mapping map[string]string) Selector {
	return newProjection(pr, mapping)
}

func (pr *Persistent) iter(ranges map[string]*BytesRange) (iter.Seq2[*persistentRow, error], error) {
	selectedIndexes := make([]string, 0, len(ranges))
	for _, idxName := range pr.indexNames {
		if _, ok := ranges[idxName]; ok {
			selectedIndexes = append(selectedIndexes, idxName)
		}
	}
	if len(selectedIndexes) == 0 {
		// No indexes defined, full scan
		entries, err := pr.data.get(&BytesRange{
			includeEnd:   true,
			includeStart: true,
		})
		if err != nil {
			return nil, err
		}
		return func(yield func(*persistentRow, error) bool) {
			for e, err := range entries {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				matches, err := pr.matchBytesRanges(e, ranges, "")
				if err != nil {
					if !yield(nil, err) {
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
		distA := ranges[a].distance
		distB := ranges[b].distance
		return bytes.Compare(distA, distB)
	})
	rangeIdx := ranges[shortestRangeIdxName]
	idxes, err := pr.indexes.get(shortestRangeIdxName, rangeIdx)
	if err != nil {
		return nil, err
	}
	return func(yield func(*persistentRow, error) bool) {
		for id := range idxes {
			values, err := pr.data.get(&BytesRange{
				includeEnd:   true,
				includeStart: true,
				start:        id[:],
				end:          id[:],
			})
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			for e, err := range values {
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				}
				// Match other ops
				matches, err := pr.matchBytesRanges(e, ranges, shortestRangeIdxName)
				if err != nil {
					if !yield(nil, err) {
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

func (pr *Persistent) computeKey(obj Row, name string) ([]byte, error) {
	keySpec, ok := pr.fields[name]
	if !ok {
		return nil, ErrFieldNotFound(name)
	}
	var keyParts []any
	if len(keySpec.ReferenceCols) > 0 {
		keyParts = make([]any, 0, len(keySpec.ReferenceCols))
		for _, refCol := range keySpec.ReferenceCols {
			v, err := obj.Get(refCol)
			if err != nil {
				return nil, err
			}
			keyParts = append(keyParts, v)
		}
	} else {
		v, err := obj.Get(name)
		if err != nil {
			return nil, err
		}
		keyParts = []any{v}
	}
	return ToKey(keyParts...)
}

func (pr *Persistent) matchBytesRanges(value Row, keyRanges map[string]*BytesRange, skip string) (bool, error) {
	for name, r := range keyRanges {
		if name == skip {
			continue
		}
		vBytes, err := pr.computeKey(value, name)
		if err != nil {
			return false, err
		}
		if !r.Contains(vBytes) {
			return false, nil
		}
	}
	return true, nil
}

type persistentRow struct {
	bucket *boltdb.Bucket
	id     [8]byte
	value  map[string]any
	maUn   MarshalUnmarshaler
	fields []string
}

func newPersistentRow(bucket *boltdb.Bucket, maUn MarshalUnmarshaler, fields []string, id [8]byte) *persistentRow {
	return &persistentRow{
		bucket: bucket,
		id:     id,
		value:  make(map[string]any),
		maUn:   maUn,
		fields: fields,
	}
}

func (r *persistentRow) Get(field string) (any, error) {
	if val, ok := r.value[field]; ok {
		return val, nil
	} else {
		valuesBucket := r.bucket.Bucket([]byte("values"))
		if valuesBucket == nil {
			return nil, boltdb_errors.ErrBucketNotFound
		}
		bckField := valuesBucket.Bucket([]byte(field))
		if bckField == nil {
			return nil, ErrFieldNotFound(field)
		}
		v := bckField.Get(r.id[:])
		if v == nil {
			return nil, ErrFieldNotFound(field)
		}
		var val any
		err := r.maUn.Unmarshal(v, &val)
		if err != nil {
			return nil, err
		}
		r.value[field] = val
		return val, nil
	}
}

func (r *persistentRow) ToMap() (map[string]any, error) {
	result := make(map[string]any)
	for _, field := range r.fields {
		val, err := r.Get(field)
		if err != nil {
			return nil, err
		}
		result[field] = val
	}
	return result, nil
}
