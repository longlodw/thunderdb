package relations

import (
	"fmt"
	"iter"
	"reflect"
	"slices"

	"github.com/longlodw/thunder"
	"github.com/openkvlab/boltdb"
	"github.com/openkvlab/boltdb/errors"
)

// PersistentRelation represents an object relation in the database.
type Persistent struct {
	data        *dataStorage
	indexes     *indexStorage
	reverseIdx  *reverseIndexStorage
	indexesMeta map[string][]string
	columns     []string
	relation    string
}

func CreatePersistent(
	tnx *boltdb.Tx,
	relation string,
	maUn thunder.MarshalUnmarshaler,
	columns []string,
	indexes map[string][]string,
) (*Persistent, error) {
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
	}, nil
}

func LoadPersistent(
	tnx *boltdb.Tx,
	relation string,
	maUn thunder.MarshalUnmarshaler,
) (*Persistent, error) {
	bucket := tnx.Bucket([]byte(relation))
	if bucket == nil {
		return nil, errors.ErrBucketNotFound
	}

	metaBucket := bucket.Bucket([]byte("meta"))
	if metaBucket == nil {
		return nil, errors.ErrBucketNotFound
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
	}, nil
}

func (pr *Persistent) Insert(obj map[string]any) error {
	if len(obj) != len(pr.columns) {
		return fmt.Errorf("object has incorrect number of fields")
	}
	for _, col := range pr.columns {
		if _, ok := obj[col]; !ok {
			return fmt.Errorf("object is missing field %s", col)
		}
	}
	id, err := pr.data.insert(obj)
	if err != nil {
		return err
	}

	// Update indexes
	revIdx := make(map[string][]byte)
	for idxName, keyFields := range pr.indexesMeta {
		keyParts := make([]any, len(keyFields))
		for i, kf := range keyFields {
			keyParts[i] = obj[kf]
		}
		revIdxField, err := pr.indexes.insert(idxName, keyParts, id)
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

func (pr *Persistent) Delete(ops ...thunder.Op) error {
	iterEntries, err := pr.iter(ops...)
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
			keyFields, ok := pr.indexesMeta[idxName]
			if !ok {
				return fmt.Errorf("index metadata not found for index %s", idxName)
			}
			keyParts := make([]any, len(keyFields))
			for i, kf := range keyFields {
				keyParts[i] = e.value[kf]
			}
			if err := pr.indexes.delete(idxName, keyParts, revIdxField); err != nil {
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

func (pr *Persistent) Select(ops ...thunder.Op) (iter.Seq2[map[string]any, error], error) {
	iterEntries, err := pr.iter(ops...)
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

func (pr *Persistent) iter(ops ...thunder.Op) (iter.Seq2[entry, error], error) {
	idsSet := make(map[string]int)
	indexCount := 0
	nonIndexedOps := make([]thunder.Op, 0, len(ops))
	for _, op := range ops {
		if _, ok := pr.indexesMeta[op.Field]; !ok {
			nonIndexedOps = append(nonIndexedOps, op)
			continue
		}
		indexCount++
		values, ok := op.Value.([]any)
		if !ok {
			return nil, fmt.Errorf("operation value must be a slice")
		}
		iterIds, err := pr.indexes.get(op.Type, op.Field, values)
		if err != nil {
			return nil, err
		}
		for idBytes := range iterIds {
			idStr := string(idBytes)
			if _, exists := idsSet[idStr]; exists {
				idsSet[idStr]++
			} else {
				idsSet[idStr] = 1
			}
		}
	}
	return func(yield func(entry, error) bool) {
		for idStr, count := range idsSet {
			if count != indexCount {
				continue
			}
			idBytes := []byte(idStr)
			value, err := pr.data.get(idBytes)
			if err != nil {
				if !yield(entry{}, err) {
					return
				}
				continue
			}
			matches := true
			for _, op := range nonIndexedOps {
				fieldValue, ok := value[op.Field]
				if !ok {
					matches = false
					break
				}
				match, err := apply(fieldValue, op)
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					matches = false
					break
				}
				if !match {
					matches = false
					break
				}
			}
			if matches && !yield(entry{
				id:    idBytes,
				value: value,
			}, nil) {
				return
			}
		}
	}, nil
}

func toKey(
	maUn thunder.MarshalUnmarshaler,
	keyFields []string,
	objInt map[string]any,
) ([]byte, error) {
	keyObj := make([]any, len(keyFields))
	for i, k := range keyFields {
		if val, ok := objInt[k]; ok {
			keyObj[i] = val
		} else {
			return nil, fmt.Errorf("key field %d not found in object", k)
		}
	}
	keyBytes, err := maUn.Marshal(keyObj)
	if err != nil {
		return nil, err
	}
	return keyBytes, nil
}

func apply(value any, o thunder.Op) (bool, error) {
	if reflect.TypeOf(value) != reflect.TypeOf(o.Value) {
		return false, fmt.Errorf("type mismatch: %T vs %T", value, o.Value)
	}
	switch o.Type {
	case thunder.OpEq:
		if !reflect.ValueOf(value).Comparable() {
			return false, fmt.Errorf("value is not comparable")
		}
		return value == o.Value, nil
	case thunder.OpNe:
		if !reflect.ValueOf(value).Comparable() {
			return false, fmt.Errorf("value is not comparable")
		}
		return value != o.Value, nil
	case thunder.OpGt:
		if !isOrdered(value) {
			return false, fmt.Errorf("value is not ordered")
		}
		return reflect.ValueOf(value).Interface().(interface {
			GreaterThan(any) bool
		}).GreaterThan(o.Value), nil
	case thunder.OpLt:
		if !isOrdered(value) {
			return false, fmt.Errorf("value is not ordered")
		}
		return reflect.ValueOf(value).Interface().(interface {
			LessThan(any) bool
		}).LessThan(o.Value), nil
	case thunder.OpGe:
		if !isOrdered(value) {
			return false, fmt.Errorf("value is not ordered")
		}
		gt, err := apply(value, thunder.Gt(o.Value))
		if err != nil {
			return false, err
		}
		eq, err := apply(value, thunder.Eq(o.Value))
		if err != nil {
			return false, err
		}
		return gt || eq, nil
	case thunder.OpLe:
		if !isOrdered(value) {
			return false, fmt.Errorf("value is not ordered")
		}
		lt, err := apply(value, thunder.Lt(o.Value))
		if err != nil {
			return false, err
		}
		eq, err := apply(value, thunder.Eq(o.Value))
		if err != nil {
			return false, err
		}
		return lt || eq, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", o.Type)
	}
}

func isOrdered(a any) bool {
	switch a.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		string:
		return true
	default:
		return false
	}
}
