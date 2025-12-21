package thunder

import (
	"bytes"
	"cmp"
	"iter"
	"reflect"
	"slices"
)

// Persistent represents an object relation in the database.
type Persistent struct {
	data        *dataStorage
	indexes     *indexStorage
	reverseIdx  *reverseIndexStorage
	indexesMeta map[string][]string
	uniquesMeta map[string][]string
	columns     []string
	relation    string
}

func (pr *Persistent) Insert(obj map[string]any) error {
	if len(obj) != len(pr.columns) {
		return ErrObjectFieldCountMismatch
	}
	for _, col := range pr.columns {
		if _, ok := obj[col]; !ok {
			return ErrObjectMissingField(col)
		}
	}
	id, err := pr.data.insert(obj)
	if err != nil {
		return err
	}
	// Check uniques
	for uniqueName, keyFields := range pr.uniquesMeta {
		keyParts := make([]any, len(keyFields))
		for i, kf := range keyFields {
			keyParts[i] = obj[kf]
		}
		exists, err := pr.indexes.get(OpEq, uniqueName, keyParts)
		if err != nil {
			return err
		}
		for range exists {
			return ErrUniqueConstraint(uniqueName)
		}
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
	for idxName, keyFields := range pr.uniquesMeta {
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

func (pr *Persistent) Delete(ops ...Op) error {
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
				return ErrIndexMetadataNotFound(idxName)
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

func (pr *Persistent) Select(ops ...Op) (iter.Seq2[map[string]any, error], error) {
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

func (pr *Persistent) Name() string {
	return pr.relation
}

func (pr *Persistent) Columns() []string {
	return slices.Clone(pr.columns)
}

func (pr *Persistent) Project(mapping map[string]string) (Selector, error) {
	return newProjection(pr, mapping)
}

func (pr *Persistent) iter(ops ...Op) (iter.Seq2[entry, error], error) {
	idsSet := make(map[string]int)
	indexCount := 0
	nonIndexedOps := make([]Op, 0, len(ops))
	for _, op := range ops {
		_, okUnique := pr.uniquesMeta[op.Field]
		_, okIndex := pr.indexesMeta[op.Field]
		if !(okUnique || okIndex) {
			nonIndexedOps = append(nonIndexedOps, op)
			if !slices.Contains(pr.columns, op.Field) {
				return nil, ErrFieldNotFoundInColumns(op.Field)
			}
			continue
		}
		indexCount++
		values, ok := op.Value.([]any)
		if !ok {
			values = []any{op.Value}
		}
		if okUnique && !(len(pr.uniquesMeta[op.Field]) == len(values)) {
			return nil, ErrUniqueIndexValueCount(op.Field, len(pr.uniquesMeta[op.Field]), len(values))
		}
		if okIndex && !(len(pr.indexesMeta[op.Field]) == len(values)) {
			return nil, ErrIndexValueCount(op.Field, len(pr.indexesMeta[op.Field]), len(values))
		}
		iterIds, err := pr.indexes.get(op.Type, op.Field, values)
		if err != nil {
			return nil, err
		}
		for idBytes := range iterIds {
			idStr := string(idBytes)
			idsSet[idStr]++
		}
	}
	return func(yield func(entry, error) bool) {
		if indexCount == 0 {
			c := pr.data.bucket.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var value map[string]any
				if err := pr.data.maUn.Unmarshal(v, &value); err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				matches, err := pr.matchOps(value, nonIndexedOps)
				if err != nil {
					if !yield(entry{}, err) {
						return
					}
					continue
				}
				if matches && !yield(entry{
					id:    k,
					value: value,
				}, nil) {
					return
				}
			}
			return
		}

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
			matches, err := pr.matchOps(value, nonIndexedOps)
			if err != nil {
				if !yield(entry{}, err) {
					return
				}
				continue
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

func (pr *Persistent) matchOps(value map[string]any, ops []Op) (bool, error) {
	for _, op := range ops {
		fieldValue, ok := value[op.Field]
		if !ok {
			return false, ErrFieldNotFoundInObject(op.Field)
		}
		match, err := apply(fieldValue, op)
		if err != nil {
			return false, err
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func apply(value any, o Op) (bool, error) {
	if reflect.TypeOf(value) != reflect.TypeOf(o.Value) {
		return false, ErrTypeMismatch(value, o.Value)
	}
	v, err := compare(value, o.Value)
	if err != nil {
		return false, err
	}
	switch o.Type {
	case OpEq:
		return v == 0, nil
	case OpNe:
		return v != 0, nil
	case OpGt:
		return v > 0, nil
	case OpLt:
		return v < 0, nil
	case OpGe:
		return v >= 0, nil
	case OpLe:
		return v <= 0, nil
	default:
		return false, ErrUnsupportedOperator(o.Type)
	}
}

func compare(a, b any) (int, error) {
	switch va := a.(type) {
	case int:
		return cmp.Compare(va, b.(int)), nil
	case int8:
		return cmp.Compare(va, b.(int8)), nil
	case int16:
		return cmp.Compare(va, b.(int16)), nil
	case int32:
		return cmp.Compare(va, b.(int32)), nil
	case int64:
		return cmp.Compare(va, b.(int64)), nil
	case uint:
		return cmp.Compare(va, b.(uint)), nil
	case uint8:
		return cmp.Compare(va, b.(uint8)), nil
	case uint16:
		return cmp.Compare(va, b.(uint16)), nil
	case uint32:
		return cmp.Compare(va, b.(uint32)), nil
	case uint64:
		return cmp.Compare(va, b.(uint64)), nil
	case uintptr:
		return cmp.Compare(va, b.(uintptr)), nil
	case float32:
		return cmp.Compare(va, b.(float32)), nil
	case float64:
		return cmp.Compare(va, b.(float64)), nil
	case string:
		return cmp.Compare(va, b.(string)), nil
	case []any:
		ba, err := orderedMa.Marshal(a)
		if err != nil {
			return 0, err
		}
		bb, err := orderedMa.Marshal(b)
		if err != nil {
			return 0, err
		}
		return bytes.Compare(ba, bb), nil
	default:
		return 0, ErrUnsupportedType(a)
	}
}
