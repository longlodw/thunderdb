package thunder

import (
	"bytes"
	"iter"
	"slices"
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

func (pr *Persistent) Insert(obj map[string]any) error {
	value := make(map[string][]byte)
	for k, v := range pr.fields {
		refs := v.ReferenceCols
		if len(refs) > 0 {
			refValues := make([]any, len(refs))
			for i, refCol := range refs {
				refV, ok := obj[refCol]
				if !ok {
					return ErrObjectMissingField(refCol)
				}
				refValues[i] = refV
			}
			refBytes, err := orderedMa.Marshal(refValues)
			if err != nil {
				return err
			}
			value[k] = refBytes
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
	id, err := pr.data.insert(value)
	if err != nil {
		return err
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
			if err := pr.indexes.delete(idxName, e.value[idxName], revIdxField); err != nil {
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
			result := make(map[string]any)
			for _, k := range pr.columns {
				var v []any
				if err := orderedMa.Unmarshal(e.value[k], &v); err != nil {
					return yield(nil, err)
				}
				if len(v) != 1 {
					return yield(nil, ErrInvalidDataFormat)
				}
				result[k] = v[0]
			}
			return yield(result, nil)
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
	ranges, err := toRanges(ops...)
	if err != nil {
		return nil, err
	}
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
				matches, err := pr.matchOps(e.value, ranges, "")
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
				matches, err := pr.matchOps(e.value, ranges, shortestRangeIdxName)
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
