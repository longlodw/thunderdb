package thunderdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"maps"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
)

type storage struct {
	bucket   *boltdb.Bucket
	name     string
	metadata Metadata
	maUn     MarshalUnmarshaler
}

type Row struct {
	values map[int][]byte
	maUn   MarshalUnmarshaler
}

func (sr *Row) Get(idx int, v any) error {
	vBytes, ok := sr.values[idx]
	if !ok {
		return ErrFieldNotFound(fmt.Sprintf("column %d", idx))
	}
	return sr.maUn.Unmarshal(vBytes, v)
}

func (sr *Row) Iter() iter.Seq2[int, *Value] {
	return func(yield func(int, *Value) bool) {
		for k, b := range sr.values {
			if !yield(k, ValueOfRaw(b, sr.maUn)) {
				return
			}
		}
	}
}

func newStorage(
	tx *boltdb.Tx,
	name string,
	columnsCount int,
	indexes map[uint64]bool,
	maUn MarshalUnmarshaler,
) (*storage, error) {
	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}
	if _, err := bucket.CreateBucketIfNotExists([]byte("data")); err != nil {
		return nil, err
	}
	idxBucket, err := bucket.CreateBucketIfNotExists([]byte("index"))
	if err != nil {
		return nil, err
	}
	for i := range indexes {
		var idxName [8]byte
		binary.BigEndian.PutUint64(idxName[:], i)
		if _, err := idxBucket.CreateBucketIfNotExists(idxName[:]); err != nil {
			return nil, err
		}
	}

	s := &storage{
		bucket: bucket,
		name:   name,
		metadata: Metadata{
			ColumnsCount: columnsCount,
			Indexes:      indexes,
		},
		maUn: maUn,
	}
	metadataBytes, err := GobMaUn.Marshal(s.metadata)
	if err != nil {
		return nil, err
	}
	err = bucket.Put([]byte("metadata"), metadataBytes)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func loadMetadata(
	tx *boltdb.Tx,
	name string,
	metadata *Metadata,
) error {
	bucket := tx.Bucket([]byte(name))
	if bucket == nil {
		return boltdb_errors.ErrBucketNotFound
	}
	metadataBytes := bucket.Get([]byte("metadata"))
	if metadataBytes == nil {
		return ErrMetaDataNotFound(name)
	}
	if err := GobMaUn.Unmarshal(metadataBytes, metadata); err != nil {
		return err
	}
	return nil
}

func loadStorage(
	tx *boltdb.Tx,
	name string,
	maUn MarshalUnmarshaler,
) (*storage, error) {
	bucket := tx.Bucket([]byte(name))
	if bucket == nil {
		return nil, boltdb_errors.ErrBucketNotFound
	}
	s := &storage{
		bucket: bucket,
		name:   name,
		maUn:   maUn,
	}
	if err := loadMetadata(tx, name, &s.metadata); err != nil {
		return nil, err
	}
	return s, nil
}

func deleteStorage(tx *boltdb.Tx, name string) error {
	return tx.DeleteBucket([]byte(name))
}

func (s *storage) dataBucket() *boltdb.Bucket {
	return s.bucket.Bucket([]byte("data"))
}

func (s *storage) indexBucket() *boltdb.Bucket {
	return s.bucket.Bucket([]byte("index"))
}

func (s *storage) deleteIndexes(id []byte, values *map[int]any, skip map[uint64]bool) error {
	for i := range s.metadata.Indexes {
		if skip != nil && skip[i] {
			continue
		}
		selectedValues := make([]any, 0, s.metadata.ColumnsCount)
		for j := range ReferenceColumns(uint64(i)) {
			if v, ok := (*values)[j]; ok {
				selectedValues = append(selectedValues, v)
			} else {
				return ErrFieldNotFound(fmt.Sprintf("column %d", j))
			}
		}
		vKey, err := ToKey(selectedValues...)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		copy(compositeKey[len(vKey):], id)
		var idxBytes [8]byte
		binary.BigEndian.PutUint64(idxBytes[:], i)
		if err := s.indexBucket().Bucket(idxBytes[:]).Delete(compositeKey); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) toKeyFromColumn(id []byte, value *map[int]any, idx int) ([]byte, error) {
	if _, ok := (*value)[idx]; !ok {
		compositeKey := [12]byte{}
		copy(compositeKey[0:8], id)
		binary.BigEndian.PutUint32(compositeKey[8:12], uint32(idx))
		var vAny any
		if err := s.maUn.Unmarshal(s.dataBucket().Get(compositeKey[:]), &vAny); err != nil {
			return nil, err
		}
		(*value)[idx] = vAny
	}
	return ToKey((*value)[idx])
}

func (s *storage) toIndexKey(id []byte, value *map[int]any, idx uint64) ([]byte, error) {
	refColumns := ReferenceColumns(idx)
	selectedValues := make([]any, len(refColumns))
	for k, colIdx := range refColumns {
		if _, ok := (*value)[colIdx]; !ok {
			compositeKey := [12]byte{}
			copy(compositeKey[0:8], id)
			binary.BigEndian.PutUint32(compositeKey[8:12], uint32(colIdx))
			var vAny any
			if err := s.maUn.Unmarshal(s.dataBucket().Get(compositeKey[:]), &vAny); err != nil {
				return nil, err
			}
			(*value)[colIdx] = vAny
		}
		selectedValues[k] = (*value)[colIdx]
	}
	return ToKey(selectedValues...)
}

type scanResult struct {
	id               []byte
	values           map[int]any
	rawValues        map[int][]byte
	deleteIndexEntry func() error
}

func (s *storage) scan(
	forcedIndex uint64,
	forcedRange *Range,
	equals map[int]*Value,
	ranges map[int]*Range,
	excludes map[int][]*Value,
	unmarshalCols map[int]bool,
	rawCols map[int]bool,
) iter.Seq2[*scanResult, error] {
	return func(yield func(*scanResult, error) bool) {
		if forcedIndex == 0 {
			// Full Scan
			c := s.dataBucket().Cursor()
			var prev []byte
			vals := make(map[int]any)
			rawVals := make(map[int][]byte)

			checkAndYield := func() bool {
				if prev == nil {
					return true
				}
				ok, err := s.inRanges(prev, &vals, equals, ranges, excludes)
				if err != nil {
					return yield(nil, err)
				}
				if ok {
					res := &scanResult{
						id:        prev,
						values:    vals,
						rawValues: rawVals,
					}
					if !yield(res, nil) {
						return false
					}
				}
				return true
			}

			for k, v := c.First(); k != nil; k, v = c.Next() {
				id := k[0:8]
				col := int(binary.BigEndian.Uint32(k[8:12]))

				if prev != nil && !bytes.Equal(prev, id) {
					if !checkAndYield() {
						return
					}
					vals = make(map[int]any)
					rawVals = make(map[int][]byte)
				}
				prev = id

				// Logic for what to unmarshal/keep raw
				shouldUnmarshal := unmarshalCols == nil || unmarshalCols[col]
				shouldKeepRaw := rawCols != nil && rawCols[col]

				if shouldKeepRaw {
					vCopy := make([]byte, len(v))
					copy(vCopy, v)
					rawVals[col] = vCopy
				}

				if shouldUnmarshal {
					var vAny any
					if err := s.maUn.Unmarshal(v, &vAny); err != nil {
						yield(nil, err)
						return
					}
					vals[col] = vAny
				}
			}
			checkAndYield()
		} else {
			// Index Scan
			var idxBytes [8]byte
			binary.BigEndian.PutUint64(idxBytes[:], forcedIndex)
			idxBucket := s.indexBucket().Bucket(idxBytes[:])
			c := idxBucket.Cursor()
			var k []byte
			if forcedRange.start != nil {
				start, err := forcedRange.start.GetRaw()
				if err != nil {
					yield(nil, err)
					return
				}
				k, _ = c.Seek(start)
			} else {
				k, _ = c.First()
			}
			lessThan := func(k []byte) bool {
				if forcedRange.end == nil {
					return true
				}
				end, err := forcedRange.end.GetRaw()
				if err != nil {
					return false
				}
				cmp := bytes.Compare(k[:len(k)-8], end)
				return cmp < 0 || (cmp == 0 && forcedRange.includeEnd)
			}
			for ; k != nil && lessThan(k); k, _ = c.Next() {
				vals := make(map[int]any)
				rowID := k[len(k)-8:]
				ok, err := s.inRanges(rowID, &vals, equals, ranges, excludes)
				if err != nil {
					if !yield(nil, err) {
						return
					}
					continue
				} else if !ok {
					continue
				}

				res := &scanResult{
					id:     rowID,
					values: vals,
					deleteIndexEntry: func() error {
						return c.Delete()
					},
				}
				if !yield(res, nil) {
					return
				}
			}
		}
	}
}

func (s *storage) deleteData(id []byte) error {
	for i := range s.metadata.ColumnsCount {
		var columnKey [12]byte
		binary.BigEndian.PutUint64(columnKey[0:8], binary.BigEndian.Uint64(id))
		binary.BigEndian.PutUint32(columnKey[8:12], uint32(i))
		if err := s.dataBucket().Delete(columnKey[:]); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) updateData(id []byte, updates map[int]any) error {
	for colIdx, newVal := range updates {
		var columnKey [12]byte
		copy(columnKey[0:8], id)
		binary.BigEndian.PutUint32(columnKey[8:12], uint32(colIdx))
		vBytes, err := s.maUn.Marshal(newVal)
		if err != nil {
			return err
		}
		if err := s.dataBucket().Put(columnKey[:], vBytes); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) Update(
	equals map[int]*Value,
	ranges map[int]*Range,
	excludes map[int][]*Value,
	updates map[int]any,
) error {
	// compute indexes to update and columns to unmarshal
	indexesToUpdate := make(map[uint64]bool)
	colsToUnmarshal := make(map[int]bool)
	for i := range s.metadata.Indexes {
		for volIdx := range updates {
			if i&(1<<uint64(volIdx)) != 0 {
				indexesToUpdate[i] = true
			}
		}
		refCols := ReferenceColumns(i)
		for _, colIdx := range refCols {
			colsToUnmarshal[colIdx] = true
		}
	}
	indexesToSkip := make(map[uint64]bool)
	for i := range s.metadata.Indexes {
		if !indexesToUpdate[i] {
			indexesToSkip[i] = true
		}
	}
	shortestIndex, shortestRange, err := s.metadata.bestIndex(equals, ranges)
	if err != nil {
		return err
	}
	updateMainIndex := indexesToUpdate[shortestIndex]

	for res, err := range s.scan(shortestIndex, shortestRange, equals, ranges, excludes, colsToUnmarshal, nil) {
		if err != nil {
			return err
		}

		// Ensure we have all columns needed for index updates
		for colIdx := range colsToUnmarshal {
			if _, ok := res.values[colIdx]; !ok {
				_, err := s.toKeyFromColumn(res.id, &res.values, colIdx)
				if err != nil {
					return err
				}
			}
		}

		// delete old indexes
		if err := s.deleteIndexes(res.id, &res.values, indexesToSkip); err != nil {
			return err
		}
		// If iterating by index and that index is being updated, we must delete the current index entry
		if updateMainIndex && res.deleteIndexEntry != nil {
			if err := res.deleteIndexEntry(); err != nil {
				return err
			}
			indexesToSkip[shortestIndex] = true
		}
		// apply updates
		maps.Copy(res.values, updates)
		// insert new indexes
		if err := s.insertIndexes(res.id, &res.values, indexesToSkip); err != nil {
			return err
		}
		// update data
		if err := s.updateData(res.id, updates); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) insertIndexes(id []byte, values *map[int]any, skip map[uint64]bool) error {
	for i, isUnique := range s.metadata.Indexes {
		if skip != nil && skip[i] {
			continue
		}
		selectedValues := make([]any, 0, s.metadata.ColumnsCount)
		for _, j := range ReferenceColumns(i) {
			if v, ok := (*values)[j]; ok {
				selectedValues = append(selectedValues, v)
			} else {
				return ErrFieldNotFound(fmt.Sprintf("column %d", j))
			}
		}
		vKey, err := ToKey(selectedValues...)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		var idxName [8]byte
		binary.BigEndian.PutUint64(idxName[:], uint64(i))
		curIdxBucket := s.indexBucket().Bucket(idxName[:])
		if curIdxBucket == nil {
			return ErrIndexNotFound(s.name, i)
		}
		if isUnique {
			k, _ := curIdxBucket.Cursor().Seek(vKey)
			if k != nil && bytes.Equal(k[:len(vKey)], vKey) {
				// check if it's the same row
				existingID := binary.BigEndian.Uint64(k[len(vKey):])
				if existingID == binary.BigEndian.Uint64(id) {
					// same row, skip
					continue
				}
				return ErrUniqueConstraint(s.name, i, vKey)
			}
		}
		// insert index
		binary.BigEndian.PutUint64(compositeKey[len(vKey):], binary.BigEndian.Uint64(id))
		err = curIdxBucket.Put(compositeKey, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) Delete(equals map[int]*Value, ranges map[int]*Range, excludes map[int][]*Value) error {
	shortestIndex, shortestRange, err := s.metadata.bestIndex(equals, ranges)
	if err != nil {
		return err
	}

	for res, err := range s.scan(shortestIndex, shortestRange, equals, ranges, excludes, nil, nil) {
		if err != nil {
			return err
		}
		// delete indexes
		skip := map[uint64]bool{}
		if res.deleteIndexEntry != nil {
			skip[shortestIndex] = true
			if err := res.deleteIndexEntry(); err != nil {
				return err
			}
		}
		if err := s.deleteIndexes(res.id, &res.values, skip); err != nil {
			return err
		}
		// delete data
		if err := s.deleteData(res.id); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) find(
	mainIndex uint64,
	indexRange *Range,
	equals map[int]*Value,
	ranges map[int]*Range,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	return func(yield func(*Row, error) bool) {
		for res, err := range s.scan(mainIndex, indexRange, equals, ranges, exclusion, nil, cols) {
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if res.rawValues == nil {
				res.rawValues = make(map[int][]byte)
			}
			// Ensure we have raw values for requested cols (if not already there)
			for col := range cols {
				if _, ok := res.rawValues[col]; !ok {
					var columnKey [12]byte
					copy(columnKey[0:8], res.id)
					binary.BigEndian.PutUint32(columnKey[8:12], uint32(col))
					v := s.dataBucket().Get(columnKey[:])
					if v != nil {
						// BoltDB returns valid slice until next call. Copy it.
						vCopy := make([]byte, len(v))
						copy(vCopy, v)
						res.rawValues[col] = vCopy
					}
				}
			}
			row := &Row{
				values: res.rawValues,
				maUn:   s.maUn,
			}
			if !yield(row, nil) {
				return
			}
		}
	}, nil
}

func (s *storage) inRanges(id []byte, vals *map[int]any, equals map[int]*Value, ranges map[int]*Range, exclusions map[int][]*Value) (bool, error) {
	for idx, val := range equals {
		kBytes, err := s.toKeyFromColumn(id, vals, idx)
		if err != nil {
			return false, err
		}
		eqBytes, err := val.GetRaw()
		if err != nil {
			return false, err
		}
		if !bytes.Equal(kBytes, eqBytes) {
			return false, nil
		}
	}
	for idx, kr := range ranges {
		kBytes, err := s.toKeyFromColumn(id, vals, idx)
		if err != nil {
			return false, err
		}
		kVals := ValueOfRaw(kBytes, s.maUn)
		if con, err := kr.Contains(kVals); err != nil {
			return false, err
		} else if !con {
			return false, nil
		}
	}
	for idx, exList := range exclusions {
		kBytes, err := s.toKeyFromColumn(id, vals, idx)
		if err != nil {
			return false, err
		}
		for _, ex := range exList {
			rawEx, err := ex.GetRaw()
			if err != nil {
				return false, err
			}
			if bytes.Equal(kBytes, rawEx) {
				return false, nil
			}
		}
	}
	return true, nil
}

func (s *storage) Insert(values map[int]any) error {
	id, err := s.dataBucket().NextSequence()
	if err != nil {
		return err
	}
	// insert data
	for i, v := range values {
		var columnKey [12]byte
		binary.BigEndian.PutUint64(columnKey[0:8], id)
		binary.BigEndian.PutUint32(columnKey[8:12], uint32(i))
		vBytes, err := s.maUn.Marshal(v)
		if err != nil {
			return err
		}
		err = s.dataBucket().Put(columnKey[:], vBytes)
		if err != nil {
			return err
		}
	}
	// check unique and insert indexes
	for i, isUnique := range s.metadata.Indexes {
		vKey, err := s.toIndexKey(nil, &values, uint64(i))
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		var idxName [8]byte
		binary.BigEndian.PutUint64(idxName[:], i)
		curIdxBucket := s.indexBucket().Bucket(idxName[:])
		if curIdxBucket == nil {
			return ErrIndexNotFound(s.name, i)
		}
		if isUnique {
			k, _ := curIdxBucket.Cursor().Seek(vKey)
			if k != nil && bytes.Equal(k[:len(vKey)], vKey) {
				return ErrUniqueConstraint(s.name, i, vKey)
			}
		}
		// insert index
		binary.BigEndian.PutUint64(compositeKey[len(vKey):], id)
		err = curIdxBucket.Put(compositeKey, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func ReferenceColumns(idx uint64) []int {
	refs := []int{}
	for i := range 64 {
		if (idx & (1 << uint64(i))) != 0 {
			refs = append(refs, i)
		}
	}
	return refs
}
