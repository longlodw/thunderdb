package thunderdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"slices"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
)

type ComputedColumnSpec struct {
	FieldRefs []int
	IsUnique  bool
}

type ColumnSpec struct {
	IsUnique  bool
	IsIndexed bool
}

type storageMetadata struct {
	ColumnSpecs         []ColumnSpec
	ComputedColumnSpecs []ComputedColumnSpec
}

func (sm *storageMetadata) bestIndex(ranges map[int]*BytesRange) int {
	selectedIndexes := make([]int, 0, len(ranges))
	for i := range ranges {
		if (i < len(sm.ColumnSpecs) && sm.ColumnSpecs[i].IsIndexed || sm.ColumnSpecs[i].IsUnique) || (i >= len(sm.ColumnSpecs) && sm.ComputedColumnSpecs[i-len(sm.ColumnSpecs)].IsUnique) {
			selectedIndexes = append(selectedIndexes, i)
		}
	}
	if len(selectedIndexes) == 0 {
		return -1
	}
	shortestIndex := slices.MinFunc(selectedIndexes, func(a, b int) int {
		return bytes.Compare(ranges[a].distance, ranges[b].distance)
	})
	return shortestIndex
}

type storage struct {
	bucket   *boltdb.Bucket
	metadata storageMetadata
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

func (sr *Row) Iter() iter.Seq2[int, Value] {
	return func(yield func(int, Value) bool) {
		for k, b := range sr.values {
			var v any
			err := sr.maUn.Unmarshal(b, &v)
			if !yield(k, Value{value: v, err: err}) {
				return
			}
		}
	}
}

type Value struct {
	value any
	err   error
}

func (v *Value) Get() (any, error) {
	return v.value, v.err
}

func newStorage(
	tx *boltdb.Tx,
	name string,
	collumnSpecs []ColumnSpec,
	computedColumnSpecs []ComputedColumnSpec,
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
	for i, spec := range collumnSpecs {
		if !(spec.IsUnique || spec.IsIndexed) {
			continue
		}
		var idxName [4]byte
		binary.BigEndian.PutUint32(idxName[0:4], uint32(i))
		if _, err := idxBucket.CreateBucketIfNotExists(idxName[:]); err != nil {
			return nil, err
		}
	}
	for i := range computedColumnSpecs {
		var idxName [4]byte
		binary.BigEndian.PutUint32(idxName[0:4], uint32(i+len(collumnSpecs)))
		if _, err := idxBucket.CreateBucketIfNotExists(idxName[:]); err != nil {
			return nil, err
		}
	}

	s := &storage{
		bucket: bucket,
		metadata: storageMetadata{
			ColumnSpecs:         collumnSpecs,
			ComputedColumnSpecs: computedColumnSpecs,
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
	metadata *storageMetadata,
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
		maUn:   maUn,
	}
	if err := loadMetadata(tx, name, &s.metadata); err != nil {
		return nil, err
	}
	return s, nil
}

func deleteStorage(rootBucket *boltdb.Bucket, name string) error {
	return rootBucket.DeleteBucket([]byte(name))
}

func (s *storage) ComputedColumnsCount() int {
	return len(s.metadata.ComputedColumnSpecs)
}

func (s *storage) ColumnsCount() int {
	return len(s.metadata.ColumnSpecs)
}

func (s *storage) dataBucket() *boltdb.Bucket {
	return s.bucket.Bucket([]byte("data"))
}

func (s *storage) indexBucket() *boltdb.Bucket {
	return s.bucket.Bucket([]byte("index"))
}

func (s *storage) deleteIndexes(id []byte, values *map[int]any, skip int) error {
	for i, spec := range s.metadata.ColumnSpecs {
		if !(spec.IsIndexed || spec.IsUnique) || i == skip {
			continue
		}
		var v any
		if val, ok := (*values)[i]; ok {
			v = val
		} else {
			compositeKey := [12]byte{}
			copy(compositeKey[0:8], id)
			binary.BigEndian.PutUint32(compositeKey[8:12], uint32(i))
			var vAny any
			if err := s.maUn.Unmarshal(s.dataBucket().Get(compositeKey[:]), &vAny); err != nil {
				return err
			}
			v = vAny
			(*values)[i] = vAny
		}
		vKey, err := ToKey(v)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		binary.BigEndian.PutUint64(compositeKey[len(vKey):], binary.BigEndian.Uint64(id[0:8]))
		idxBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(idxBytes[0:4], uint32(i))
		if err := s.indexBucket().Bucket(idxBytes).Delete(compositeKey); err != nil {
			return err
		}
	}
	for i, spec := range s.metadata.ComputedColumnSpecs {
		if i+len(s.metadata.ColumnSpecs) == skip {
			continue
		}
		selectedValues := make([]any, len(spec.FieldRefs))
		for j, ref := range spec.FieldRefs {
			if v, ok := (*values)[j]; ok {
				selectedValues[j] = v
			} else {
				compositeKey := [12]byte{}
				copy(compositeKey[0:8], id)
				binary.BigEndian.PutUint32(compositeKey[8:12], uint32(ref))
				var vAny any
				if err := s.maUn.Unmarshal(s.dataBucket().Get(compositeKey[:]), &vAny); err != nil {
					return err
				}
				selectedValues[j] = vAny
				(*values)[j] = vAny
			}
		}
		vKey, err := ToKey(selectedValues...)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		binary.BigEndian.PutUint64(compositeKey[len(vKey):], binary.BigEndian.Uint64(id[0:8]))
		idxBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(idxBytes[0:4], uint32(i+len(s.metadata.ColumnSpecs)))
		if err := s.indexBucket().Bucket(idxBytes).Delete(compositeKey); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) toKey(id []byte, value *map[int]any, idx int) ([]byte, error) {
	if idx >= len(s.metadata.ColumnSpecs) {
		// computed column
		spec := s.metadata.ComputedColumnSpecs[idx-len(s.metadata.ColumnSpecs)]
		selectedValues := make([]any, len(spec.FieldRefs))
		for j, ref := range spec.FieldRefs {
			if v, ok := (*value)[j]; ok {
				selectedValues[j] = v
			} else {
				compositeKey := [12]byte{}
				copy(compositeKey[0:8], id)
				binary.BigEndian.PutUint32(compositeKey[8:12], uint32(ref))
				var vAny any
				if err := s.maUn.Unmarshal(s.dataBucket().Get(compositeKey[:]), &vAny); err != nil {
					return nil, err
				}
				selectedValues[j] = vAny
				(*value)[j] = vAny
			}
		}
		return ToKey(selectedValues...)
	} else {
		// regular column
		var v any
		if val, ok := (*value)[idx]; ok {
			v = val
		} else {
			compositeKey := [12]byte{}
			copy(compositeKey[0:8], id)
			binary.BigEndian.PutUint32(compositeKey[8:12], uint32(idx))
			var vAny any
			if err := s.maUn.Unmarshal(s.dataBucket().Get(compositeKey[:]), &vAny); err != nil {
				return nil, err
			}
			v = vAny
			(*value)[idx] = vAny
		}
		return ToKey(v)
	}
}

func (s *storage) Delete(ranges map[int]*BytesRange) error {
	shortestIndex := s.metadata.bestIndex(ranges)
	if shortestIndex == -1 {
		var prev []byte
		vals := make(map[int]any)
		c := s.dataBucket().Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			id := k[0:8]
			if prev != nil && !bytes.Equal(prev, id) {
				// delete indexes
				if err := s.deleteIndexes(prev, &vals, -1); err != nil {
					return err
				}
			}
			prev = id
			var vAny any
			if err := s.maUn.Unmarshal(v, &vAny); err != nil {
				return err
			}
			vals[int(binary.BigEndian.Uint32(k[8:12]))] = vAny
			if err := c.Delete(); err != nil {
				return err
			}
		}
		if prev != nil {
			if err := s.deleteIndexes(prev, &vals, -1); err != nil {
				return err
			}
		}
	}
	chosenRange := ranges[shortestIndex]
	var idxBytes [4]byte
	binary.BigEndian.PutUint32(idxBytes[:], uint32(shortestIndex))
	idxBucket := s.indexBucket().Bucket(idxBytes[:])
	c := idxBucket.Cursor()
	var k []byte
	if chosenRange.start != nil {
		k, _ = c.Seek(chosenRange.start)
	} else {
		k, _ = c.First()
	}
	lessThan := func(k []byte) bool {
		if chosenRange.end == nil {
			return true
		}
		cmp := bytes.Compare(k[:len(k)-8], chosenRange.end)
		return cmp < 0 || (cmp == 0 && chosenRange.includeEnd)
	}
	for ; k != nil && lessThan(k); k, _ = c.Next() {
		vals := make(map[int]any)
		// check other ranges
		if ok, err := s.inRanges(&vals, ranges); err != nil {
			return err
		} else if !ok {
			continue
		}
		// delete indexes
		if err := s.deleteIndexes(k[len(k)-8:], &vals, shortestIndex); err != nil {
			return err
		}
		// delete data
		for i := 0; i < len(s.metadata.ColumnSpecs); i++ {
			var columnKey [12]byte
			binary.BigEndian.PutUint64(columnKey[0:8], binary.BigEndian.Uint64(k[len(k)-8:]))
			binary.BigEndian.PutUint32(columnKey[8:12], uint32(i))
			if err := s.dataBucket().Delete(columnKey[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *storage) find(ranges map[int]*BytesRange, cols map[int]bool, mainIndex int) (iter.Seq2[*Row, error], error) {
	shortestIndex := mainIndex
	if shortestIndex < 0 {
		return func(yield func(*Row, error) bool) {
			var prev []byte
			results := &Row{
				values: make(map[int][]byte),
				maUn:   s.maUn,
			}
			c := s.dataBucket().Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				id := k[0:8]
				col := int(binary.BigEndian.Uint32(k[8:12]))
				if !cols[col] {
					continue
				}
				if prev != nil && !bytes.Equal(prev, id) {
					if !yield(results, nil) {
						return
					}
					results = &Row{
						values: make(map[int][]byte),
						maUn:   s.maUn,
					}
				}
				prev = id
				results.values[int(binary.BigEndian.Uint32(k[8:12]))] = v
			}
			if prev != nil {
				yield(results, nil)
			}
		}, nil
	}
	chosenRange := ranges[shortestIndex]
	return func(yield func(*Row, error) bool) {
		var idxBytes [4]byte
		binary.BigEndian.PutUint32(idxBytes[:], uint32(shortestIndex))
		idxBucket := s.indexBucket().Bucket(idxBytes[:])
		c := idxBucket.Cursor()
		var k []byte
		if chosenRange.start != nil {
			k, _ = c.Seek(chosenRange.start)
		} else {
			k, _ = c.First()
		}
		lessThan := func(k []byte) bool {
			if chosenRange.end == nil {
				return true
			}
			cmp := bytes.Compare(k[:len(k)-8], chosenRange.end)
			return cmp < 0 || (cmp == 0 && chosenRange.includeEnd)
		}
		for ; k != nil && lessThan(k); k, _ = c.Next() {
			// check range
			result := &Row{
				values: make(map[int][]byte),
				maUn:   s.maUn,
			}
			vals := make(map[int]any)
			// check other ranges
			if ok, err := s.inRanges(&vals, ranges); err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			} else if !ok {
				continue
			}
			for colIdx := range len(s.metadata.ColumnSpecs) {
				if !cols[colIdx] {
					continue
				}
				rowId := [12]byte{}
				copy(rowId[0:8], k[len(k)-8:])
				binary.BigEndian.PutUint32(rowId[8:12], uint32(colIdx))
				result.values[colIdx] = s.dataBucket().Get(rowId[:])
			}
			if !yield(result, nil) {
				return
			}
		}
	}, nil
}

func (s *storage) inRanges(vals *map[int]any, ranges map[int]*BytesRange) (bool, error) {
	for idx, kr := range ranges {
		kBytes, err := s.toKey(nil, vals, idx)
		if err != nil {
			return false, err
		}
		if !kr.Contains(kBytes) {
			return false, nil
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
	for i, spec := range s.metadata.ColumnSpecs {
		if !(spec.IsIndexed || spec.IsUnique) {
			continue
		}
		v := values[i]
		vKey, err := ToKey(v)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		var idxName [4]byte
		binary.BigEndian.PutUint32(idxName[:], uint32(i))
		curIdxBucket := s.indexBucket().Bucket(idxName[:])
		if curIdxBucket == nil {
			return ErrIndexNotFound(fmt.Sprintf("column %d", i))
		}
		if spec.IsUnique {
			k, _ := curIdxBucket.Cursor().Seek(vKey)
			if k != nil && bytes.Equal(k[:len(k)-8], vKey) {
				return ErrUniqueConstraint(fmt.Sprintf("column %d", i), v)
			}
		}
		// insert index
		binary.BigEndian.PutUint64(compositeKey[len(vKey):], id)
		err = curIdxBucket.Put(compositeKey, nil)
		if err != nil {
			return err
		}
	}
	for i, spec := range s.metadata.ComputedColumnSpecs {
		selectedValues := make([]any, len(spec.FieldRefs))
		for j, ref := range spec.FieldRefs {
			selectedValues[j] = values[ref]
		}
		vKey, err := ToKey(selectedValues...)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		var idxName [4]byte
		binary.BigEndian.PutUint32(idxName[:], uint32(i+len(s.metadata.ColumnSpecs)))
		curIdxBucket := s.indexBucket().Bucket(idxName[:])
		if curIdxBucket == nil {
			return ErrIndexNotFound(fmt.Sprintf("computed column %d", i))
		}
		if spec.IsUnique {
			k, _ := curIdxBucket.Cursor().Seek(vKey)
			if k != nil && bytes.Equal(k[:len(k)-8], vKey) {
				return ErrUniqueConstraint(fmt.Sprintf("computed column %d", i), selectedValues)
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
