package thunderdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
)

type storage struct {
	bucket   *boltdb.Bucket
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
	for i, _ := range indexes {
		var idxName [8]byte
		binary.BigEndian.PutUint64(idxName[:], i)
		if _, err := idxBucket.CreateBucketIfNotExists(idxName[:]); err != nil {
			return nil, err
		}
	}

	s := &storage{
		bucket: bucket,
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

func (s *storage) deleteIndexes(id []byte, values *map[int]any, skip *uint64) error {
	for i, _ := range s.metadata.Indexes {
		if skip != nil && uint64(i) == *skip {
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

func (s *storage) Delete(equals map[int]*Value, ranges map[int]*BytesRange, excludes map[int][]*Value) error {
	shortestIndex, shortestRanges, err := s.metadata.bestIndex(equals, ranges)
	if err != nil {
		return err
	}
	if shortestRanges == nil {
		var prev []byte
		vals := make(map[int]any)
		c := s.dataBucket().Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			id := k[0:8]
			if prev != nil && !bytes.Equal(prev, id) {
				// delete indexes
				if err := s.deleteIndexes(prev, &vals, nil); err != nil {
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
			if err := s.deleteIndexes(prev, &vals, nil); err != nil {
				return err
			}
		}
	}
	var idxBytes [4]byte
	binary.BigEndian.PutUint32(idxBytes[:], uint32(shortestIndex))
	idxBucket := s.indexBucket().Bucket(idxBytes[:])
	c := idxBucket.Cursor()
	var k []byte
	if shortestRanges.start != nil {
		k, _ = c.Seek(shortestRanges.start)
	} else {
		k, _ = c.First()
	}
	lessThan := func(k []byte) bool {
		if shortestRanges.end == nil {
			return true
		}
		cmp := bytes.Compare(k[:len(k)-8], shortestRanges.end)
		return cmp < 0 || (cmp == 0 && shortestRanges.includeEnd)
	}
	for ; k != nil && lessThan(k); k, _ = c.Next() {
		vals := make(map[int]any)
		// check other ranges
		if ok, err := s.inRanges(k[len(k)-8:], &vals, equals, ranges, excludes); err != nil {
			return err
		} else if !ok {
			continue
		}
		// delete indexes
		if err := s.deleteIndexes(k[len(k)-8:], &vals, &shortestIndex); err != nil {
			return err
		}
		// delete data
		for i := range s.metadata.ColumnsCount {
			var columnKey [12]byte
			binary.BigEndian.PutUint64(columnKey[0:8], binary.BigEndian.Uint64(k[len(k)-8:]))
			binary.BigEndian.PutUint32(columnKey[8:12], uint32(i))
			if err := s.dataBucket().Delete(columnKey[:]); err != nil {
				return err
			}
		}
		// delete current index entry
		if err := c.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) find(
	mainIndex uint64,
	indexRange *BytesRange,
	equals map[int]*Value,
	ranges map[int]*BytesRange,
	exclusion map[int][]*Value,
	cols map[int]bool,
) (iter.Seq2[*Row, error], error) {
	if indexRange == nil {
		return func(yield func(*Row, error) bool) {
			var prev []byte
			results := &Row{
				values: make(map[int][]byte),
				maUn:   s.maUn,
			}
			vals := make(map[int]any)
			c := s.dataBucket().Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				id := k[0:8]
				col := int(binary.BigEndian.Uint32(k[8:12]))
				if !cols[col] {
					continue
				}
				if prev != nil && !bytes.Equal(prev, id) {
					if ok, err := s.inRanges(prev, &vals, equals, ranges, exclusion); err != nil {
						if !yield(nil, err) {
							return
						}
						return
					} else if ok {
						if !yield(results, nil) {
							return
						}
					}
					results = &Row{
						values: make(map[int][]byte),
						maUn:   s.maUn,
					}
					vals = make(map[int]any)
				}
				prev = id
				results.values[col] = v
			}
			if prev != nil {
				if ok, err := s.inRanges(prev, &vals, equals, ranges, exclusion); err != nil {
					yield(nil, err)
				} else if ok {
					yield(results, nil)
				}
			}
		}, nil
	}
	return func(yield func(*Row, error) bool) {
		var idxBytes [8]byte
		binary.BigEndian.PutUint64(idxBytes[:], mainIndex)
		idxBucket := s.indexBucket().Bucket(idxBytes[:])
		c := idxBucket.Cursor()
		var k []byte
		if indexRange.start != nil {
			k, _ = c.Seek(indexRange.start)
		} else {
			k, _ = c.First()
		}
		lessThan := func(k []byte) bool {
			if indexRange.end == nil {
				return true
			}
			cmp := bytes.Compare(k[:len(k)-8], indexRange.end)
			return cmp < 0 || (cmp == 0 && indexRange.includeEnd)
		}
		for ; k != nil && lessThan(k); k, _ = c.Next() {
			// check range
			result := &Row{
				values: make(map[int][]byte),
				maUn:   s.maUn,
			}
			vals := make(map[int]any)
			// check other ranges
			if ok, err := s.inRanges(k[len(k)-8:], &vals, equals, ranges, exclusion); err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			} else if !ok {
				continue
			}
			for colIdx := range s.metadata.ColumnsCount {
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

func (s *storage) inRanges(id []byte, vals *map[int]any, equals map[int]*Value, ranges map[int]*BytesRange, exclusions map[int][]*Value) (bool, error) {
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
		if !kr.Contains(kBytes) {
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
			return ErrIndexNotFound(fmt.Sprintf("column %d", i))
		}
		if isUnique {
			k, _ := curIdxBucket.Cursor().Seek(vKey)
			if k != nil && bytes.Equal(k[:len(k)-8], vKey) {
				return ErrUniqueConstraint(fmt.Sprintf("column %d", i), vKey)
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
	for i := 0; i < 64; i++ {
		if (idx & (1 << uint64(i))) != 0 {
			refs = append(refs, i)
		}
	}
	return refs
}
