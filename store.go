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
	fieldRefs []int
	isUnique  bool
}

type ColumnSpec struct {
	isUnique  bool
	isIndexed bool
}

type storageMetadata struct {
	collumnSpecs        []ColumnSpec
	computedColumnSpecs []ComputedColumnSpec
}

type storage struct {
	bucket   *boltdb.Bucket
	metadata storageMetadata
	maUn     MarshalUnmarshaler
}

type storageRow struct {
	values map[int][]byte
	maUn   MarshalUnmarshaler
}

func (sr *storageRow) Get(idx int, v any) error {
	vBytes, ok := sr.values[idx]
	if !ok {
		return ErrFieldNotFound(fmt.Sprintf("column %d", idx))
	}
	return sr.maUn.Unmarshal(vBytes, v)
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
		if !(spec.isUnique || spec.isIndexed) {
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
		binary.BigEndian.PutUint32(idxName[0:4], uint32(i))
		if _, err := idxBucket.CreateBucketIfNotExists(idxName[:]); err != nil {
			return nil, err
		}
	}

	s := &storage{
		bucket: bucket,
		metadata: storageMetadata{
			collumnSpecs:        collumnSpecs,
			computedColumnSpecs: computedColumnSpecs,
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

func loadStorage(
	rootBucket *boltdb.Bucket,
	name string,
	maUn MarshalUnmarshaler,
) (*storage, error) {
	bucket := rootBucket.Bucket([]byte(name))
	if bucket == nil {
		return nil, boltdb_errors.ErrBucketNotFound
	}
	metadataBytes := bucket.Get([]byte("metadata"))
	if metadataBytes == nil {
		return nil, ErrMetaDataNotFound(name)
	}
	s := &storage{
		bucket: bucket,
		maUn:   maUn,
	}
	if err := GobMaUn.Unmarshal(metadataBytes, &(s.metadata)); err != nil {
		return nil, err
	}
	return s, nil
}

func deleteStorage(rootBucket *boltdb.Bucket, name string) error {
	return rootBucket.DeleteBucket([]byte(name))
}

func (s *storage) dataBucket() *boltdb.Bucket {
	return s.bucket.Bucket([]byte("data"))
}

func (s *storage) indexBucket() *boltdb.Bucket {
	return s.bucket.Bucket([]byte("index"))
}

func (s *storage) deleteIndexes(id []byte, values *map[int]any, skip int) error {
	for i, spec := range s.metadata.collumnSpecs {
		if !(spec.isIndexed || spec.isUnique) || i == skip {
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
	for i, spec := range s.metadata.computedColumnSpecs {
		if i+len(s.metadata.collumnSpecs) == skip {
			continue
		}
		selectedValues := make([]any, len(spec.fieldRefs))
		for j, ref := range spec.fieldRefs {
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
		binary.BigEndian.PutUint32(idxBytes[0:4], uint32(i+len(s.metadata.collumnSpecs)))
		if err := s.indexBucket().Bucket(idxBytes).Delete(compositeKey); err != nil {
			return err
		}
	}
	return nil
}

func (s *storage) toKey(id []byte, value *map[int]any, idx int) ([]byte, error) {
	if idx >= len(s.metadata.collumnSpecs) {
		// computed column
		spec := s.metadata.computedColumnSpecs[idx-len(s.metadata.collumnSpecs)]
		selectedValues := make([]any, len(spec.fieldRefs))
		for j, ref := range spec.fieldRefs {
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
	selectedIndexes := make([]int, 0, len(ranges))
	for i := range ranges {
		if (i < len(s.metadata.collumnSpecs) && s.metadata.collumnSpecs[i].isIndexed || s.metadata.collumnSpecs[i].isUnique) || (i >= len(s.metadata.collumnSpecs) && s.metadata.computedColumnSpecs[i-len(s.metadata.collumnSpecs)].isUnique) {
			selectedIndexes = append(selectedIndexes, i)
		}
	}
	if len(selectedIndexes) == 0 {
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
	shortestIndex := slices.MinFunc(selectedIndexes, func(a, b int) int {
		return bytes.Compare(ranges[a].distance, ranges[b].distance)
	})
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
		for i := 0; i < len(s.metadata.collumnSpecs); i++ {
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

func (s *storage) Find(ranges map[int]*BytesRange, cols map[int]bool) (iter.Seq2[*storageRow, error], error) {
	selectedIndexes := make([]int, 0, len(ranges))
	for i := range ranges {
		if (i < len(s.metadata.collumnSpecs) && s.metadata.collumnSpecs[i].isIndexed || s.metadata.collumnSpecs[i].isUnique) || (i >= len(s.metadata.collumnSpecs) && s.metadata.computedColumnSpecs[i-len(s.metadata.collumnSpecs)].isUnique) {
			selectedIndexes = append(selectedIndexes, i)
		}
	}
	if len(selectedIndexes) == 0 {
		return func(yield func(*storageRow, error) bool) {
			var prev []byte
			results := &storageRow{
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
					results = &storageRow{
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
	shortestIndex := slices.MinFunc(selectedIndexes, func(a, b int) int {
		return bytes.Compare(ranges[a].distance, ranges[b].distance)
	})
	chosenRange := ranges[shortestIndex]
	return func(yield func(*storageRow, error) bool) {
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
			result := &storageRow{
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
			for colIdx := range len(s.metadata.collumnSpecs) {
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
	for i, spec := range s.metadata.collumnSpecs {
		if !(spec.isIndexed || spec.isUnique) {
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
		if spec.isUnique {
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
	for i, spec := range s.metadata.computedColumnSpecs {
		selectedValues := make([]any, len(spec.fieldRefs))
		for j, ref := range spec.fieldRefs {
			selectedValues[j] = values[ref]
		}
		vKey, err := ToKey(selectedValues...)
		if err != nil {
			return err
		}
		compositeKey := make([]byte, len(vKey)+8)
		copy(compositeKey, vKey)
		var idxName [4]byte
		binary.BigEndian.PutUint32(idxName[:], uint32(i+len(s.metadata.collumnSpecs)))
		curIdxBucket := s.indexBucket().Bucket(idxName[:])
		if spec.isUnique {
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
