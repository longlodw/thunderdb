package thunderdb

import (
	"bytes"
	"encoding/binary"
	"iter"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
)

type storage struct {
	bucket   *boltdb.Bucket
	name     string
	metadata Metadata
}

// Row represents a single row returned from a query. It provides methods
// to access column values by index.
type Row struct {
	values map[int][]byte
}

// Get retrieves a column value by index and unmarshals it into the provided
// pointer. The destination must be a pointer to the expected type.
//
// Returns ErrFieldNotFound if the column index doesn't exist in the row.
//
// Example:
//
//	var username string
//	var age int
//	if err := row.Get(1, &username); err != nil {
//	    return err
//	}
//	if err := row.Get(2, &age); err != nil {
//	    return err
//	}
func (sr *Row) Get(idx int, v any) error {
	vBytes, ok := sr.values[idx]
	if !ok {
		return ErrFieldNotFound(idx)
	}
	return orderedMaUn.Unmarshal(vBytes, v)
}

// Iter returns an iterator over all columns in the row. Each iteration yields
// the column index and a Value that can be used to retrieve the column's data.
func (sr *Row) Iter() iter.Seq2[int, *Value] {
	return func(yield func(int, *Value) bool) {
		for k, b := range sr.values {
			if !yield(k, ValueOfRaw(b)) {
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
	}
	metadataBytes, err := gobCodec.Marshal(s.metadata)
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
	if err := gobCodec.Unmarshal(metadataBytes, metadata); err != nil {
		return err
	}
	return nil
}

func loadStorage(
	tx *boltdb.Tx,
	name string,
) (*storage, error) {
	bucket := tx.Bucket([]byte(name))
	if bucket == nil {
		return nil, boltdb_errors.ErrBucketNotFound
	}
	s := &storage{
		bucket: bucket,
		name:   name,
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

func (s *storage) deleteIndexes(id []byte, values *map[int]*Value, skip map[uint64]bool) error {
	for i := range s.metadata.Indexes {
		if skip != nil && skip[i] {
			continue
		}
		selectedValues := make([]*Value, 0, s.metadata.ColumnsCount)
		for j := range ReferenceColumns(uint64(i)) {
			if v, ok := (*values)[j]; ok {
				selectedValues = append(selectedValues, v)
			} else {
				return ErrFieldNotFound(j)
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

type scanResult struct {
	id               []byte
	values           map[int]*Value
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
	colsForConditionCheck := make(map[int]bool)
	for idx := range equals {
		colsForConditionCheck[idx] = true
	}
	for idx := range ranges {
		colsForConditionCheck[idx] = true
	}
	for idx := range excludes {
		colsForConditionCheck[idx] = true
	}
	return func(yield func(*scanResult, error) bool) {
		// Reusable buffers
		vals := make(map[int]*Value)
		rawVals := make(map[int][]byte)
		valueCache := make(map[int]*Value)
		res := &scanResult{
			values:    vals,
			rawValues: rawVals,
		}

		if forcedIndex == 0 {
			// Full Scan
			c := s.dataBucket().Cursor()
			var prev []byte

			// Clear buffers initially
			clear(vals)
			clear(rawVals)

			checkAndYield := func(k []byte) bool {
				if prev == nil {
					return true
				}
				ok, err := inRanges(vals, equals, ranges, excludes)
				if err != nil {
					return yield(nil, err)
				}
				if ok {
					res.id = prev
					// res.values/rawValues are already pointing to vals/rawVals
					if !yield(res, nil) {
						return false
					}
					if k != nil {
						c.Seek(k)
					}
				}
				return true
			}
			k, v := c.First()
			for ; k != nil; k, v = c.Next() {
				id := k[0:8]
				col := int(binary.BigEndian.Uint32(k[8:12]))

				if prev != nil && !bytes.Equal(prev, id) {
					if !checkAndYield(k) {
						return
					}
					// Reset buffers for the next row
					clear(vals)
					clear(rawVals)
				}
				prev = id

				// Logic for what to unmarshal/keep raw
				shouldUnmarshal := unmarshalCols != nil && unmarshalCols[col]
				shouldKeepRaw := rawCols != nil && rawCols[col]

				if shouldKeepRaw {
					// println("Keeping raw column", col, "relation", s.name)
					rawVals[col] = v
				}

				if shouldUnmarshal || colsForConditionCheck[col] {
					// Lazy unmarshaling: store raw bytes in Value
					val, ok := valueCache[col]
					if !ok {
						val = ValueOfRaw(v)
						valueCache[col] = val
					} else {
						val.SetRaw(v) // Use SetRaw to properly clear caches
					}
					vals[col] = val
				}
			}
			// Yield the last row
			if prev != nil {
				checkAndYield(nil)
			}
		} else {
			// Index Scan
			var idxName [8]byte
			binary.BigEndian.PutUint64(idxName[:], forcedIndex)
			idxBucket := s.indexBucket().Bucket(idxName[:])
			if idxBucket == nil {
				return
			}
			c := idxBucket.Cursor()

			var startKey []byte
			if forcedRange != nil && forcedRange.start != nil {
				sk, err := forcedRange.start.GetRaw()
				if err != nil {
					yield(nil, err)
					return
				}
				startKey = sk
			}

			// Range end check
			var endKey []byte
			if forcedRange != nil && forcedRange.end != nil {
				ek, err := forcedRange.end.GetRaw()
				if err != nil {
					yield(nil, err)
					return
				}
				endKey = ek
			}

			// Clear buffers initially
			clear(vals)
			clear(rawVals)

			var k []byte
			if startKey != nil {
				k, _ = c.Seek(startKey)
			} else {
				k, _ = c.First()
			}

			dataC := s.dataBucket().Cursor()

			for ; k != nil; k, _ = c.Next() {
				// Check range end
				if endKey != nil {
					// Index keys are [ValueBytes][RowID].
					// We check if ValueBytes > endKey.
					// ValueBytes length is len(k) - 8.
					if len(k) > 8 {
						valBytes := k[:len(k)-8]
						if bytes.Compare(valBytes, endKey) > 0 {
							break
						}
					}
				}

				// Extract ID
				if len(k) < 8 {
					continue
				}
				id := k[len(k)-8:]

				// Fetch columns for this ID
				var rowPrefix [8]byte
				copy(rowPrefix[:], id)

				// Seek to the first column of this row in data bucket
				// Keys are [ID 8 bytes][ColID 4 bytes]
				var seekData [12]byte
				copy(seekData[:], id)

				dk, dv := dataC.Seek(seekData[:])
				for ; dk != nil; dk, dv = dataC.Next() {
					if !bytes.HasPrefix(dk, rowPrefix[:]) {
						break
					}
					col := int(binary.BigEndian.Uint32(dk[8:12]))

					shouldUnmarshal := unmarshalCols != nil && unmarshalCols[col]
					shouldKeepRaw := rawCols != nil && rawCols[col]

					if shouldKeepRaw {
						rawVals[col] = dv
					}

					if shouldUnmarshal || colsForConditionCheck[col] {
						// Lazy unmarshaling: store raw bytes in Value
						val, ok := valueCache[col]
						if !ok {
							val = ValueOfRaw(dv)
							valueCache[col] = val
						} else {
							val.SetRaw(dv) // Use SetRaw to properly clear caches
						}
						vals[col] = val
					}
				}

				ok, err := inRanges(vals, equals, ranges, excludes)
				if err != nil {
					yield(nil, err)
					return
				}
				if ok {
					res.id = id
					currentK := k
					res.deleteIndexEntry = func() error {
						return idxBucket.Delete(currentK)
					}
					if !yield(res, nil) {
						return
					}
				}

				// Reset buffers
				clear(vals)
				clear(rawVals)
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
		vBytes, err := orderedMaUn.Marshal(newVal)
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
) (int64, error) {
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
		for colIdx := range refCols {
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
		return 0, err
	}
	updateMainIndex := indexesToUpdate[shortestIndex]

	var updated int64
	for res, err := range s.scan(shortestIndex, shortestRange, equals, ranges, excludes, colsToUnmarshal, nil) {
		if err != nil {
			return updated, err
		}
		// delete old indexes
		if err := s.deleteIndexes(res.id, &res.values, indexesToSkip); err != nil {
			return updated, err
		}
		// If iterating by index and that index is being updated, we must delete the current index entry
		if updateMainIndex && res.deleteIndexEntry != nil {
			if err := res.deleteIndexEntry(); err != nil {
				return updated, err
			}
			indexesToSkip[shortestIndex] = true
		}
		// apply updates
		for colIdx, newVal := range updates {
			res.values[colIdx] = ValueOfLiteral(newVal)
		}
		// insert new indexes
		if err := s.insertIndexes(res.id, &res.values, indexesToSkip); err != nil {
			return updated, err
		}
		// update data
		if err := s.updateData(res.id, updates); err != nil {
			return updated, err
		}
		updated++
	}
	return updated, nil
}

func (s *storage) insertIndexes(id []byte, values *map[int]*Value, skip map[uint64]bool) error {
	for i, isUnique := range s.metadata.Indexes {
		if skip != nil && skip[i] {
			continue
		}
		selectedValues := make([]*Value, 0, s.metadata.ColumnsCount)
		for j := range ReferenceColumns(i) {
			if v, ok := (*values)[j]; ok {
				selectedValues = append(selectedValues, v)
			} else {
				return ErrFieldNotFound(j)
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

func (s *storage) Delete(equals map[int]*Value, ranges map[int]*Range, excludes map[int][]*Value) (int64, error) {
	shortestIndex, shortestRange, err := s.metadata.bestIndex(equals, ranges)
	if err != nil {
		return 0, err
	}

	var deleted int64
	for res, err := range s.scan(shortestIndex, shortestRange, equals, ranges, excludes, nil, nil) {
		if err != nil {
			return deleted, err
		}
		// delete indexes
		skip := map[uint64]bool{}
		if res.deleteIndexEntry != nil {
			skip[shortestIndex] = true
			if err := res.deleteIndexEntry(); err != nil {
				return deleted, err
			}
		}
		if err := s.deleteIndexes(res.id, &res.values, skip); err != nil {
			return deleted, err
		}
		// delete data
		if err := s.deleteData(res.id); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
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
		// Create a reusable Row
		row := &Row{}

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
						res.rawValues[col] = v
					}
				}
			}

			// Point the reusable row to the current values
			row.values = res.rawValues

			if !yield(row, nil) {
				return
			}
		}
	}, nil
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
		vBytes, err := orderedMaUn.Marshal(v)
		if err != nil {
			return err
		}
		err = s.dataBucket().Put(columnKey[:], vBytes)
		if err != nil {
			return err
		}
	}
	boxedValues := make(map[int]*Value)
	for i, v := range values {
		boxedValues[i] = ValueOfLiteral(v)
	}
	// check unique and insert indexes
	for i, isUnique := range s.metadata.Indexes {
		// Collect values for this index
		selectedValues := make([]*Value, 0, s.metadata.ColumnsCount)
		for j := range ReferenceColumns(i) {
			if v, ok := boxedValues[j]; ok {
				selectedValues = append(selectedValues, v)
			} else {
				return ErrFieldNotFound(j)
			}
		}
		vKey, err := ToKey(selectedValues...)
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

// ReferenceColumns returns an iterator over the column indices that are part
// of the given index bitmap. This is useful for inspecting which columns
// make up a composite index.
func ReferenceColumns(idx uint64) iter.Seq[int] {
	return func(yield func(int) bool) {
		for i := range 64 {
			if (idx & (1 << uint64(i))) != 0 {
				if !yield(i) {
					return
				}
			}
		}
	}
}
