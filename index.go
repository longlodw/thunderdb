package thunderdb

import (
	"bytes"
	"iter"

	"github.com/openkvlab/boltdb"
	"rsc.io/ordered"
)

type indexStorage struct {
	bucket *boltdb.Bucket
	maUn   MarshalUnmarshaler
}

func newIndex(
	parentBucket *boltdb.Bucket,
	idxNames []string,
	maUn MarshalUnmarshaler,
) (*indexStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("indexes"))
	if err != nil {
		return nil, err
	}
	for _, name := range idxNames {
		_, err := bucket.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return nil, err
		}
	}
	return &indexStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func loadIndex(
	parentBucket *boltdb.Bucket,
	maUn MarshalUnmarshaler,
) (*indexStorage, error) {
	bucket := parentBucket.Bucket([]byte("indexes"))
	if bucket == nil {
		return nil, nil
	}
	return &indexStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func (idx *indexStorage) insert(name string, key, id []byte) error {
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	compositeKey, err := ToKey(key, id)
	if err != nil {
		return err
	}
	return indexBk.Put(compositeKey, nil)
}

func (idx *indexStorage) delete(name string, key, id []byte) error {
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	compositeKey, err := ToKey(key, id)
	if err != nil {
		return err
	}
	return indexBk.Delete(compositeKey)
}

func (idx *indexStorage) get(name string, kr *BytesRange) (iter.Seq2[[8]byte, error], error) {
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	return func(yield func([8]byte, error) bool) {
		c := idxBk.Cursor()
		var k []byte
		var seekPrefix []byte
		var err error

		// Prepare Search Limits
		// ToKey encodes the values using ordered.Encode.
		// Since index keys are ordered.Encode(Val, ID), comparing against ordered.Encode(Val) works effectively
		// as a prefix check or range bound.
		if kr.start != nil {
			seekPrefix, err = ToKey(kr.start)
			if err != nil {
				if !yield([8]byte{}, err) {
					return
				}
				return
			}
			k, _ = c.Seek(seekPrefix)
		} else {
			k, _ = c.First()
		}

		var endLimit []byte
		if kr.end != nil {
			endLimit, err = ToKey(kr.end)
			if err != nil {
				if !yield([8]byte{}, err) {
					return
				}
				return
			}
		}

		// Optimization: Check for exact match on the prefix (Value part of the key).
		// If seekPrefix == endLimit, we are looking for a specific Value.
		// In this case, the key 'k' starts with seekPrefix, and the remainder is the Encoded ID.
		// We can skip decoding the Value and just decode the ID from the suffix.
		exactMatch := seekPrefix != nil && endLimit != nil && bytes.Equal(seekPrefix, endLimit)

		// Prepare Excludes
		var excludes [][]byte
		if len(kr.excludes) > 0 {
			excludes = make([][]byte, len(kr.excludes))
			for i, ex := range kr.excludes {
				excludes[i], err = ToKey(ex)
				if err != nil {
					if !yield([8]byte{}, err) {
						return
					}
					return
				}
			}
		}

		var id [8]byte

		// Buffers for decoding to reduce allocation
		// We reuse these slices across iterations.
		var valBuf []byte
		var idBuf []byte

		for ; k != nil; k, _ = c.Next() {
			// 1. Check End of Range
			if endLimit != nil {
				// k is [EncodedVal, EncodedID]
				// endLimit is [EncodedVal]
				// If k starts with endLimit, it means Value == EndValue.
				if bytes.HasPrefix(k, endLimit) {
					if !kr.includeEnd {
						// Exact match on value, but exclude requested.
						// Since all entries for this value are grouped, we are done.
						break
					}
				} else {
					// Check strict order
					if bytes.Compare(k, endLimit) > 0 {
						break
					}
				}
			}

			// 2. Check Start of Range
			// Seek positioned us at >= start.
			// If we are strictly greater, we are fine.
			// If we are equal (prefix match), we need to check includeStart.
			if seekPrefix != nil && !kr.includeStart {
				if bytes.HasPrefix(k, seekPrefix) {
					continue
				}
			}

			// 3. Check Excludes
			isExcluded := false
			for _, ex := range excludes {
				if bytes.HasPrefix(k, ex) {
					isExcluded = true
					break
				}
			}
			if isExcluded {
				continue
			}

			// 4. Decode ID
			// We decoded everything else via raw bytes, now we just need the ID.
			// using typed Decode is significantly faster than Unmarshal/DecodeAny.

			idBuf = idBuf[:0]

			if exactMatch {
				// Optimization: Skip Value decoding
				// k = [seekPrefix][EncodedID]
				// We just decode [EncodedID]
				suffix := k[len(seekPrefix):]
				if err := ordered.Decode(suffix, &idBuf); err != nil {
					if !yield([8]byte{}, err) {
						return
					}
					continue
				}
			} else {
				valBuf = valBuf[:0]
				if err := ordered.Decode(k, &valBuf, &idBuf); err != nil {
					if !yield([8]byte{}, err) {
						return
					}
					continue
				}
			}

			if len(idBuf) != 8 {
				if yield([8]byte{}, ErrCorruptedIndexEntry(name)) {
					continue
				} else {
					return
				}
			}
			copy(id[:], idBuf)

			if !yield(id, nil) {
				return
			}
		}
	}, nil
}
