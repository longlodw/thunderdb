package thunderdb

import (
	"bytes"
	"iter"

	"github.com/openkvlab/boltdb"
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

func (idx *indexStorage) get(name string, kr *keyRange) (iter.Seq2[[8]byte, error], error) {
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	return func(yield func([8]byte, error) bool) {
		c := idxBk.Cursor()
		var k []byte
		var seekPrefix []byte
		var err error

		if kr.startKey != nil {
			seekPrefix, err = ToKey(kr.startKey)
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

		lessThanEnd := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmpEnd := bytes.Compare(k, kr.endKey)
			return cmpEnd < 0 || (cmpEnd == 0 && kr.includeEnd)
		}

		for ; k != nil; k, _ = c.Next() {
			var parts []any
			if err := orderedMa.Unmarshal(k, &parts); err != nil {
				if !yield([8]byte{}, err) {
					return
				}
				continue
			}

			if len(parts) != 2 {
				if yield([8]byte{}, ErrCorruptedIndexEntry(name)) {
					continue
				} else {
					return
				}
			}

			var valBytes []byte
			switch v := parts[0].(type) {
			case []byte:
				valBytes = v
			case string:
				valBytes = []byte(v)
			default:
				if yield([8]byte{}, ErrCorruptedIndexEntry(name)) {
					continue
				} else {
					return
				}
			}

			idAny := parts[1]

			var id [8]byte
			switch v := idAny.(type) {
			case string:
				if len(v) != 8 {
					if yield([8]byte{}, ErrCorruptedIndexEntry(name)) {
						continue
					} else {
						return
					}
				}
				copy(id[:], v)
			case []byte:
				if len(v) != 8 {
					if yield([8]byte{}, ErrCorruptedIndexEntry(name)) {
						continue
					} else {
						return
					}
				}
				copy(id[:], v)
			default:
				if yield([8]byte{}, ErrCorruptedIndexEntry(name)) {
					continue
				} else {
					return
				}
			}

			if !lessThanEnd(valBytes) {
				break
			}
			if !kr.contains(valBytes) {
				continue
			}

			if !yield(id, nil) {
				return
			}
		}
	}, nil
}
