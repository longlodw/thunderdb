package thunder

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

func (idx *indexStorage) insert(name string, idxLoc *indexLocator) error {
	key := idxLoc.Key
	id := idxLoc.Id
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	compositeKey, err := orderedMa.Marshal([]any{key, id})
	if err != nil {
		return err
	}
	compositeKeyStored, err := idx.maUn.Marshal(idxLoc)
	if err != nil {
		return err
	}
	return indexBk.Put(compositeKey, compositeKeyStored)
}

func (idx *indexStorage) delete(name string, idxLoc *indexLocator) error {
	key := idxLoc.Key
	seq := idxLoc.Id
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	compositeKey, err := orderedMa.Marshal([]any{key, seq})
	if err != nil {
		return err
	}
	return indexBk.Delete(compositeKey)
}

type indexLocator struct {
	Key []byte `json:"key"`
	Id  uint64 `json:"id"`
}

func (idx *indexStorage) get(name string, kr *keyRange) (iter.Seq2[uint64, error], error) {
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	return func(yield func(uint64, error) bool) {
		c := idxBk.Cursor()
		var startKey, v []byte
		if kr.startKey != nil {
			startKey, v = c.Seek(kr.startKey)
		} else {
			startKey, v = c.First()
		}
		if !kr.includeStart {
			startKey, v = c.Next()
		}
		lessThanEnd := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmpEnd := bytes.Compare(k, kr.endKey)
			return cmpEnd < 0 || (cmpEnd == 0 && kr.includeEnd)
		}
		for k := startKey; k != nil; k, v = c.Next() {
			var keyPart indexLocator
			if err := idx.maUn.Unmarshal(v, &keyPart); err != nil {
				if !yield(0, err) {
					return
				}
				continue
			}
			idxKey := keyPart.Key
			if !kr.contains(idxKey) || kr.doesExclude(idxKey) {
				continue
			}
			if !lessThanEnd(idxKey) {
				break
			}
			if !yield(keyPart.Id, nil) {
				return
			}
		}
	}, nil
}
