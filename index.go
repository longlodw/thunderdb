package thunder

import (
	"bytes"
	"iter"

	"github.com/openkvlab/boltdb"
)

type indexStorage struct {
	bucket *boltdb.Bucket
}

func newIndex(
	parentBucket *boltdb.Bucket,
	idxNames []string,
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
	}, nil
}

func loadIndex(
	parentBucket *boltdb.Bucket,
) (*indexStorage, error) {
	bucket := parentBucket.Bucket([]byte("indexes"))
	if bucket == nil {
		return nil, nil
	}
	return &indexStorage{
		bucket: bucket,
	}, nil
}

func (idx *indexStorage) insert(name string, key []byte, id []byte) ([]byte, error) {
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	seq, err := indexBk.NextSequence()
	if err != nil {
		return nil, err
	}
	seqBytes, err := orderedMa.Marshal([]any{seq})
	if err != nil {
		return nil, err
	}
	bk, err := indexBk.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}
	return seqBytes, bk.Put(seqBytes, id)
}

func (idx *indexStorage) delete(name string, key []byte, seq []byte) error {
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return ErrIndexNotFound(name)
	}
	bk := indexBk.Bucket(key)
	if bk == nil {
		return nil
	}
	return bk.Delete(seq)
}

func (idx *indexStorage) get(name string, kr *keyRange) (iter.Seq[[]byte], error) {
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	return func(yield func([]byte) bool) {
		c := idxBk.Cursor()
		var startKey []byte
		if kr.startKey != nil {
			startKey, _ = c.Seek(kr.startKey)
		} else {
			startKey, _ = c.First()
		}
		if !kr.includeStart {
			startKey, _ = c.Next()
		}
		lessThanEnd := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmpEnd := bytes.Compare(k, kr.endKey)
			return cmpEnd < 0 || (cmpEnd == 0 && kr.includeEnd)
		}
		for k := startKey; k != nil && lessThanEnd(k); k, _ = c.Next() {
			if kr.doesExclude(k) {
				continue
			}
			curBucket := idxBk.Bucket(k)
			if curBucket == nil {
				continue
			}
			bc := curBucket.Cursor()
			for ik, iv := bc.First(); ik != nil; ik, iv = bc.Next() {
				if !yield(iv) {
					return
				}
			}
		}
	}, nil
}
