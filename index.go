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

func (idx *indexStorage) insert(name string, keyParts []any, id []byte) ([]byte, error) {
	key, err := orderedMa.Marshal(keyParts)
	if err != nil {
		return nil, err
	}
	indexBk := idx.bucket.Bucket([]byte(name))
	if indexBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	seq, err := indexBk.NextSequence()
	if err != nil {
		return nil, err
	}
	seqBytes, err := orderedMa.Marshal(seq)
	if err != nil {
		return nil, err
	}
	bk, err := indexBk.CreateBucketIfNotExists(key)
	if err != nil {
		return nil, err
	}
	return seqBytes, bk.Put(seqBytes, id)
}

func (idx *indexStorage) delete(name string, keyParts []any, seq []byte) error {
	key, err := orderedMa.Marshal(keyParts)
	if err != nil {
		return err
	}
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

func (idx *indexStorage) get(operator OpType, name string, keyParts []any) (iter.Seq[[]byte], error) {
	key, err := orderedMa.Marshal(keyParts)
	if err != nil {
		return nil, err
	}
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, ErrIndexNotFound(name)
	}
	return func(yield func([]byte) bool) {
		if operator&OpLt != 0 {
			c := idxBk.Cursor()
			k, _ := c.Seek(key)
			if k == nil {
				k, _ = c.Last()
			} else {
				k, _ = c.Prev()
			}
			for ; k != nil; k, _ = c.Prev() {
				bk := idxBk.Bucket(k)
				if bk == nil {
					continue
				}
				c2 := bk.Cursor()
				for k2, v2 := c2.First(); k2 != nil; k2, v2 = c2.Next() {
					if v2 != nil && !yield(v2) {
						return
					}
				}
			}
		}
		if operator&OpEq != 0 {
			bk := idxBk.Bucket(key)
			if bk != nil {
				c := bk.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					if v != nil && !yield(v) {
						return
					}
				}
			}
		}
		if operator&OpGt != 0 {
			c := idxBk.Cursor()
			k, _ := c.Seek(key)
			if k != nil && bytes.Equal(k, key) {
				k, _ = c.Next()
			}
			for ; k != nil; k, _ = c.Next() {
				bk := idxBk.Bucket(k)
				if bk == nil {
					continue
				}
				c2 := bk.Cursor()
				for k2, v2 := c2.First(); k2 != nil; k2, v2 = c2.Next() {
					if v2 != nil && !yield(v2) {
						return
					}
				}
			}
		}
	}, nil
}
