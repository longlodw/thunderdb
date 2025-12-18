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
	maUn MarshalUnmarshaler,
) (*indexStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("indexes"))
	if err != nil {
		return nil, err
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

func (idx *indexStorage) insert(name string, keyParts []any, id []byte) ([]byte, error) {
	key, err := idx.toKey(keyParts)
	if err != nil {
		return nil, err
	}
	indexBk, err := idx.bucket.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}
	seq, err := indexBk.NextSequence()
	if err != nil {
		return nil, err
	}
	seqBytes, err := idx.maUn.Marshal(seq)
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
	key, err := idx.toKey(keyParts)
	if err != nil {
		return err
	}
	indexBk, err := idx.bucket.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return err
	}
	bk := indexBk.Bucket(key)
	if bk == nil {
		return nil
	}
	return bk.Delete(seq)
}

func (idx *indexStorage) get(operator OpType, name string, keyParts []any) (iter.Seq[[]byte], error) {
	key, err := idx.toKey(keyParts)
	if err != nil {
		return nil, err
	}
	idxBk := idx.bucket.Bucket([]byte(name))
	if idxBk == nil {
		return nil, nil
	}
	return func(yield func([]byte) bool) {
		if operator&OpLt != 0 {
			c := idxBk.Cursor()
			for k, _ := c.First(); k != nil && bytes.Compare(k, key) < 0; k, _ = c.Next() {
				bk := idxBk.Bucket(k)
				if bk != nil {
					continue
				}
				c2 := bk.Cursor()
				for k2, v2 := c2.First(); k2 != nil; k2, v2 = c2.Next() {
					if v2 != nil && !yield(k2) {
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
			c.Seek(key)
			for k, _ := c.Next(); k != nil; k, _ = c.Next() {
				bk := idxBk.Bucket(k)
				if bk != nil {
					continue
				}
				c2 := bk.Cursor()
				for k2, v2 := c2.First(); k2 != nil; k2, v2 = c2.Next() {
					if v2 != nil && !yield(k2) {
						return
					}
				}
			}
		}
	}, nil
}

func (idx *indexStorage) toKey(keyParts []any) ([]byte, error) {
	keyChunks := make([]byte, 0)
	for _, part := range keyParts {
		partBytes, err := idx.maUn.Marshal(part)
		if err != nil {
			return nil, err
		}
		keyChunks = append(keyChunks, partBytes...)
	}
	return keyChunks, nil
}
