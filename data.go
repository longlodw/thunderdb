package thunder

import (
	"bytes"
	"iter"

	"github.com/openkvlab/boltdb"
)

type dataStorage struct {
	bucket *boltdb.Bucket
	fields []string
}

func newData(
	parentBucket *boltdb.Bucket,
	fields []string,
) (*dataStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("data"))
	if err != nil {
		return nil, err
	}
	return &dataStorage{
		bucket: bucket,
		fields: fields,
	}, nil
}

func loadData(
	parentBucket *boltdb.Bucket,
	fields []string,
) (*dataStorage, error) {
	bucket := parentBucket.Bucket([]byte("data"))
	if bucket == nil {
		return nil, nil
	}
	return &dataStorage{
		bucket: bucket,
		fields: fields,
	}, nil
}

func (d *dataStorage) insert(value map[string][]byte) ([]byte, error) {
	if len(value) != len(d.fields) {
		return nil, ErrObjectFieldCountMismatch
	}
	id, err := d.bucket.NextSequence()
	if err != nil {
		return nil, err
	}
	idBytes, err := orderedMa.Marshal([]any{id})
	if err != nil {
		return nil, err
	}
	bck, err := d.bucket.CreateBucketIfNotExists(idBytes)
	if err != nil {
		return nil, err
	}
	for _, f := range d.fields {
		if v, ok := value[f]; !ok {
			return nil, ErrObjectMissingField(f)
		} else if err := bck.Put([]byte(f), v); err != nil {
			return nil, err
		}
	}
	return idBytes, nil
}

func (d *dataStorage) get(kr *keyRange) (iter.Seq2[entry, error], error) {
	return func(yield func(entry, error) bool) {
		c := d.bucket.Cursor()
		lessThan := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmp := bytes.Compare(k, kr.endKey)
			return cmp < 0 || (cmp == 0 && kr.includeEnd)
		}
		var k []byte
		if kr.startKey != nil {
			k, _ = c.Seek(kr.startKey)
		} else {
			k, _ = c.First()
		}
		if !kr.includeStart {
			k, _ = c.Next()
		}
		for ; k != nil && lessThan(k); k, _ = c.Next() {
			if !kr.contains(k) {
				continue
			}
			bck := d.bucket.Bucket(k)
			if bck == nil {
				if !yield(entry{}, ErrDataNotFound) {
					return
				}
				continue
			}
			value := make(map[string][]byte)
			for _, f := range d.fields {
				value[f] = bck.Get([]byte(f))
			}
			if !yield(entry{
				id:    k,
				value: value,
			}, nil) {
				return
			}
		}
	}, nil
}

func (d *dataStorage) delete(id []byte) error {
	return d.bucket.DeleteBucket(id)
}

type entry struct {
	id    []byte
	value map[string][]byte
}
