package thunder

import (
	"bytes"
	"encoding/binary"
	"iter"

	"github.com/openkvlab/boltdb"
)

type dataStorage struct {
	bucket *boltdb.Bucket
	fields []string
	maUn   MarshalUnmarshaler
}

func newData(
	parentBucket *boltdb.Bucket,
	fields []string,
	maUn MarshalUnmarshaler,
) (*dataStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("data"))
	if err != nil {
		return nil, err
	}
	return &dataStorage{
		bucket: bucket,
		fields: fields,
		maUn:   maUn,
	}, nil
}

func loadData(
	parentBucket *boltdb.Bucket,
	fields []string,
	maUn MarshalUnmarshaler,
) (*dataStorage, error) {
	bucket := parentBucket.Bucket([]byte("data"))
	if bucket == nil {
		return nil, nil
	}
	return &dataStorage{
		bucket: bucket,
		fields: fields,
		maUn:   maUn,
	}, nil
}

func (d *dataStorage) insert(value map[string]any) (uint64, error) {
	if len(value) != len(d.fields) {
		return 0, ErrObjectFieldCountMismatch
	}
	id, err := d.bucket.NextSequence()
	if err != nil {
		return 0, err
	}
	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], id)
	valueBytes, err := d.maUn.Marshal(value)
	if err != nil {
		return 0, err
	}
	return id, d.bucket.Put(idBytes[:], valueBytes)
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
		var k, v []byte
		if kr.startKey != nil {
			k, v = c.Seek(kr.startKey)
		} else {
			k, v = c.First()
		}
		if !kr.includeStart {
			k, v = c.Next()
		}
		for ; k != nil && lessThan(k); k, v = c.Next() {
			if !kr.contains(k) {
				continue
			}
			var value map[string]any
			if err := d.maUn.Unmarshal(v, &value); err != nil {
				if !yield(entry{}, err) {
					return
				}
				continue
			}
			id := binary.BigEndian.Uint64(k)
			if !yield(entry{
				value: value,
				id:    id,
			}, nil) {
				return
			}
		}
	}, nil
}

func (d *dataStorage) delete(id []byte) error {
	return d.bucket.Delete(id)
}

type entry struct {
	id    uint64
	value map[string]any
}
