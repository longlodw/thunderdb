package thunderdb

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

func (d *dataStorage) insert(value map[string]any) ([8]byte, error) {
	if len(value) != len(d.fields) {
		return [8]byte{}, ErrFieldCountMismatch(len(d.fields), len(value))
	}
	id, err := d.bucket.NextSequence()
	if err != nil {
		return [8]byte{}, err
	}
	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], id)
	valueBytes, err := d.maUn.Marshal(value)
	if err != nil {
		return idBytes, err
	}
	return idBytes, d.bucket.Put(idBytes[:], valueBytes)
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
			idFixed := [8]byte{}
			copy(idFixed[:], k)
			if !yield(entry{
				value: value,
				id:    idFixed,
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
	id    [8]byte
	value map[string]any
}
