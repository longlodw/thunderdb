package thunderdb

import (
	"bytes"
	"encoding/binary"
	"iter"

	"github.com/openkvlab/boltdb"
	boltdb_errors "github.com/openkvlab/boltdb/errors"
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
	_, err = bucket.CreateBucketIfNotExists([]byte("ids"))
	if err != nil {
		return nil, err
	}
	valuesBucket, err := bucket.CreateBucketIfNotExists([]byte("values"))
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		_, err := valuesBucket.CreateBucketIfNotExists([]byte(field))
		if err != nil {
			return nil, err
		}
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
	idsBucket := d.bucket.Bucket([]byte("ids"))
	if idsBucket == nil {
		return [8]byte{}, boltdb_errors.ErrBucketNotFound
	}
	valuesBucket := d.bucket.Bucket([]byte("values"))
	if valuesBucket == nil {
		return [8]byte{}, boltdb_errors.ErrBucketNotFound
	}
	var idBytes [8]byte
	binary.BigEndian.PutUint64(idBytes[:], id)
	for _, field := range d.fields {
		fieldBucket := valuesBucket.Bucket([]byte(field))
		if fieldBucket == nil {
			return [8]byte{}, ErrFieldNotFound(field)
		}
		fieldValue, ok := value[field]
		if !ok {
			fieldValue = nil
		}
		fieldValueBytes, err := d.maUn.Marshal(fieldValue)
		if err != nil {
			return [8]byte{}, err
		}
		// idBytes as key
		err = fieldBucket.Put(idBytes[:], fieldValueBytes)
		if err != nil {
			return [8]byte{}, err
		}
	}
	return idBytes, idsBucket.Put(idBytes[:], nil)
}

func (d *dataStorage) update(id []byte, value map[string]any) error {
	valuesBucket := d.bucket.Bucket([]byte("values"))
	if valuesBucket == nil {
		return boltdb_errors.ErrBucketNotFound
	}
	for field, v := range value {
		fieldBucket := valuesBucket.Bucket([]byte(field))
		if fieldBucket == nil {
			return ErrFieldNotFound(field)
		}
		fieldValueBytes, err := d.maUn.Marshal(v)
		if err != nil {
			return err
		}
		// idBytes as key
		err = fieldBucket.Put(id, fieldValueBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *dataStorage) get(kr *keyRange) (iter.Seq2[*persistentRow, error], error) {
	idsBucket := d.bucket.Bucket([]byte("ids"))
	if idsBucket == nil {
		return nil, boltdb_errors.ErrBucketNotFound
	}
	return func(yield func(*persistentRow, error) bool) {
		c := idsBucket.Cursor()
		lessThan := func(k []byte) bool {
			if kr.endKey == nil {
				return true
			}
			cmp := bytes.Compare(k, kr.endKey)
			return cmp < 0 || (cmp == 0 && kr.includeEnd)
		}
		var k, _ []byte
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
			idFixed := [8]byte{}
			copy(idFixed[:], k)
			if !yield(newPersistentRow(d.bucket, d.maUn, d.fields, idFixed), nil) {
				return
			}
		}
	}, nil
}

func (d *dataStorage) delete(id []byte) error {
	valuesBucket := d.bucket.Bucket([]byte("values"))
	if valuesBucket == nil {
		return boltdb_errors.ErrBucketNotFound
	}
	idsBucket := d.bucket.Bucket([]byte("ids"))
	if idsBucket == nil {
		return boltdb_errors.ErrBucketNotFound
	}
	for _, field := range d.fields {
		fieldBucket := valuesBucket.Bucket([]byte(field))
		if fieldBucket == nil {
			return ErrFieldNotFound(field)
		}
		c := fieldBucket.Cursor()
		prefix := id
		for k, _ := c.Seek(prefix); k != nil && bytes.HasSuffix(k, prefix); k, _ = c.Next() {
			err := fieldBucket.Delete(k)
			if err != nil {
				return err
			}
		}
	}
	return idsBucket.Delete(id)
}
