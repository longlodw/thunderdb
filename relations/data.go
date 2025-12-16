package relations

import (
	"github.com/longlodw/thunder"
	"github.com/openkvlab/boltdb"
)

type dataStorage struct {
	bucket *boltdb.Bucket
	maUn   thunder.MarshalUnmarshaler
}

func newData(
	parentBucket *boltdb.Bucket,
	maUn thunder.MarshalUnmarshaler,
) (*dataStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("data"))
	if err != nil {
		return nil, err
	}
	return &dataStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func (d *dataStorage) insert(value any) ([]byte, error) {
	id, err := d.bucket.NextSequence()
	if err != nil {
		return nil, err
	}
	idBytes, err := d.maUn.Marshal(id)
	if err != nil {
		return nil, err
	}
	dataBytes, err := d.maUn.Marshal(value)
	if err != nil {
		return nil, err
	}
	return idBytes, d.bucket.Put(idBytes, dataBytes)
}

func (d *dataStorage) get(id []byte) (map[string]any, error) {
	dataBytes := d.bucket.Get(id)
	if dataBytes == nil {
		return nil, nil
	}
	var value map[string]any
	if err := d.maUn.Unmarshal(dataBytes, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func (d *dataStorage) delete(id []byte) error {
	return d.bucket.Delete(id)
}

type entry struct {
	id    []byte
	value map[string]any
}
