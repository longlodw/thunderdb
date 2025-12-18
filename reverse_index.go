package thunder

import (
	"github.com/openkvlab/boltdb"
)

type reverseIndexStorage struct {
	bucket *boltdb.Bucket
	maUn   MarshalUnmarshaler
}

func newReverseIndex(
	parentBucket *boltdb.Bucket,
	maUn MarshalUnmarshaler,
) (*reverseIndexStorage, error) {
	bucket, err := parentBucket.CreateBucketIfNotExists([]byte("reverse_index"))
	if err != nil {
		return nil, err
	}
	return &reverseIndexStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func loadReverseIndex(
	parentBucket *boltdb.Bucket,
	maUn MarshalUnmarshaler,
) (*reverseIndexStorage, error) {
	bucket := parentBucket.Bucket([]byte("reverse_index"))
	if bucket == nil {
		return nil, nil
	}
	return &reverseIndexStorage{
		bucket: bucket,
		maUn:   maUn,
	}, nil
}

func (ridx *reverseIndexStorage) insert(id []byte, rev map[string][]byte) error {
	revData, err := ridx.maUn.Marshal(rev)
	if err != nil {
		return err
	}
	return ridx.bucket.Put(id, revData)
}

func (ridx *reverseIndexStorage) delete(id []byte) error {
	return ridx.bucket.Delete(id)
}

func (ridx *reverseIndexStorage) get(id []byte) (map[string][]byte, error) {
	data := ridx.bucket.Get(id)
	if data == nil {
		return nil, nil
	}
	var rev map[string][]byte
	err := ridx.maUn.Unmarshal(data, &rev)
	if err != nil {
		return nil, err
	}
	return rev, nil
}
