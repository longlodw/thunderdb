package relations

import (
	"bytes"
	"errors"
	"iter"

	"github.com/longlodw/thunder"
	"github.com/openkvlab/boltdb"
)

type iBucketScanner interface {
	Scan(key []byte) iter.Seq2[[]byte, []byte]
}

func newBucketScanner(operator thunder.OpType, bucket *boltdb.Bucket) (iBucketScanner, error) {
	switch operator {
	case thunder.OpEq:
		return newEqBucketScanner(bucket), nil
	case thunder.OpNe:
		return newNeBucketScanner(bucket), nil
	case thunder.OpGt:
		return newGtBucketScanner(bucket), nil
	case thunder.OpLt:
		return newLtBucketScanner(bucket), nil
	case thunder.OpGe:
		return newGeBucketScanner(bucket), nil
	case thunder.OpLe:
		return newLeBucketScanner(bucket), nil
	default:
		return nil, errors.New("unsupported operator")
	}
}

type eqBucketScanner struct {
	bucket *boltdb.Bucket
}

func newEqBucketScanner(bucket *boltdb.Bucket) *eqBucketScanner {
	return &eqBucketScanner{
		bucket: bucket,
	}
}

func (s *eqBucketScanner) Scan(key []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		value := s.bucket.Get(key)
		if value != nil {
			yield(key, value)
		}
	}
}

type neBucketScanner struct {
	bucket *boltdb.Bucket
}

func newNeBucketScanner(bucket *boltdb.Bucket) *neBucketScanner {
	return &neBucketScanner{
		bucket: bucket,
	}
}

func (s *neBucketScanner) Scan(key []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := s.bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !bytes.Equal(k, key) {
				if !yield(k, v) {
					return
				}
			}
		}
	}
}

type gtBucketScanner struct {
	bucket *boltdb.Bucket
}

func newGtBucketScanner(bucket *boltdb.Bucket) *gtBucketScanner {
	return &gtBucketScanner{
		bucket: bucket,
	}
}

func (s *gtBucketScanner) Scan(key []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := s.bucket.Cursor()
		c.Seek(key)
		for k, v := c.Next(); k != nil; k, v = c.Next() {
			if !yield(k, v) {
				return
			}
		}
	}
}

type ltBucketScanner struct {
	bucket *boltdb.Bucket
}

func newLtBucketScanner(bucket *boltdb.Bucket) *ltBucketScanner {
	return &ltBucketScanner{
		bucket: bucket,
	}
}

func (s *ltBucketScanner) Scan(key []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := s.bucket.Cursor()
		c.Seek(key)
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			if !yield(k, v) {
				return
			}
		}
	}
}

type geBucketScanner struct {
	bucket *boltdb.Bucket
}

func newGeBucketScanner(bucket *boltdb.Bucket) *geBucketScanner {
	return &geBucketScanner{
		bucket: bucket,
	}
}

func (s *geBucketScanner) Scan(key []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := s.bucket.Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			if !yield(k, v) {
				return
			}
		}
	}
}

type leBucketScanner struct {
	bucket *boltdb.Bucket
}

func newLeBucketScanner(bucket *boltdb.Bucket) *leBucketScanner {
	return &leBucketScanner{
		bucket: bucket,
	}
}

func (s *leBucketScanner) Scan(key []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := s.bucket.Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Prev() {
			if !yield(k, v) {
				return
			}
		}
	}
}

type allBucketScanner struct {
	bucket *boltdb.Bucket
}

func newAllBucketScanner(bucket *boltdb.Bucket) *allBucketScanner {
	return &allBucketScanner{
		bucket: bucket,
	}
}

func (s *allBucketScanner) Scan(_ []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := s.bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !yield(k, v) {
				return
			}
		}
	}
}
