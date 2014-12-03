package bmdb

import (
	"errors"

	"github.com/szferi/gomdb"
)

var (
	NotImplemented    = errors.New("not implemented")
	ErrBucketNotFound = errors.New("bucket not found")
)

func (t *Tx) CreateBucket(name []byte) (*Bucket, error) {
	n := string(name)
	dbi, err := t.txn.DBIOpen(&n, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	return &Bucket{dbi, t}, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	// FIXME
	return nil, NotImplemented
	// return tx.CreateBucket(name)
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	// FIXME
	// b, _ := tx.CreateBucket(name)
	return nil
}

type Bucket struct {
	dbi mdb.DBI
	tx  *Tx
}

func (b *Bucket) Get(key []byte) []byte {
	v, err := b.tx.txn.Get(b.dbi, key)
	if err != nil {
		return nil
	}
	return v
}

func (b *Bucket) Put(key, val []byte) error {
	if !b.tx.rw {
		return ErrReadOnly
	}
	return b.tx.txn.Put(b.dbi, key, val, 0)
}

func (b *Bucket) Delete(key []byte) error {
	if !b.tx.rw {
		return ErrReadOnly
	}
	return b.tx.txn.Del(b.dbi, key, nil)
}

func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Drop deletes the bucket, if fromEnv is true it will also delete it from the environment and close the handle.
func (b *Bucket) Drop(fromEnv bool) error {
	if fromEnv {
		// 1 to delete the DB from the environment and close the handle.
		return b.tx.txn.Drop(b.dbi, 1)
	}
	// 0 to empty the DB.
	return b.tx.txn.Drop(b.dbi, 0)
}

func (b *Bucket) Stats() (*mdb.Stat, error) {
	return b.tx.txn.Stat(b.dbi)
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	cur, err := b.Cursor()
	if err != nil {
		return err
	}
	defer cur.Close()
	for {
		k, v := cur.Next()
		if k == nil {
			break
		}
		if err = fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
