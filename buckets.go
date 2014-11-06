package bmdb

import (
	"errors"
	"log"

	"github.com/szferi/gomdb"
)

var (
	ErrBucketNotFound = errors.New("bucket not found")
)

func (t *Tx) CreateBucket(name []byte) (*Bucket, error) {
	n := string(name)
	dbi, err := t.txn.DBIOpen(&n, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	b := &Bucket{dbi, t}
	//closeOnCrash(b.Close)
	return b, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.CreateBucket(name)
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	b, err := tx.CreateBucket(name)
	if err != nil {
		log.Printf("bucket (%s) error = %v", name, err)
	}
	return b
}

type Bucket struct {
	dbi mdb.DBI
	tx  *Tx
}

func (b *Bucket) Get(key []byte) (v []byte) {
	v, _ = b.tx.txn.Get(b.dbi, key)
	return
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

/*
this is not needed
func (b *Bucket) Close() error {
	go removeCloser(b.Close)
	b.tx.db.env.DBIClose(b.dbi)
	return nil
}
*/
// Drop deletes this bucket, if fromEnv is true it will also delete it from the environment and close the db handle.
func (b *Bucket) Drop(fromEnv bool) error {
	// 0 to empty the DB, 1 to delete it from the environment and close the DB handle.
	del := 0
	if fromEnv {
		del = 1
	}
	return b.tx.txn.Drop(b.dbi, del)
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
