package bmdb

import (
	"github.com/szferi/gomdb"
)

func (t *Tx) CreateBucket(name string) (*Bucket, error) {
	dbi, err := t.txn.DBIOpen(&name, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	return &Bucket{dbi, t}, nil
}

func (t *Tx) Bucket(name string) *Bucket {
	dbi, err := t.txn.DBIOpen(&name, 0)
	if err != nil {
		return nil
	}
	return &Bucket{dbi, t}
}

type Bucket struct {
	dbi mdb.DBI
	tx  *Tx
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	return b.tx.txn.Get(b.dbi, key)

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
