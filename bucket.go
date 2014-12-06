package bmdb

import "github.com/missionMeteora/bmdb/mdb"

type Bucket struct {
	dbi mdb.DBI
	tx  *Tx
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
func (b *Bucket) Cursor() (*Cursor, error) {
	if b.tx.done {
		return nil, ErrTxDone
	}
	c, err := b.tx.txn.CursorOpen(b.dbi)
	if err != nil {
		return nil, err
	}
	if !b.tx.Writable() {
		b.tx.registerCursor((*Cursor)(c))
	}
	return (*Cursor)(c), nil
}

// Get retrieves the value for a key in the bucket. Returns a nil value if the key does not exist or
// the transaction is done.
func (b *Bucket) Get(key []byte) []byte {
	if b.tx.done {
		return nil
	}
	v, err := b.tx.txn.Get(b.dbi, key)
	if err != nil {
		return nil
	}
	return v
}

func (b *Bucket) Put(key, val []byte) error {
	if b.tx.done {
		return ErrTxDone
	} else if !b.tx.Writable() {
		return ErrTxNotWritable
	}
	return b.tx.txn.Put(b.dbi, key, val, 0)
}

func (b *Bucket) Delete(key []byte) error {
	if b.tx.done {
		return ErrTxDone
	} else if !b.tx.Writable() {
		return ErrTxNotWritable
	}
	return b.tx.txn.Del(b.dbi, key, nil)
}

func (b *Bucket) Tx() *Tx {
	return b.tx
}

func (b *Bucket) Stats() (*mdb.Stat, error) {
	return b.tx.txn.Stat(b.dbi)
}

func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	c, err := b.Cursor()
	if err != nil {
		return err
	}
	defer c.Close()
	for {
		k, v := c.Next()
		if k == nil {
			break
		}
		if err = fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
