package bmdb

import (
	"github.com/szferi/gomdb"
)

type Cursor struct {
	cur *mdb.Cursor
}

func (b *Bucket) Cursor() (*Cursor, error) {
	c, err := b.tx.txn.CursorOpen(b.dbi)
	if err != nil {
		return nil, err
	}
	return &Cursor{c}, nil
}

func (c *Cursor) Close() error {
	c.cur.Close()
	return nil
}

func (c *Cursor) First() (key, val []byte) {
	key, val, _ = c.cur.Get(nil, nil, mdb.FIRST)
	return
}
func (c *Cursor) Last() (key, val []byte) {
	key, val, _ = c.cur.Get(nil, nil, mdb.LAST)
	return
}

func (c *Cursor) Next() (key, val []byte) {
	key, val, _ = c.cur.Get(nil, nil, mdb.NEXT)
	return
}
func (c *Cursor) Prev() (key, val []byte) {
	key, val, _ = c.cur.Get(nil, nil, mdb.PREV)
	return
}
