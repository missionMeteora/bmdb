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
