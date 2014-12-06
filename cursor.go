package bmdb

import (
	"github.com/missionMeteora/bmdb/mdb"
)

type Cursor mdb.Cursor

func (c *Cursor) Close() error {
	return (*mdb.Cursor)(c).Close()
}

func (c *Cursor) First() (key, val []byte) {
	key, val, _ = (*mdb.Cursor)(c).Get(nil, nil, mdb.FIRST)
	return
}

func (c *Cursor) Last() (key, val []byte) {
	key, val, _ = (*mdb.Cursor)(c).Get(nil, nil, mdb.LAST)
	return
}

func (c *Cursor) Next() (key, val []byte) {
	key, val, _ = (*mdb.Cursor)(c).Get(nil, nil, mdb.NEXT)
	return
}

func (c *Cursor) Prev() (key, val []byte) {
	key, val, _ = (*mdb.Cursor)(c).Get(nil, nil, mdb.PREV)
	return
}
