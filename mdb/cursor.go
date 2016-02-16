package mdb

/*
#cgo CFLAGS: -pthread -W -Wall -Wno-unused-parameter -Wbad-function-cast -O2 -g
#cgo freebsd CFLAGS: -DMDB_DSYNC=O_SYNC
#cgo openbsd CFLAGS: -DMDB_DSYNC=O_SYNC
#cgo netbsd CFLAGS: -DMDB_DSYNC=O_SYNC
#include <stdlib.h>
#include <stdio.h>
#include "lmdb.h"

#define LMDBGO_SET_VAL(val, size, data) *(val) = (MDB_val){.mv_size = (size), .mv_data = (data)}

static int lmdbgo_mdb_cursor_put2(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, unsigned int flags) {
    MDB_val key, val;
    LMDBGO_SET_VAL(&key, kn, kdata);
    LMDBGO_SET_VAL(&val, vn, vdata);
    return mdb_cursor_put(cur, &key, &val, flags);
}

static int lmdbgo_mdb_cursor_get2(MDB_cursor *cur, void *kdata, size_t kn, void *vdata, size_t vn, MDB_val *key, MDB_val *val, MDB_cursor_op op) {
    LMDBGO_SET_VAL(key, kn, kdata);
    LMDBGO_SET_VAL(val, vn, vdata);
    return mdb_cursor_get(cur, key, val, op);
}

*/
import "C"

import (
	"errors"
)

// MDB_cursor_op
const (
	FIRST = iota
	FIRST_DUP
	GET_BOTH
	GET_RANGE
	GET_CURRENT
	GET_MULTIPLE
	LAST
	LAST_DUP
	NEXT
	NEXT_DUP
	NEXT_MULTIPLE
	NEXT_NODUP
	PREV
	PREV_DUP
	PREV_NODUP
	SET
	SET_KEY
	SET_RANGE
)

func (cursor *Cursor) Close() error {
	if cursor._cursor == nil {
		return errors.New("Cursor already closed")
	}
	C.mdb_cursor_close(cursor._cursor)
	cursor._cursor = nil
	return nil
}

func (cursor *Cursor) Txn() *Txn {
	var _txn *C.MDB_txn
	_txn = C.mdb_cursor_txn(cursor._cursor)
	if _txn != nil {
		return &Txn{_txn}
	}
	return nil
}

func (cursor *Cursor) DBI() DBI {
	var _dbi C.MDB_dbi
	_dbi = C.mdb_cursor_dbi(cursor._cursor)
	return DBI(_dbi)
}

// Retrieves the low-level MDB cursor.
func (cursor *Cursor) MdbCursor() *C.MDB_cursor {
	return cursor._cursor
}

func (cursor *Cursor) Get(set_key, sval []byte, op uint) (key, val []byte, err error) {
	k, v, err := cursor.GetVal(set_key, sval, op)
	if err != nil {
		return nil, nil, err
	}
	return k.Bytes(), v.Bytes(), nil
}

func (cursor *Cursor) GetVal(inkey, inval []byte, op uint) (*Val, *Val, error) {
	key := new(C.MDB_val)
	val := new(C.MDB_val)
	kdata, kn := valBytes(inkey)
	vdata, vn := valBytes(inval)
	ret := C.lmdbgo_mdb_cursor_get2(
		cursor._cursor,
		kdata, C.size_t(kn),
		vdata, C.size_t(vn),
		key, val,
		C.MDB_cursor_op(op),
	)
	return (*Val)(key), (*Val)(val), errno(ret)
}

func (cursor *Cursor) Put(key, val []byte, flags uint) error {
	kdata, kn := valBytes(key)
	vdata, vn := valBytes(val)
	ret := C.lmdbgo_mdb_cursor_put2(
		cursor._cursor,
		kdata, C.size_t(kn),
		vdata, C.size_t(vn),
		C.uint(flags),
	)
	return errno(ret)
}

func (cursor *Cursor) Del(flags uint) error {
	ret := C.mdb_cursor_del(cursor._cursor, C.uint(flags))
	return errno(ret)
}

func (cursor *Cursor) Count() (uint64, error) {
	var _size C.size_t
	ret := C.mdb_cursor_count(cursor._cursor, &_size)
	if ret != SUCCESS {
		return 0, errno(ret)
	}
	return uint64(_size), nil
}
