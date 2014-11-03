package bmdb

import (
	"errors"

	"github.com/szferi/gomdb"
)

const DefaultBucket = "default"

var (
	ErrManaged  = errors.New("cannot commit a managed transaction")
	ErrReadOnly = errors.New("read-only transaction")
)

type Tx struct {
	db        *DB
	txn       *mdb.Txn
	listeners []func()
	managed   bool
	rw        bool
}

// Put puts the key/val pair into the default bucket
func (tx *Tx) Put(key, val []byte) error {
	if !tx.rw {
		return ErrReadOnly
	}
	b, err := tx.CreateBucket(DefaultBucket)
	if err != nil {
		return err
	}
	return b.Put(key, val)
}

func (tx *Tx) OnCommit(fn func()) {
	tx.listeners = append(tx.listeners, fn)
}

func (tx *Tx) Rollback() error {
	if tx.managed {
		return ErrManaged
	}
	go removeCloser(tx)
	tx.txn.Abort()
	return nil
}

func (tx *Tx) Commit() (err error) {
	if tx.managed {
		return ErrManaged
	}
	go removeCloser(tx)
	if err = tx.txn.Commit(); err == nil {
		for _, fn := range tx.listeners {
			fn()
		}
	}
	return
}

// Close is an alias for Rollback, used internally for the crash handler
func (tx *Tx) Close() error {
	return tx.Rollback()
}
