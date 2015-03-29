package bmdb

import (
	"fmt"
	"sync"

	"github.com/missionMeteora/bmdb/mdb"
)

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
type Tx struct {
	db       *DB
	txn      *mdb.Txn
	managed  bool
	writable bool
	done     bool

	// closeCallback is used to notify the parent DB about a closed transaction
	closeCallback func()

	// A protected registry.
	mux            sync.RWMutex
	commitHandlers []func()
	cursors        map[*Cursor]struct{}
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if tx.done {
		return nil, ErrTxDone
	} else if len(name) == 0 {
		return nil, ErrNoBucketName
	} else if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}
	if b := tx.Bucket(name); b != nil {
		return nil, ErrBucketExists
	}
	n := string(name)
	dbi, err := tx.txn.DBIOpen(&n, mdb.CREATE)
	if err != nil {
		return nil, err
	}
	return &Bucket{dbi: dbi, tx: tx}, nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	if tx.done {
		return nil, ErrTxDone
	} else if !tx.Writable() {
		return nil, ErrTxNotWritable
	} else if len(name) == 0 {
		return nil, ErrNoBucketName
	} else if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}
	b := tx.Bucket(name)
	if b != nil {
		return b, nil
	}
	return tx.CreateBucket(name)
}

// Bucket retrieves a bucket by name. Returns nil if the bucket does not exist.
func (tx *Tx) Bucket(name []byte) *Bucket {
	if tx.done {
		return nil
	} else if len(name) == 0 {
		return nil
	} else if len(name) > MaxNameLength {
		return nil
	}
	// try to open an existing bucket
	n := string(name)
	dbi, err := tx.txn.DBIOpen(&n, 0)
	if err != nil {
		return nil
	}
	return &Bucket{dbi: dbi, tx: tx}
}

// BucketNames XXX.
func (tx *Tx) BucketNames() *Bucket {
	// try to open an existing bucket
	dbi, err := tx.txn.DBIOpen(nil, 0)
	if err != nil {
		return nil
	}
	return &Bucket{dbi: dbi, tx: tx}
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found or the provided name was incorrect.
func (tx *Tx) DeleteBucket(name []byte) error {
	if tx.done {
		return ErrTxDone
	} else if len(name) == 0 {
		return ErrNoBucketName
	} else if len(name) > MaxNameLength {
		return ErrNameTooLong
	} else if !tx.Writable() {
		return ErrTxNotWritable
	}
	b := tx.Bucket(name)
	if b != nil {
		return tx.txn.Drop(b.dbi, 1)
	}
	return ErrBucketNotFound
}

// OnCommit adds a handler function to be executed after the transaction successfully commits.
func (tx *Tx) OnCommit(fn func()) {
	tx.mux.Lock()
	tx.commitHandlers = append(tx.commitHandlers, fn)
	tx.mux.Unlock()
}

// Rollback closes the transaction and ignores all previous updates.
func (tx *Tx) Rollback() error {
	tx.mux.Lock()
	defer tx.mux.Unlock()
	if tx.managed {
		return ErrTxManaged
	} else if tx.done {
		return ErrTxDone
	}
	tx.done = true
	tx.txn.Abort()
	if !tx.Writable() {
		for c := range tx.cursors {
			c.Close()
		}
	}
	tx.cursors = nil
	tx.commitHandlers = nil
	if tx.closeCallback != nil {
		tx.closeCallback()
	}
	return nil
}

// Commit commits all the operations of a transaction into the database and writes to the disk.
// The transaction handle is freed. It and its cursors must not be used again after this call.
func (tx *Tx) Commit() error {
	tx.mux.Lock()
	defer tx.mux.Unlock()
	if tx.managed {
		return ErrTxManaged
	} else if tx.done {
		return ErrTxDone
	}
	tx.done = true
	err := tx.txn.Commit()
	if err != nil {
		fmt.Println("BMDB: error committing:", err)
	} else {
		for _, hdl := range tx.commitHandlers {
			hdl()
		}
	}
	if !tx.Writable() {
		for c := range tx.cursors {
			c.Close()
		}
	}
	tx.cursors = nil
	tx.commitHandlers = nil
	if tx.closeCallback != nil {
		tx.closeCallback()
	}
	return err
}

// Writable returns whether the transaction can perform write operations.
func (tx *Tx) Writable() bool {
	return tx.writable
}

func (tx *Tx) close() {
	if tx.done {
		return
	}
	tx.done = true
	tx.mux.Lock()
	defer tx.mux.Unlock()
	tx.txn.Abort()
	if !tx.Writable() {
		for c := range tx.cursors {
			c.Close()
		}
	}
	tx.cursors = nil
	if tx.closeCallback != nil {
		tx.closeCallback()
	}
}

func (tx *Tx) registerCursor(c *Cursor) {
	tx.mux.Lock()
	tx.cursors[c] = struct{}{}
	tx.mux.Unlock()
}

func (tx *Tx) activeCursorsCount() int {
	tx.mux.RLock()
	n := len(tx.cursors)
	tx.mux.RUnlock()
	return n
}
