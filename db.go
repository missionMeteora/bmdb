package bmdb

import (
	"os"
	"sync"

	"github.com/missionMeteora/bmdb/mdb"
)

type DB struct {
	path   string
	env    *mdb.Env
	opts   *Options
	closed bool

	// A protected registry of transactions.
	mux          sync.Mutex
	transactions map[*Tx]struct{}
}

type EnvFlag uint

const (
	FIXEDMAP   EnvFlag = mdb.FIXEDMAP   // mmap at a fixed address (experimental)
	NOSUBDIR   EnvFlag = mdb.NOSUBDIR   // no environment directory
	NOSYNC     EnvFlag = mdb.NOSYNC     // don't fsync after commit
	RDONLY     EnvFlag = mdb.RDONLY     // read only
	NOMETASYNC EnvFlag = mdb.NOMETASYNC // don't fsync metapage after commit
	WRITEMAP   EnvFlag = mdb.WRITEMAP   // use writable mmap
	MAPASYNC   EnvFlag = mdb.MAPASYNC   // use asynchronous msync when MDB_WRITEMAP is use
	NOTLS      EnvFlag = mdb.NOTLS      // tie reader locktable slots to Txn objects instead of threads
)

type Options struct {
	Flags      EnvFlag
	MapSize    uint64
	MaxReaders uint
	MaxBuckets uint
}

var defaultOptions = &Options{
	MapSize:    1 << 20,
	MaxBuckets: 1 << 10, // this is extremely important, apparently..
}

func checkOpts(opts *Options) *Options {
	if opts == nil {
		return defaultOptions
	}
	if opts.MapSize == 0 {
		opts.MapSize = defaultOptions.MapSize
	}
	if opts.MaxBuckets == 0 {
		opts.MaxBuckets = defaultOptions.MaxBuckets
	}
	return opts
}

// Open opens a database.
func Open(path string, mode os.FileMode, opts *Options) (*DB, error) {
	opts = checkOpts(opts)
	env, err := mdb.NewEnv()
	if err != nil {
		return nil, err
	}
	if err = env.SetMapSize(opts.MapSize); err != nil {
		env.Close() // required by the MDB spec
		return nil, err
	}
	if opts.MaxReaders > 0 {
		if err = env.SetMaxReaders(opts.MaxReaders); err != nil {
			env.Close()
			return nil, err
		}
	}
	if err = env.SetMaxDBs(mdb.DBI(opts.MaxBuckets)); err != nil {
		env.Close()
		return nil, err
	}

	if err = env.Open(path, uint(opts.Flags), uint(mode)); err != nil {
		env.Close()
		return nil, err
	}
	db := &DB{
		path: path,
		env:  env,
		opts: opts,
	}
	registerDB(db)
	return db, nil
}

// Path returns the path to currently open database file.
func (db *DB) Path() string {
	return db.path
}

// Close releases all database resources. All transactions will be aborted.
func (db *DB) Close() error {
	defer unregisterDB(db)
	return db.close()
}

func (db *DB) close() error {
	if db.closed {
		return ErrDatabaseNotOpen
	}
	db.mux.Lock()
	defer db.mux.Unlock()
	for tx := range db.transactions {
		tx.close()
	}
	db.closed = true
	return db.env.Close()
}

// Begin starts a new transaction.
//
// IMPORTANT: You must close read-only transactions after you are finished.
func (db *DB) Begin(writable bool) (*Tx, error) {
	if db.closed {
		return nil, ErrDatabaseNotOpen
	}
	var flags uint
	if !writable {
		flags = mdb.RDONLY
	}
	txn, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, err
	}
	tx := &Tx{txn: txn, writable: writable}
	db.registerTransaction(tx)
	return tx, nil
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is returned from the Update() method.
func (db *DB) Update(fn func(*Tx) error) error {
	if db.closed {
		return ErrDatabaseNotOpen
	}
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	db.registerTransaction(tx)
	defer db.unregisterTransaction(tx)
	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// View executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
func (db *DB) View(fn func(*Tx) error) error {
	if db.closed {
		return ErrDatabaseNotOpen
	}
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	db.registerTransaction(tx)
	defer db.unregisterTransaction(tx)
	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (db *DB) registerTransaction(tx *Tx) {
	mux.Lock()
	db.transactions[tx] = struct{}{}
	mux.Unlock()
}

func (db *DB) unregisterTransaction(tx *Tx) {
	mux.Lock()
	delete(db.transactions, tx)
	mux.Unlock()
}
