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

type Options struct {
	Flags      EnvFlag
	MapSize    uint64
	MaxReaders uint
	MaxBuckets uint
	NoSync     bool
}

var defaultOptions = &Options{
	MapSize:    10 * 1024 * 1024, // 10 MB
	MaxBuckets: 8096,             // TODO: study caveats
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
	if opts.NoSync {
		opts.Flags |= mdb.NOSYNC | mdb.NOMETASYNC | mdb.WRITEMAP | mdb.MAPASYNC
	}
	return opts
}

// Open creates and opens a database at the given path.
// If the directory does not exist then it will be created automatically.
// Passing in nil options will cause BMDB to open the database with the default options.
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

	if err := os.MkdirAll(path, 0755); err != nil {
		env.Close()
		return nil, err
	}
	if err = env.Open(path, uint(opts.Flags), uint(mode)); err != nil {
		env.Close()
		return nil, err
	}
	db := &DB{
		path:         path,
		env:          env,
		opts:         opts,
		transactions: make(map[*Tx]struct{}, registryMapCap),
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
	db.closed = true
	mux.Lock()
	defer mux.Unlock()
	db.mux.Lock()
	defer db.mux.Unlock()
	for tx := range db.transactions {
		tx.close()
	}
	db.transactions = nil
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
	tx := &Tx{
		txn:      txn,
		writable: writable,
		cursors:  make(map[*Cursor]struct{}, registryMapCap),
	}
	db.registerTransaction(tx)
	tx.closeCallback = func() {
		if db.closed {
			return
		}
		db.unregisterTransaction(tx)
	}
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
	// db.registerTransaction(tx)
	// defer db.unregisterTransaction(tx)
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
	// db.registerTransaction(tx)
	// defer db.unregisterTransaction(tx)
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
	if db.closed {
		return
	}
	mux.Lock()
	db.transactions[tx] = struct{}{}
	mux.Unlock()
}

func (db *DB) unregisterTransaction(tx *Tx) {
	if db.closed {
		return
	}
	mux.Lock()
	delete(db.transactions, tx)
	mux.Unlock()
}

func (db *DB) activeTransactionsCount() int {
	return len(db.transactions)
}
