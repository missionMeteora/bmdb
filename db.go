package bmdb

import (
	"github.com/szferi/gomdb"
)

type DB struct {
	env  *mdb.Env
	opts *Options
}

type Options struct {
	MapSize    uint64
	Flags      uint
	MaxReaders uint
	MaxDBs     uint
}

var defaultOptions = &Options{
	MapSize: 1 << 20,
	MaxDBs:  1, //this is extremely important, apparently..
}

func Open(path string, mode uint, opts *Options) (db *DB, err error) {
	if opts == nil {
		opts = defaultOptions
	}
	var env *mdb.Env
	if env, err = mdb.NewEnv(); err != nil {
		return
	}
	if err = env.SetMapSize(opts.MapSize); err != nil {
		return
	}
	if opts.MaxReaders > 0 {
		if err = env.SetMaxReaders(opts.MaxReaders); err != nil {
			return
		}
	}
	if err = env.SetMaxDBs(mdb.DBI(opts.MaxDBs)); err != nil {
		return
	}

	if err = env.Open(path, opts.Flags, mode); err != nil {
		return
	}
	db = &DB{
		env:  env,
		opts: opts,
	}
	closeOnCrash(db)
	return
}

func (db *DB) Close() error {
	go removeCloser(db)
	return db.env.Close()
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	var flags uint = mdb.RDONLY
	if writable {
		flags = 0
	}
	txn, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		return nil, err
	}
	return &Tx{txn: txn, rw: writable}, nil
}

func (db *DB) Update(fn func(*Tx) error) (err error) {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if perr, _ := recover().(error); err != nil {
			err = perr
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err == nil {
		err = tx.Commit()
	}
	return
}

func (db *DB) View(fn func(*Tx) error) (err error) {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	tx.managed = true
	err = fn(tx)
	tx.managed = false
	return
}
