package bmdb

import (
	"errors"

	"github.com/missionMeteora/bmdb/mdb"
)

const registryMapCap = 128

const (
	// MaxNameLength is the maximum length of a bucket name, in bytes.
	MaxNameLength = 64
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768
	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = 4294967295
)

var DefaultBucketName = []byte("default")

var (
	ErrKeyTooLarge     = errors.New("key is too large")
	ErrValueTooLarge   = errors.New("value is too large")
	ErrBucketExists    = errors.New("bucket already exists")
	ErrNameTooLong     = errors.New("bucket name is too long")
	ErrNoBucketName    = errors.New("no bucket name provided")
	ErrBucketNotFound  = errors.New("bucket not found")
	ErrKeyRequired     = errors.New("key is required")
	ErrTxManaged       = errors.New("this transaction is managed")
	ErrTxDone          = errors.New("this transaction is done")
	ErrDatabaseNotOpen = errors.New("database not open")
	ErrTxNotWritable   = errors.New("read-only transaction")
)

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
	//
	REVERSEKEY EnvFlag = mdb.REVERSEKEY // use reverse string keys
	DUPSORT    EnvFlag = mdb.DUPSORT    // use sorted duplicates
	INTEGERKEY EnvFlag = mdb.INTEGERKEY // numeric keys in native byte order. The keys must all be of the same size.
	DUPFIXED   EnvFlag = mdb.DUPFIXED   // with DUPSORT, sorted dup items have fixed size
	INTEGERDUP EnvFlag = mdb.INTEGERDUP // with DUPSORT, dups are numeric in native byte order
	REVERSEDUP EnvFlag = mdb.REVERSEDUP // with DUPSORT, use reverse string dups
	CREATE     EnvFlag = mdb.CREATE     // create DB if not already existing
)
