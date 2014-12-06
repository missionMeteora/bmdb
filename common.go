package bmdb

import "errors"

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
