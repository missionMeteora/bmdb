package bmdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test0(t *testing.T) {
	if !assert.NoError(t, os.RemoveAll(TEST_DIR)) {
		t.Fatalf("unable to remove the test dir: %s", TEST_DIR)
	}
}

type testFunc func(db *DB)

func testWrap(t *testing.T, fn testFunc) {
	assert := assert.New(t)
	dbCount := ActiveCount()
	db, err := getDB()
	if !assert.NoError(err) {
		return
	}
	fn(db) // run the test case
	assert.NoError(db.Close())
	assert.Equal(dbCount, ActiveCount())
}

func TestOpenClose(t *testing.T) {
	testWrap(t, func(*DB) {})
}

func TestBeginRollbackCommit(t *testing.T) {
	testWrap(t, func(db *DB) {
		assert := assert.New(t)
		txR1, err := db.Begin(false)
		if err != nil {
			assert.NoError(err)
			return
		} else if !assert.Equal(db.activeTransactionsCount(), 1) {
			return
		}
		txR2, err := db.Begin(false)
		if err != nil {
			assert.NoError(err)
			return
		} else if !assert.Equal(db.activeTransactionsCount(), 2) {
			return
		}
		txRW, err := db.Begin(true)
		if err != nil {
			assert.NoError(err)
			return
		} else if !assert.Equal(db.activeTransactionsCount(), 3) {
			return
		}
		if err = txR1.Rollback(); err != nil {
			assert.NoError(err)
			return
		} else if err = txR2.Commit(); err != nil {
			assert.NoError(err)
			return
		} else if !assert.Equal(db.activeTransactionsCount(), 1) {
			return
		}
		if err := txRW.Put(FOO, BAR); err != nil {
			assert.NoError(err)
			return
		} else if err := txRW.Commit(); err != nil {
			assert.NoError(err)
			return
		} else if !assert.Equal(db.activeTransactionsCount(), 0) {
			return
		}
		txR3, err := db.Begin(false)
		if err != nil {
			assert.NoError(err)
			return
		} else if !assert.NotNil(txR3.Bucket(DefaultBucketName)) {
			return
		} else if !assert.Equal(txR3.Bucket(DefaultBucketName).Get(FOO), BAR) {
			return
		}
	})
}
