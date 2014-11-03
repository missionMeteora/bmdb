package bmdb

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
)

var (
	db   *DB
	path string
)

func init() {
	var err error
	path, err = ioutil.TempDir("/tmp", "bmdb_test")
	if err != nil {
		panic("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0700)
	if err != nil {
		panic("Cannot create directory: " + path)
	}
	db, err = Open(path, 0600, nil)
	if err != nil {
		panic(err)
	}
}
func TestAccess(t *testing.T) {
	err := db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket("coooookie")
		if err != nil {
			return err
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}

		for i := byte(0); i < 10; i++ {
			if err := b.Put(append([]byte("foo-"), 48+i), append([]byte("bar-"), 48+i)); err != nil {
				return err
			}

		}
		return nil
	})
	if logIf(err, t.Fatalf, "db.Update error = %v", err) {
		return
	}
	err = db.View(func(tx *Tx) error {
		foo, err := tx.Bucket("coooookie").Get([]byte("foo"))
		if err != nil {
			return err
		}
		t.Logf("foo = %s", foo)
		return nil
	})
	logIf(err, t.Fatalf, "db.View error = %v")
}

func TestForEach(t *testing.T) {
	err := db.View(func(tx *Tx) error {
		b := tx.Bucket("coooookie")
		if b == nil {
			return errors.New("couldn't load bucket")
		}
		b.ForEach(func(k, v []byte) error {
			t.Logf("%s = %s", k, v)
			return nil
		})
		return nil
	})
	logIf(err, t.Fatalf, "error = %v")
}

func TestReverseCursor(t *testing.T) {
	err := db.View(func(tx *Tx) error {
		b := tx.Bucket("coooookie")
		if b == nil {
			return errors.New("couldn't load bucket")
		}
		cur, err := b.Cursor()
		if logIf(err, t.Fatalf, "b.Cursor error = %v") {
			return err
		}
		for k, v := cur.Last(); k != nil; k, v = cur.Prev() {
			t.Logf("%s = %s", k, v)
		}
		return nil
	})
	logIf(err, t.Fatalf, "error = %v")
}

func TestZCleanup(t *testing.T) {
	if db != nil {
		db.Close()
		os.RemoveAll(path)
	}
}
func logIf(err error, lf func(string, ...interface{}), fmt string, args ...interface{}) bool {
	if err != nil {
		lf(fmt, append(args, err))
	}
	return err != nil
}
