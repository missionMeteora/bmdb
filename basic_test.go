package bmdb

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestAccess(t *testing.T) {
	path, err := ioutil.TempDir("/tmp", "bmdb_test")
	if err != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0700)
	defer os.RemoveAll(path)
	if err != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	db, err := Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		b, err := tx.CreateBucket("coooookie")
		if err != nil {
			return err
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
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

func logIf(err error, lf func(string, ...interface{}), fmt string, args ...interface{}) bool {
	if err != nil {
		lf(fmt, append(args, err))
	}
	return err != nil
}
