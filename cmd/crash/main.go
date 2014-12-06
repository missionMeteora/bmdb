package main

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/missionMeteora/bmdb"
)

func main() {
	path, err := ioutil.TempDir("/tmp", "bmdb_test")
	if err != nil {
		panic("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0700)
	if err != nil {
		panic("Cannot create directory: " + path)
	}
	db, err := bmdb.Open(path, 0600, nil)
	if err != nil {
		panic(err)
	}
	db.Update(func(tx *bmdb.Tx) error {
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
	bmdb.OnExit(func() {
		log.Println("why cruel world, why.")
		os.RemoveAll(path)
	})
	time.Sleep(time.Hour * 24)
}
