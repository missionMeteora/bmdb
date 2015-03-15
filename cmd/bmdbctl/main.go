package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/missionMeteora/binny"
	"github.com/missionMeteora/bmdb"
	"gopkg.in/mflag.v1"
)

var (
	kind       = mflag.String([]string{"t", "-type"}, "bytes", "value interpretation (bytes, string, int, float)")
	dbPath     = mflag.String([]string{"d", "-db"}, "", "path to a BMDB database")
	action     string
	bucketName string
	key        string
	value      string
)

func init() {
	mflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <action> [bucket] [key value]\n", os.Args[0])
		mflag.PrintDefaults()
	}
	mflag.Parse()

	action = mflag.Arg(0)
	bucketName = mflag.Arg(1)
	key = mflag.Arg(2)
	value = mflag.Arg(3)

	if len(action) == 0 {
		mflag.Usage()
		os.Exit(1)
	}
	if action == "update" {
		if len(bucketName) == 0 || len(key) == 0 {
			mflag.Usage()
			os.Exit(1)
		}
	}
	if len(*dbPath) == 0 {
		mflag.Usage()
		os.Exit(1)
	}
}

func main() {
	db, err := bmdb.Open(*dbPath, 0600, &bmdb.Options{})
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	switch action {
	case "view":
		view(db)
	case "update":
		update(db)
	default:
		log.Fatalln("unknown action")
	}
}

func update(db *bmdb.DB) {
	if err := db.Update(func(tx *bmdb.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		v, _ := binny.Marshal(27)
		return b.Put([]byte(key), v)
	}); err != nil {
		log.Fatalln(err)
	}
}

func view(db *bmdb.DB) {
	if len(bucketName) == 0 {
		if err := db.View(func(tx *bmdb.Tx) error {
			names := tx.BucketNames()
			if names == nil {
				return errors.New("cannot reach the unnamed bucket")
			}
			return names.ForEach(func(name, _ []byte) error {
				var i int64
				return tx.Bucket(name).ForEach(func(k, v []byte) error {
					i++
					printValue(i, name, k, v)
					return nil
				})
			})
		}); err != nil {
			log.Fatalln(err)
		}
		return
	}
	if err := db.View(func(tx *bmdb.Tx) error {
		var i int64
		return tx.Bucket([]byte(bucketName)).ForEach(func(k, v []byte) error {
			i++
			printValue(i, []byte(bucketName), k, v)
			return nil
		})
	}); err != nil {
		log.Fatalln(err)
	}
}

func printValue(n int64, name, k, v []byte) {
	var value string
	switch *kind {
	case "string":
		value = string(v)
	case "int":
		var i int64
		if err := binny.Unmarshal(v, &i); err != nil {
			fmt.Println("error unmarshaling int64 value:", v, err)
		}
		value = strconv.FormatInt(i, 10)
	case "float":
		var f float32
		if err := binny.Unmarshal(v, &f); err != nil {
			fmt.Println("error unmarshaling float32 value:", v, err)
		}
		value = strconv.FormatFloat(float64(f), 'f', 4, 32)
	default:
		value = fmt.Sprint(v)
	}
	fmt.Printf("%s/%04d %s = %v\n", string(name), n, string(k), value)
}
