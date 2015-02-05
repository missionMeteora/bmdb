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

var dbPath string
var kind string

func init() {
	mflag.StringVar(&kind, []string{"t", "-type"}, "bytes", "value interpretation (bytes, string, int, float)")
	mflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <path to db>\n", os.Args[0])
		mflag.PrintDefaults()
	}
	mflag.Parse()
	dbPath = mflag.Arg(0)
	if len(dbPath) == 0 {
		mflag.Usage()
		os.Exit(1)
	}
}

func main() {
	db, err := bmdb.Open(dbPath, 0600, &bmdb.Options{})
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
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
}

func printValue(n int64, name, k, v []byte) {
	var value string
	switch kind {
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
