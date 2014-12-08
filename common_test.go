package bmdb

import (
	"fmt"
	"path/filepath"
	"sync"
)

const (
	TEST_DIR = "tmp"
)

var (
	FOO = []byte("foo")
	BAR = []byte("bar")
)

// var r = rand.New(rand.Seed(time.Now().UnixNano()))

// func getRand() int {
// 	n := 1000000
// 	return r.Intn(n*9) + n
// }

var (
	testMux sync.Mutex
	testDBI int
)

func getID() int {
	testMux.Lock()
	testDBI++
	testMux.Unlock()
	return testDBI
}

func getDB() (*DB, error) {
	path := filepath.Join(TEST_DIR, fmt.Sprintf("%04d.db", getID()))
	return Open(path, 0644, nil)
}
