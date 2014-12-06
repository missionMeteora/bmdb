package bmdb

import "sync"

var (
	// A global protected registry for
	// all the active databases.
	mux       sync.RWMutex
	activeDBs map[*DB]struct{}
)

func init() {
	activeDBs = make(map[*DB]struct{}, 32)
}

func registerDB(db *DB) {
	mux.Lock()
	activeDBs[db] = struct{}{}
	mux.Unlock()
}

func unregisterDB(db *DB) {
	mux.Lock()
	delete(activeDBs, db)
	mux.Unlock()
}

// Finalize gracefully aborts all the active transactions and closes all the active databases.
// It must be called before the application exits, despite the cause of its exit.
func Finalize() {
	mux.RLock()
	defer mux.RUnlock()
	for db := range activeDBs {
		db.Close()
	}
}
