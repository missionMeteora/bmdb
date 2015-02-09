package bmdb

import "sync"

var (
	// A global protected registry for
	// all the active databases.
	mux       sync.RWMutex
	activeDBs map[*DB]struct{}
)

func init() {
	activeDBs = make(map[*DB]struct{}, registryMapCap)
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
	mux.Lock()
	defer mux.Unlock()
	for db := range activeDBs {
		db.close()
	}
	activeDBs = nil
}

// ActiveCount gets the count of currently opened DBs.
func ActiveCount() int {
	mux.RLock()
	n := len(activeDBs)
	mux.RUnlock()
	return n
}
