package bmdb

// Put sets the value for a key in the default bucket.
// If the key exist then its previous value will be overwritten.
// Returns an error if the bucket was created from a read-only transaction,
// if the key is too large, or if the value is too large.
func (tx *Tx) Put(key, value []byte) error {
	if tx.done {
		return ErrTxDone
	} else if !tx.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}
	b, err := tx.CreateBucketIfNotExists(DefaultBucketName)
	if err != nil {
		return err
	}
	return b.Put(key, value)
}

// Get retrieves the value for a key in the default bucket.
// Returns a nil value if the key does not exist or the transaction is done.
func (tx *Tx) Get(key []byte) []byte {
	if tx.done {
		return nil
	}
	b := tx.Bucket(DefaultBucketName)
	if b == nil {
		return nil
	}
	return b.Get(key)
}

func (b *Bucket) Exists(key []byte) bool {
	if b.tx.done {
		return false
	}
	_, err := b.tx.txn.Get(b.dbi, key)
	if err != nil {
		return false
	}
	return true
}
