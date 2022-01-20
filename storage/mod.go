package storage

// LastBlockKey defines the key in the blockchain store that stores the last
// blockchain block's hash.
const LastBlockKey = "0000000000000000000000000000000000000000000000000000000000000000"

// Storage describes the stores provided to the peer that must be used.
type Storage interface {
	// GetDataBlobStore returns a storage to store data blobs. The storage
	// must use either a metahash or a chunk's hash as key.
	GetDataBlobStore() Store

	// GetNamingStore returns a storage to store the names mapping. The
	// storage must use tags/filenames as key, and metahashes as values.
	GetNamingStore() Store

	// GetBlockchainStore returns a storage to store the blockchain blocks.
	GetBlockchainStore() Store
}

// Store describes the primitives of a simple storage.
type Store interface {
	// Get returns nil if not found
	Get(key string) (val []byte)

	Set(key string, val []byte)

	Delete(key string)

	Len() int

	// Calls the function on each key/value pair. Aborts if the function returns
	// false.
	ForEach(func(key string, val []byte) bool)
}
