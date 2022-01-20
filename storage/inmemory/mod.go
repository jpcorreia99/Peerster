package inmemory

import (
	"sync"

	"go.dedis.ch/cs438/storage"
)

// NewPersistency return a new initialized in-memory storage. Opeartions are
// thread-safe with a global mutex.
func NewPersistency() storage.Storage {
	return Storage{
		blob:       newStore(),
		naming:     newStore(),
		blockchain: newStore(),
	}
}

// Storage implements an in-memory storage.
//
// - implements storage.Storage
type Storage struct {
	blob       storage.Store
	naming     storage.Store
	blockchain storage.Store
}

// GetDataBlobStore implements storage.Storage
func (s Storage) GetDataBlobStore() storage.Store {
	return s.blob
}

// GetNamingStore implements storage.Storage
func (s Storage) GetNamingStore() storage.Store {
	return s.naming
}

// GetBlockchainStore implements storage.Storage
func (s Storage) GetBlockchainStore() storage.Store {
	return s.blockchain
}

func newStore() *store {
	return &store{
		data: make(map[string][]byte),
	}
}

// store implements an in-memory store.
//
// - implements storage.Store
type store struct {
	sync.Mutex
	data map[string][]byte
}

// Get implements storage.Store
func (s *store) Get(key string) (val []byte) {
	s.Lock()
	defer s.Unlock()

	return s.data[string(key)]
}

// Set implements storage.Store
func (s *store) Set(key string, val []byte) {
	s.Lock()
	defer s.Unlock()

	s.data[string(key)] = val
}

// Delete implements storage.Store
func (s *store) Delete(key string) {
	s.Lock()
	defer s.Unlock()

	delete(s.data, string(key))
}

// ForEach implements storage.Store
func (s *store) ForEach(f func(key string, val []byte) bool) {
	s.Lock()
	defer s.Unlock()

	for k, v := range s.data {
		cont := f(k, v)
		if !cont {
			return
		}
	}
}

// Len implements storage.Store
func (s *store) Len() int {
	s.Lock()
	defer s.Unlock()

	return len(s.data)
}
