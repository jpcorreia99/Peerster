package file

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"go.dedis.ch/cs438/storage"
	"golang.org/x/xerrors"
)

const (
	blob       = "blob"
	naming     = "naming"
	blockchain = "blockchain"
)

// NewPersistency return a new initialized file-based storage. Opeartions are
// thread-safe with a global mutex.
func NewPersistency(folderPath string) (storage.Storage, error) {
	err := os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		return nil, xerrors.Errorf("failed to create root folder: %v", err)
	}

	blobStore, err := newStore(filepath.Join(folderPath, blob))
	if err != nil {
		return nil, xerrors.Errorf("failed to create blobStore: %v", err)
	}

	namingStore, err := newStore(filepath.Join(folderPath, naming))
	if err != nil {
		return nil, xerrors.Errorf("failed to create namingStore: %v", err)
	}

	blockchainStore, err := newStore(filepath.Join(folderPath, blockchain))
	if err != nil {
		return nil, xerrors.Errorf("failed to create blockchainStore: %v", err)
	}

	return Storage{
		folderPath: folderPath,
		blob:       blobStore,
		naming:     namingStore,
		blockchain: blockchainStore,
	}, nil
}

// Storage implements an in-memory storage.
//
// - implements storage.Storage
type Storage struct {
	folderPath string

	blob       storage.Store
	naming     storage.Store
	blockchain storage.Store
}

// GetFolderPath returns the folder path
func (s Storage) GetFolderPath() string {
	return s.folderPath
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

func newStore(folderPath string) (*store, error) {
	err := os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		return nil, xerrors.Errorf("failed to create store folder: %v", err)
	}

	return &store{
		folderPath: folderPath,
	}, nil
}

// store implements a file-based store.
//
// - implements storage.Store
type store struct {
	sync.Mutex
	folderPath string
}

// Get implements storage.Store
func (s *store) Get(key string) (val []byte) {
	s.Lock()
	defer s.Unlock()

	val, err := os.ReadFile(filepath.Join(s.folderPath, string(key)))
	if err != nil {
		return nil
	}

	return val
}

// Set implements storage.Store
func (s *store) Set(key string, val []byte) {
	s.Lock()
	defer s.Unlock()

	os.WriteFile(filepath.Join(s.folderPath, string(key)), val, os.ModePerm)
}

// Delete implements storage.Store
func (s *store) Delete(key string) {
	s.Lock()
	defer s.Unlock()

	os.Remove(filepath.Join(s.folderPath, string(key)))
}

// ForEach implements storage.Store
func (s *store) ForEach(f func(key string, val []byte) bool) {
	s.Lock()
	defer s.Unlock()

	fileInfos, err := ioutil.ReadDir(s.folderPath)
	if err != nil {
		return
	}

	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		val, err := os.ReadFile(filepath.Join(s.folderPath, fileInfo.Name()))
		if err != nil {
			continue
		}

		cont := f(fileInfo.Name(), val)
		if !cont {
			return
		}
	}
}

// Len implements storage.Store
func (s *store) Len() int {
	s.Lock()
	defer s.Unlock()

	fileInfos, err := ioutil.ReadDir(s.folderPath)
	if err != nil {
		return 0
	}

	i := 0

	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() {
			i++
		}
	}

	return i
}
