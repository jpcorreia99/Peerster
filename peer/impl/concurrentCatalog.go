package impl

import (
	"go.dedis.ch/cs438/peer"
	"sync"
)

// storer of filenames existant in other peers other than mine
type ConcurrentCatalog struct {
	catalog peer.Catalog
	mutex   sync.RWMutex
}

func newConcurrentCatalog() *ConcurrentCatalog {
	return &ConcurrentCatalog{
		catalog: make(peer.Catalog),
		mutex:   sync.RWMutex{},
	}
}

func (cC *ConcurrentCatalog) add(hexHash string, address string) {
	cC.mutex.Lock()
	defer cC.mutex.Unlock()

	_, exists := cC.catalog[hexHash]
	if !exists {
		cC.catalog[hexHash] = make(map[string]struct{})
	}

	cC.catalog[hexHash][address] = struct{}{}
}

func (cC *ConcurrentCatalog) getCatalog() peer.Catalog {
	cC.mutex.RLock()
	defer cC.mutex.RUnlock()

	catalogCopy := make(peer.Catalog)

	for hexHash, addressSet := range cC.catalog {
		catalogCopy[hexHash] = addressSet
	}

	return catalogCopy
}

// get returns nil if not found
func (cC *ConcurrentCatalog) get(hexHash string) []string {
	cC.mutex.Lock()
	defer cC.mutex.Unlock()

	addressMap, exists := cC.catalog[hexHash]

	if exists {
		addressList := make([]string, len(addressMap))

		i := 0
		for address := range addressMap {
			addressList[i] = address
			i++
		}

		return addressList
	} else {
		return nil
	}
}
