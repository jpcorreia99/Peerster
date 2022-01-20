package impl

import "sync"

// used in the SearchAll function to store the files returned
type concurrentSearchResultsStore struct {
	searchResultsStore map[string]map[string]bool // key: requestID, value: set of names corresponding to that search
	mutex              sync.RWMutex
}

func newConcurrentSearchResultsStore() *concurrentSearchResultsStore {
	return &concurrentSearchResultsStore{
		searchResultsStore: make(map[string]map[string]bool),
		mutex:              sync.RWMutex{},
	}
}

func (cSRS *concurrentSearchResultsStore) createEntry(requestID string) {
	cSRS.mutex.Lock()
	defer cSRS.mutex.Unlock()

	cSRS.searchResultsStore[requestID] = make(map[string]bool)
}

func (cSRS *concurrentSearchResultsStore) registerFile(requestID string, filename string) {
	cSRS.mutex.Lock()
	defer cSRS.mutex.Unlock()

	_, exists := cSRS.searchResultsStore[requestID]

	if exists {
		cSRS.searchResultsStore[requestID][filename] = true
	}
}

func (cSRS *concurrentSearchResultsStore) get(requestID string) []string {
	cSRS.mutex.Lock()
	defer cSRS.mutex.Unlock()

	filenamesSet := cSRS.searchResultsStore[requestID]

	var filenames = make([]string, 0, len(filenamesSet))

	for filename := range filenamesSet {
		filenames = append(filenames, filename)
	}

	delete(cSRS.searchResultsStore, requestID)

	return filenames
}

func (cSRS *concurrentSearchResultsStore) contains(requestID string) bool {
	cSRS.mutex.RLock()
	defer cSRS.mutex.RUnlock()

	_, exists := cSRS.searchResultsStore[requestID]

	return exists
}
