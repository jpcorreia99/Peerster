package impl

import "sync"

// used by the search functions. It is used to signal that a query was answered
type ConcurrentRequestMap struct {
	requestMap map[string]*chan struct{} //  key: data requestID, value: channel to signal its receipt
	mutex      sync.Mutex
}

func newConcurrentRequestMap() *ConcurrentRequestMap {
	return &ConcurrentRequestMap{
		requestMap: make(map[string]*chan struct{}), // requestID, channel to signal its receipt
		mutex:      sync.Mutex{},
	}
}

func (cRM *ConcurrentRequestMap) createEntry(requestID string) *chan struct{} {
	newChannel := make(chan struct{})
	cRM.mutex.Lock()
	defer cRM.mutex.Unlock()

	cRM.requestMap[requestID] = &newChannel

	return &newChannel
}

func (cRM *ConcurrentRequestMap) closeChannel(requestID string) {
	cRM.mutex.Lock()
	defer cRM.mutex.Unlock()

	chanPointer, exists := cRM.requestMap[requestID]
	if !exists {
		return
	}

	close(*chanPointer)
	delete(cRM.requestMap, requestID) // removes the channel from the map
}

func (cRM *ConcurrentRequestMap) closeAllChannels() {
	cRM.mutex.Lock()
	defer cRM.mutex.Unlock()

	for _, channel := range cRM.requestMap {
		close(*channel)
	}
}
