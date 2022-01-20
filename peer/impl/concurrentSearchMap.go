package impl

import "sync"

// used in the searchFirst function
type concurrentSearchMap struct {
	searchMap map[string]*chan string // key: search requestID -> chanel to send file if found
	mutex     sync.Mutex
}

func newConcurrentSearchMap() *concurrentSearchMap {
	return &concurrentSearchMap{
		searchMap: make(map[string]*chan string),
		mutex:     sync.Mutex{},
	}
}

func (cSM *concurrentSearchMap) createEntry(requestID string) *chan string {
	cSM.mutex.Lock()
	defer cSM.mutex.Unlock()

	newChannel := make(chan string)

	cSM.searchMap[requestID] = &newChannel

	return &newChannel
}

func (cSM *concurrentSearchMap) signalChannel(requestID string, filename string) {
	cSM.mutex.Lock()
	defer cSM.mutex.Unlock()

	requestChannel, exists := cSM.searchMap[requestID]

	if exists {
		*requestChannel <- filename
	}

	delete(cSM.searchMap, filename)
}

func (cSM *concurrentSearchMap) closeChannel(requestID string) {
	cSM.mutex.Lock()
	defer cSM.mutex.Unlock()
	channel, exists := cSM.searchMap[requestID]

	if exists {
		close(*channel)
		delete(cSM.searchMap, requestID)
	}
}
func (cSM *concurrentSearchMap) closeAllChannels() {
	cSM.mutex.Lock()
	defer cSM.mutex.Unlock()

	for _, channel := range cSM.searchMap {
		close(*channel)
	}
}

func (cSM *concurrentSearchMap) contains(requestID string) bool {
	cSM.mutex.Lock()
	defer cSM.mutex.Unlock()

	_, exists := cSM.searchMap[requestID]

	return exists
}
