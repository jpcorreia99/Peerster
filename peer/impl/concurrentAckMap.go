package impl

import "sync"

// used by the broadcasting function to wait until the ack has been received,
type ConcurrentAckMap struct {
	ackMap map[string]*chan struct{} // packet ID, channel to signal it has been acked back
	mutex  sync.Mutex
}

func newConcurrentAckMap() *ConcurrentAckMap {
	return &ConcurrentAckMap{
		ackMap: make(map[string]*chan struct{}), // packetId, channel to signal its receipt
		mutex:  sync.Mutex{},
	}
}

func (cAM *ConcurrentAckMap) createEntry(packetId string) *chan struct{} {
	newChannel := make(chan struct{})
	cAM.mutex.Lock()
	defer cAM.mutex.Unlock()

	cAM.ackMap[packetId] = &newChannel

	return &newChannel
}

// signals the channel (by closing it) that an ack was received
func (cAM *ConcurrentAckMap) closeChannel(packetId string) {
	cAM.mutex.Lock()
	defer cAM.mutex.Unlock()

	chanPointer, exists := cAM.ackMap[packetId]
	if !exists {
		return
	}

	close(*chanPointer)
	delete(cAM.ackMap, packetId) // removes the channel from the map
}

func (cAM *ConcurrentAckMap) closeAllChannels() {
	cAM.mutex.Lock()
	defer cAM.mutex.Unlock()

	for _, channel := range cAM.ackMap {
		close(*channel)
	}
}
