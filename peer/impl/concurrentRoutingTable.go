package impl

import (
	"math/rand"
	"sync"
	"time"

	"go.dedis.ch/cs438/peer"
)

type ConcurrentRoutingTable struct {
	routingTable  peer.RoutingTable
	neighbourList []string
	mutex         sync.RWMutex
}

func newConcurrentRoutingTable() *ConcurrentRoutingTable {
	return &ConcurrentRoutingTable{
		routingTable:  make(peer.RoutingTable),
		neighbourList: []string{},
		mutex:         sync.RWMutex{},
	}
}

func (cRT *ConcurrentRoutingTable) addPair(key, value string) {
	cRT.mutex.Lock()
	defer cRT.mutex.Unlock()

	cRT.routingTable[key] = value
}

func (cRT *ConcurrentRoutingTable) addkeys(keys []string) {
	cRT.mutex.Lock()
	defer cRT.mutex.Unlock()

	for _, key := range keys {
		cRT.routingTable[key] = key
		cRT.neighbourList = append(cRT.neighbourList, key)
	}
}

func (cRT *ConcurrentRoutingTable) get(key string) (string, bool) {
	cRT.mutex.RLock()
	defer cRT.mutex.RUnlock()

	value, exists := cRT.routingTable[key]
	return value, exists
}

func (cRT *ConcurrentRoutingTable) delete(key string) {
	cRT.mutex.Lock()
	defer cRT.mutex.Unlock()

	delete(cRT.routingTable, key)
}

func (cRT *ConcurrentRoutingTable) getRoutingTable() peer.RoutingTable {
	returnedTable := make(peer.RoutingTable)

	cRT.mutex.RLock()
	defer cRT.mutex.RUnlock()

	// Copy from the original map to the target map
	for key, value := range cRT.routingTable {
		returnedTable[key] = value
	}

	return returnedTable
}

// returns a shuffled neighbour list
func (cRT *ConcurrentRoutingTable) getNeighboursList() []string {
	cRT.mutex.RLock()
	defer cRT.mutex.RUnlock()

	cpy := make([]string, len(cRT.neighbourList))
	copy(cpy, cRT.neighbourList)

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(cpy), func(i, j int) { cpy[i], cpy[j] = cpy[j], cpy[i] })

	return cpy
}

// if the sourceAddress is not already a neighbour, add its entry to the routing table
func (cRT *ConcurrentRoutingTable) updateIfNotNeighbour(sourceAddress string, relayerAddress string) {
	cRT.mutex.Lock()
	defer cRT.mutex.Unlock()
	for _, neighbourAddress := range cRT.neighbourList {
		if sourceAddress == neighbourAddress {
			return // end execution if sourceAddress is neighbour
		}
	}

	if sourceAddress == relayerAddress {
		cRT.neighbourList = append(cRT.neighbourList, sourceAddress)
	}

	cRT.routingTable[sourceAddress] = relayerAddress
}
