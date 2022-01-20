package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"sync"
)

// used for all the logic related to rumors, keeps track of sequence numbers and rumors send/received
type sequenceTable map[string]uint     // address, last sequence number
type rumorMap map[string][]types.Rumor // [originAddress] [list of rumors]

type ConcurrentStatusManager struct {
	sequenceTable sequenceTable
	rumorMap      rumorMap
	mutex         sync.RWMutex
}

func newConcurrentSequenceTableManager() *ConcurrentStatusManager {
	sequenceTable := make(sequenceTable)
	rumorMap := make(rumorMap)

	return &ConcurrentStatusManager{
		sequenceTable: sequenceTable,
		rumorMap:      rumorMap,
		mutex:         sync.RWMutex{},
	}
}

func (cST *ConcurrentStatusManager) getSequenceTable() sequenceTable {
	cST.mutex.RLock()
	defer cST.mutex.RUnlock()

	returnedTable := make(sequenceTable)

	// Copy from the original map to the target map
	for key, value := range cST.sequenceTable {
		returnedTable[key] = value
	}

	return returnedTable
}

// increments the owners sequence number and returns the new value
// creates a new rumour with that value and stores it on the map
func (cST *ConcurrentStatusManager) newRumor(originAddress string, msg *transport.Message) types.Rumor {
	cST.mutex.Lock()
	defer cST.mutex.Unlock()

	sequenceNumber := cST.sequenceTable[originAddress]
	sequenceNumber++
	cST.sequenceTable[originAddress] = sequenceNumber

	rumor := types.Rumor{
		Origin:   originAddress,
		Sequence: sequenceNumber,
		Msg:      msg,
	}

	cST.rumorMap[originAddress] = append(cST.rumorMap[originAddress], rumor)
	return rumor

}

/*
2. for each rumor in rumors packet:
2.1 See if sequence number was expected
2.2 if so update expected sequence number process the packet inside the rumor

returns: boolean indicating the any of the rumors was new

*/
func (cST *ConcurrentStatusManager) processRumors(rumorsMsg *types.RumorsMessage, pkt *transport.Packet, n *node) (bool, error) {
	cST.mutex.Lock()
	defer cST.mutex.Unlock()
	newRumor := false

	for _, rumor := range rumorsMsg.Rumors {
		lastSequenceNumber := cST.sequenceTable[rumor.Origin]

		if rumor.Sequence == lastSequenceNumber+1 {
			// update routing table if peer was not neighbour
			n.routingTable.updateIfNotNeighbour(rumor.Origin, pkt.Header.RelayedBy) // needs to be nested since a rumorsPacket can contain rumors from multiple origins
			cST.rumorMap[rumor.Origin] = append(cST.rumorMap[rumor.Origin], rumor)
			newRumor = true
			cST.sequenceTable[rumor.Origin] = rumor.Sequence // update to new expected value
			enclosedRumor := transport.Packet{               // enclose the rumor in a packet so that the registry can process it
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}

			cST.mutex.Unlock()
			err := n.registry.ProcessPacket(enclosedRumor)
			cST.mutex.Lock()
			if err != nil {
				return false, err
			}
		}
	}

	return newRumor, nil
}

// indicates if the current sequence table is missing rumors when compared to another node's table
func (cST *ConcurrentStatusManager) isMissingRumors(sequenceTable sequenceTable) bool {
	cST.mutex.RLock()
	defer cST.mutex.RUnlock()

	for address, sequenceNumber := range sequenceTable {
		currentSequenceNumber, exists := cST.sequenceTable[address]
		if !exists || sequenceNumber > currentSequenceNumber {
			return true
		}
	}

	return false
}

// given a sequence table from another node, returns a list with all rumors the current node has that the other node has not received
func (cST *ConcurrentStatusManager) getMissingRumors(sequenceTable sequenceTable) []types.Rumor {
	cST.mutex.RLock()
	defer cST.mutex.RUnlock()

	var rumors []types.Rumor

	for address, rumorList := range cST.rumorMap {
		sequenceNumber := sequenceTable[address]
		if sequenceNumber < cST.sequenceTable[address] {
			for i := sequenceNumber; i < cST.sequenceTable[address]; i++ {
				rumors = append(rumors, rumorList[i])
			}
		}
	}

	return rumors
}
