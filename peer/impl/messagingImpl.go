package impl

import (
	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"time"
)

// ------------------ HW 0 and 1  --------------------
// functions responsible for sharing messages between peers

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	forwardingAddress, err := n.getForwardingAddress(dest)
	if err != nil {
		return err
	}

	header := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), dest, 1)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}
	err = n.socket.Send(forwardingAddress, pkt, 0)
	return err
}

// Broadcast implements peer.Messaging
/*
	1. the status manager creates a new rumor, increaring the sequence number for our own peed
	2. wrap the rumor in a RumorsMessage
	3. ask for a shuffled neighbour list and, if there's any neighbour, send the packet
*/
func (n *node) Broadcast(msg transport.Message) error {
	rumor := n.statusManager.newRumor(n.socket.GetAddress(), &msg)

	// building the rumours message and transforming it to transport.Message
	rumors := types.Message(&types.RumorsMessage{
		Rumors: []types.Rumor{rumor},
	})

	rumorsMsg, err := n.registry.MarshalMessage(rumors)
	if err != nil {
		return err
	}

	header := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), n.socket.GetAddress(), 0)
	pkt := transport.Packet{
		Header: &header,
		Msg:    &rumorsMsg,
	}

	// randomly finding the address to whom forward the packet
	neighbourList := n.routingTable.getNeighboursList()
	if len(neighbourList) > 0 {
		neighbourAddress := neighbourList[0] // the list was previously shuffled, so there's no need ot pick a random index
		pkt.Header.Destination = neighbourAddress

		err = n.socket.Send(neighbourAddress, pkt, 0)
		if err != nil {
			return err
		}
		n.wg.Add(1)
		go n.ackAwaitingRoutine(pkt)
	}

	// processing the message locally
	err = n.selfRumorsMessageCallback(rumors, pkt)
	if err != nil {
		return err
	}

	return nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	n.routingTable.addkeys(addr)
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	return n.routingTable.getRoutingTable()
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.routingTable.delete(origin)
	} else {
		n.routingTable.addPair(origin, relayAddr)
	}
}

/*
 Creates a channel for the packet we're waiting to be acked.
 The channel is inside a map where the key is the packetID
 When the message is acked the channel is closed, triggering the select statement.

  If the wait is infinite sleep for 1 second for each iteration,
 	to be able to check if the channel has been closed in the meantime
 	to allow the app to gracefully shutdown

 If the interval times out, the original packet is sent to a different neighbour
*/
func (n *node) ackAwaitingRoutine(pkt transport.Packet) {
	defer n.wg.Done()

	alreadySentMap := make(map[string]bool)
	alreadySentMap[pkt.Header.Destination] = true

	ackChanelPtr := n.ackMap.createEntry(pkt.Header.PacketID)
	running := true

	if n.configuration.AckTimeout != 0 {
		time.Sleep(n.configuration.AckTimeout)
	} else {
		time.Sleep(200 * time.Millisecond) // if the wait is infinite sleep for 1 second
	}
	for running {
		select {
		case <-*ackChanelPtr:
			running = false
		default:

			if n.configuration.AckTimeout == 0 {
				time.Sleep(200 * time.Millisecond)
				break // return to the loop
			}
			newAddress := ""
			neighbourList := n.routingTable.getNeighboursList()

			for _, neighbourAddress := range neighbourList {
				if !alreadySentMap[neighbourAddress] {
					newAddress = neighbourAddress
					alreadySentMap[neighbourAddress] = true
					break
				}
			}

			if newAddress == "" {
				return
			}

			pkt.Header.Destination = newAddress
			err := n.socket.Send(pkt.Header.Destination, pkt, 0)
			if err != nil {
				log.Err(err)
				return
			}

			time.Sleep(n.configuration.AckTimeout)
		}
	}
}

func (n *node) getForwardingAddress(dstAddress string) (string, error) {
	forwardingAddress, exists := n.routingTable.get(dstAddress)

	if !exists {
		return "", xerrors.Errorf("Address not in routing table: %v", dstAddress)
	}

	return forwardingAddress, nil
}
