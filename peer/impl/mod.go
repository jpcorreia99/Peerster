package impl

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewPeer creates a new peer. You can change the content and location of this
// function, but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	routingTable := newConcurrentRoutingTable()
	statusManager := newConcurrentSequenceTableManager()
	ackMap := newConcurrentAckMap()
	catalog := newConcurrentCatalog()
	requestMap := newConcurrentRequestMap()
	searchStorer := newConcurrentSearchResultsStore()
	searchMap := newConcurrentSearchMap()
	paxosController := newPaxosController(conf)

	node := &node{
		running:            uint32(0),
		terminationChannel: make(chan struct{}),
		configuration:      conf,
		socket:             conf.Socket,
		registry:           conf.MessageRegistry,
		wg:                 sync.WaitGroup{},
		errorChannel:       make(chan error),
		routingTable:       routingTable,
		statusManager:      statusManager,
		ackMap:             ackMap,
		catalog:            catalog,
		requestMap:         requestMap,
		searchStorer:       searchStorer,
		searchMap:          searchMap,
		paxosController:    paxosController,
	}

	// adding itself to the routing table
	node.routingTable.addPair(node.socket.GetAddress(), node.socket.GetAddress())

	node.registry.RegisterMessageCallback(types.ChatMessage{}, node.ChatMessageCallback)
	node.registry.RegisterMessageCallback(types.EmptyMessage{}, node.EmptyMessageCallback)
	node.registry.RegisterMessageCallback(types.StatusMessage{}, node.StatusMessageCallback)
	node.registry.RegisterMessageCallback(types.AckMessage{}, node.AckMessageCallback)
	node.registry.RegisterMessageCallback(types.RumorsMessage{}, node.RumorsMessageCallback)
	node.registry.RegisterMessageCallback(types.PrivateMessage{}, node.PrivateMessageCallback)
	node.registry.RegisterMessageCallback(types.DataRequestMessage{}, node.DataRequestMessageCallback)
	node.registry.RegisterMessageCallback(types.DataReplyMessage{}, node.DataReplyMessageCallback)
	node.registry.RegisterMessageCallback(types.SearchRequestMessage{}, node.SearchRequestMessageCallback)
	node.registry.RegisterMessageCallback(types.SearchReplyMessage{}, node.SearchReplyMessageCallback)
	node.registry.RegisterMessageCallback(types.PaxosPrepareMessage{}, node.PaxosPrepareMessageCallback)
	node.registry.RegisterMessageCallback(types.PaxosProposeMessage{}, node.PaxosProposeMessageCallback)
	node.registry.RegisterMessageCallback(types.PaxosPromiseMessage{}, node.PaxosPromiseMessageCallback)
	node.registry.RegisterMessageCallback(types.PaxosAcceptMessage{}, node.PaxosAcceptMessageCallback)
	node.registry.RegisterMessageCallback(types.TLCMessage{}, node.TlCMessageCallback)
	return node
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	running            uint32
	terminationChannel chan struct{}
	configuration      peer.Configuration
	socket             transport.Socket
	registry           registry.Registry
	wg                 sync.WaitGroup
	errorChannel       chan error // channel used by the mainReadingRoutine to send errors outside
	routingTable       *ConcurrentRoutingTable
	statusManager      *ConcurrentStatusManager
	ackMap             *ConcurrentAckMap             // used by the broadcasting function to wait until the ack has been received,
	catalog            *ConcurrentCatalog            // storer of filenames existant in peers other than mine
	requestMap         *ConcurrentRequestMap         // key: data requestID, value: channel to signal its receipt
	searchStorer       *concurrentSearchResultsStore // used in the SearchAll function to store the files returned
	searchMap          *concurrentSearchMap          // used in the SearchFirst function to store channels responsible to signal a valid file
	paxosController    *paxosController
}

// Start implements peer.Service
func (n *node) Start() error {
	atomic.StoreUint32(&n.running, 1)
	n.wg.Add(1)
	go n.mainReadingRoutine()

	if n.configuration.AntiEntropyInterval > 0 {
		n.wg.Add(1)
		go n.antiEntropyMechanism()
	}

	if n.configuration.HeartbeatInterval > 0 {
		n.wg.Add(1)
		go n.heartbeatMechanism()
	}
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	var err error = nil
	// purpose of the structure is only to read an error if it is available
	select {
	case err = <-n.errorChannel:
		break
	default:
		break
	}

	atomic.StoreUint32(&n.running, 0)
	close(n.terminationChannel)
	n.ackMap.closeAllChannels()
	n.requestMap.closeAllChannels()
	n.searchMap.closeAllChannels()
	n.wg.Wait()
	return err
}

// main routine run by the peer, keeps reading new messages until it's told to stop
func (n *node) mainReadingRoutine() {
	defer n.wg.Done()

	for atomic.LoadUint32(&n.running) == 1 {
		pkt, err := n.socket.Recv(time.Second * 1)

		if errors.Is(err, transport.TimeoutErr(0)) {
			continue
		} else if err != nil { // different error that must stop the routine
			n.errorChannel <- err
			atomic.StoreUint32(&n.running, 0)
			break
		}

		n.wg.Add(1)
		go n.handlePacket(pkt)
	}
}

func (n *node) handlePacket(pkt transport.Packet) {
	defer n.wg.Done()

	var err error
	if pkt.Header.Destination == n.socket.GetAddress() {
		err = n.registry.ProcessPacket(pkt)
	} else {
		pkt.Header.RelayedBy = n.socket.GetAddress()
		err = n.socket.Send(pkt.Header.Destination, pkt, time.Second*1)
	}

	if err != nil {
		log.Info().Err(err)
	}
}

/*
  Given a fixed interval (>0) , send a status message to a random neighbour
*/
func (n *node) antiEntropyMechanism() {
	defer n.wg.Done()

	ticker := time.NewTicker(n.configuration.AntiEntropyInterval)
	for {
		select {
		case <-n.terminationChannel:
			return
		case <-ticker.C:
			neighbourList := n.routingTable.getNeighboursList()
			if len(neighbourList) > 0 {
				status := types.StatusMessage(n.statusManager.getSequenceTable())
				statusMessage, err := n.registry.MarshalMessage(status)
				if err != nil {
					log.Err(err)
					return
				}

				neighbourAddress := neighbourList[0] // can be first index since the list is shuffled when obtained
				statusHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), neighbourAddress, 0)

				pkt := transport.Packet{
					Header: &statusHeader,
					Msg:    &statusMessage,
				}

				err = n.socket.Send(neighbourAddress, pkt, 0)
				if err != nil {
					log.Err(err)
					return
				}
			}
		}
	}
}

/*
  Given an interval (>0), broadcast an empty message
  This is done to let a peer announce itself to some neighbours and let them update their routing
*/
func (n *node) heartbeatMechanism() {
	defer n.wg.Done()

	ticker := time.NewTicker(1 * time.Nanosecond) // to trigger it immediately the first time
	for {
		select {
		case <-n.terminationChannel:
			return
		case <-ticker.C:
			emptyStruct := types.EmptyMessage{}
			emptyMsg, err := n.registry.MarshalMessage(emptyStruct)
			if err != nil {
				log.Err(err)
				return
			}

			err = n.Broadcast(emptyMsg)
			if err != nil {
				log.Err(err).Msg(err.Error())
				return
			}
			ticker = time.NewTicker(n.configuration.HeartbeatInterval)
		}
	}
}
