package main

import (
	"encoding/json"
	"fmt"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/peer/impl"
	"go.dedis.ch/cs438/registry/standard"
	"go.dedis.ch/cs438/storage/inmemory"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/udp"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"time"
)

var totalNumberOfPeers = uint(3)
var count = uint(0)

func main() {
	node1 := createPeer("127.0.0.1:12345")
	node2 := createPeer("127.0.0.1:54321")
	node3 := createPeer("127.0.0.1:55555")

	node1.AddPeer("127.0.0.1:54321")
	node2.AddPeer("127.0.0.1:12345")
	node2.AddPeer("127.0.0.1:55555")

	chat := types.ChatMessage{
		Message: "Hello from node 1",
	}

	buf, err := json.Marshal(&chat)
	if err != nil {
		fmt.Println(err)
	}

	chatMsg := transport.Message{
		Type:    chat.Name(),
		Payload: buf,
	}
	err = node1.Broadcast(chatMsg)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = node1.Stop()
	if err != nil {
		fmt.Println(err)
		return
	}
	err = node2.Stop()
	if err != nil {
		fmt.Println(err)
		return
	}
	err = node3.Stop()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("closed")
}

func createPeer(address string) peer.Peer {
	count++
	var peerFactory = impl.NewPeer

	trans := udp.NewUDP()

	sock, err := trans.CreateSocket(address)
	if err != nil {
		fmt.Println(xerrors.Errorf("failed to create socket"))
		return nil
	}

	var storage = inmemory.NewPersistency()

	conf := peer.Configuration{
		Socket:          sock,
		MessageRegistry: standard.NewRegistry(),

		AntiEntropyInterval: time.Second,
		HeartbeatInterval:   time.Second,
		AckTimeout:          3 * time.Second,
		ContinueMongering:   0.5,

		ChunkSize: 8192,
		BackoffDataRequest: peer.Backoff{
			//2s 2 5
			Initial: 2 * time.Second,
			Factor:  2,
			Retry:   5,
		},
		Storage: storage,

		TotalPeers: totalNumberOfPeers,
		PaxosThreshold: func(u uint) int {
			return int(u/2 + 1)
		},
		PaxosID:            count,
		PaxosProposerRetry: 5 * time.Second,
	}

	return peerFactory(conf)
}
