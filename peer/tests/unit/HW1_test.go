package unit

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/cs438/internal/graph"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/channel"
	"go.dedis.ch/cs438/types"
)

// 1-1
//
// If I broadcast a message as a rumor and I have only one neighbor, then that
// neighbor should receive my message, answer with an ack message, and update
// its routing table.
func Test_HW1_Messaging_Broadcast_Rumor_Simple(t *testing.T) {

	getTest := func(transp transport.Transport) func(*testing.T) {
		return func(t *testing.T) {
			fake := z.NewFakeMessage(t)
			handler1, status1 := fake.GetHandler(t)
			handler2, status2 := fake.GetHandler(t)

			node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1))
			defer node1.Stop()

			node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2))
			defer node2.Stop()

			node1.AddPeer(node2.GetAddr())

			err := node1.Broadcast(fake.GetNetMsg(t))
			require.NoError(t, err)

			time.Sleep(time.Second * 2)

			n1Ins := node1.GetIns()
			n2Ins := node2.GetIns()

			n1Outs := node1.GetOuts()
			n2Outs := node2.GetOuts()

			// > n1 should have received an ack from n2

			require.Len(t, n1Ins, 1)
			pkt := n1Ins[0]
			require.Equal(t, "ack", pkt.Msg.Type)

			// > n2 should have received 1 rumor packet from n1

			require.Len(t, n2Ins, 1)

			pkt = n2Ins[0]
			require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			rumor := z.GetRumor(t, pkt.Msg)
			require.Len(t, rumor.Rumors, 1)
			r := rumor.Rumors[0]
			require.Equal(t, node1.GetAddr(), r.Origin)
			require.Equal(t, uint(1), r.Sequence) // must start with 1

			fake.Compare(t, r.Msg)

			// > n1 should have sent 1 packet to n2

			require.Len(t, n1Outs, 1)
			require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			rumor = z.GetRumor(t, pkt.Msg)
			require.Len(t, rumor.Rumors, 1)
			r = rumor.Rumors[0]
			require.Equal(t, node1.GetAddr(), r.Origin)
			require.Equal(t, uint(1), r.Sequence)

			fake.Compare(t, r.Msg)

			// > n2 should have sent an ack packet to n1

			require.Len(t, n2Outs, 1)

			pkt = n2Outs[0]
			ack := z.GetAck(t, pkt.Msg)
			require.Equal(t, n1Outs[0].Header.PacketID, ack.AckedPacketID)

			// >> node2 should have sent the following status to n1 {node1 => 1}

			require.Len(t, ack.Status, 1)
			require.Equal(t, uint(1), ack.Status[node1.GetAddr()])

			// > node1 and node2 should've executed the handlers

			status1.CheckCalled(t)
			status2.CheckCalled(t)

			// > routing table of node1 should be updated

			routing := node1.GetRoutingTable()
			require.Len(t, routing, 2)

			entry, found := routing[node1.GetAddr()]
			require.True(t, found)

			require.Equal(t, node1.GetAddr(), entry)

			entry, found = routing[node2.GetAddr()]
			require.True(t, found)

			require.Equal(t, node2.GetAddr(), entry)

			// > routing table of node2 should be updated with node1

			routing = node2.GetRoutingTable()
			require.Len(t, routing, 2)

			entry, found = routing[node2.GetAddr()]
			require.True(t, found)

			require.Equal(t, node2.GetAddr(), entry)

			entry, found = routing[node1.GetAddr()]
			require.True(t, found)

			require.Equal(t, node1.GetAddr(), entry)
		}
	}

	t.Run("channel transport", getTest(channelFac()))
	t.Run("UDP transport", getTest(udpFac()))
}

// 1-2
//
// Given the following topology:
//   A -> B -> C
// If A broadcast a message, then B should receive it AND then send it to C. C
// should also update its routing table with a relay to A via B. We're setting
// the ContinueMongering attribute to 0.
func Test_HW1_Messaging_Broadcast_Rumor_Three_Nodes_No_ContinueMongering(t *testing.T) {

	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)
	handler2, status2 := fake.GetHandler(t)
	handler3, status3 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithContinueMongering(0))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2), z.WithContinueMongering(0))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler3), z.WithContinueMongering(0))
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node3.GetAddr())

	err := node1.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	time.Sleep(time.Second * 2)

	n3Ins := node3.GetIns()
	n3Outs := node3.GetOuts()

	// > n3 should have received a rumor from n2

	require.Len(t, n3Ins, 1)
	pkt := n3Ins[0]
	require.Equal(t, "rumor", pkt.Msg.Type)

	// > n3 should have sent an ack packet to n2

	require.Len(t, n3Outs, 1)

	pkt = n3Outs[0]
	require.Equal(t, "ack", pkt.Msg.Type)

	// > node1, node2, and node3 should've executed the handlers

	status1.CheckCalled(t)
	status2.CheckCalled(t)
	status3.CheckCalled(t)

	// > checking the routing of node1

	expected := peer.RoutingTable{
		node1.GetAddr(): node1.GetAddr(),
		node2.GetAddr(): node2.GetAddr(),
	}
	require.Equal(t, expected, node1.GetRoutingTable())

	// > checking the routing of node2

	expected = peer.RoutingTable{
		node2.GetAddr(): node2.GetAddr(),
		node1.GetAddr(): node1.GetAddr(),
		node3.GetAddr(): node3.GetAddr(),
	}
	require.Equal(t, expected, node2.GetRoutingTable())

	// > checking the routing of node3, it should have a new relay to node1 via
	// node2.

	expected = peer.RoutingTable{
		node3.GetAddr(): node3.GetAddr(),
		node1.GetAddr(): node2.GetAddr(),
	}
	require.Equal(t, expected, node3.GetRoutingTable())
}

// 1-3
//
// When the anti entropy is non-zero, status message should be sent accordingly.
func Test_HW1_Messaging_AntiEntropy(t *testing.T) {

	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAntiEntropy(time.Millisecond*500))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	// As soon as node1 has a peer, it should send to that peer a status message
	// every 500 ms.
	node1.AddPeer(node2.GetAddr())

	// If we wait only 800 ms, then node1 should send only one status message to
	// node2.
	time.Sleep(time.Millisecond * 800)

	n1Ins := node1.GetIns()
	n2Ins := node2.GetIns()

	n1Outs := node1.GetOuts()
	n2Outs := node2.GetOuts()

	// > n1 should have not received any packet

	require.Len(t, n1Ins, 0)

	// > n2 should have received at least 1 status packet from n1

	require.Greater(t, len(n2Ins), 0)

	pkt := n2Ins[0]
	require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
	require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), pkt.Header.Source)

	status := z.GetStatus(t, pkt.Msg)
	require.Len(t, status, 0)

	// > n1 should have sent at least 1 packet to n2

	require.Greater(t, len(n1Outs), 0)

	pkt = n1Outs[0]

	require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
	require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), pkt.Header.Source)

	status = z.GetStatus(t, pkt.Msg)
	require.Len(t, status, 0)

	// > n2 should have not sent any packet

	require.Len(t, n2Outs, 0)
}

// 1-4
//
// When the heartbeat is non-zero, empty rumor messages should be sent
// accordingly.
func Test_HW1_Messaging_Heartbeat(t *testing.T) {

	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Millisecond*500))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	// As soon as node1 has a peer, it should send to that peer an empty rumor
	// every 50 ms.
	node1.AddPeer(node2.GetAddr())

	// If we wait only 800 ms, then node1 should send only one empty rumor to
	// node2.
	time.Sleep(time.Millisecond * 800)

	n1Ins := node1.GetIns()
	n2Ins := node2.GetIns()

	n1Outs := node1.GetOuts()
	n2Outs := node2.GetOuts()

	// > n1 should have received at least an ack message from n2

	require.Greater(t, len(n1Ins), 0)
	pkt := n1Ins[0]
	require.Equal(t, "ack", pkt.Msg.Type)

	// > n2 should have received at least a rumor from n1

	require.Greater(t, len(n2Ins), 0)

	pkt = n2Ins[0]
	require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
	require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), pkt.Header.Source)

	rumor := z.GetRumor(t, pkt.Msg)
	require.Len(t, rumor.Rumors, 1)
	z.GetEmpty(t, rumor.Rumors[0].Msg)

	// > n1 should have sent at least 1 packet to n2

	require.Greater(t, len(n1Outs), 0)

	pkt = n1Outs[0]
	require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
	require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), pkt.Header.Source)

	require.Equal(t, "rumor", pkt.Msg.Type)

	rumor = z.GetRumor(t, pkt.Msg)
	require.Len(t, rumor.Rumors, 1)
	z.GetEmpty(t, rumor.Rumors[0].Msg)

	// > n2 should have sent at least one ack packet

	require.Greater(t, len(n2Outs), 0)
	pkt = n2Outs[0]
	require.Equal(t, "ack", pkt.Msg.Type)
}

// 1-5
//
// Given the following topology:
//   A -> B
//     -> C
// We broadcast from A, and expect that the ContinueMongering will make A to
// send the message to the other peer:
//
//   A->B: Rumor    send to a random neighbor (could be C)
//   A<-B: Ack
//   A->C: Status   continue mongering
//   A<-C: Status   missing rumor, send back status
//   A->C: Rumor    send missing rumor
//   A<-C: Ack
//   A->B: Status   continue mongering, in sync: nothing to do
//
// Here A sends first to B, but it could be C, which would inverse B and C.
func Test_HW1_Messaging_Broadcast_ContinueMongering(t *testing.T) {

	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)
	handler2, status2 := fake.GetHandler(t)
	handler3, status3 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithContinueMongering(1))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2), z.WithContinueMongering(1))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler3), z.WithContinueMongering(1))
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())

	err := node1.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	n1Ins := node1.GetIns()
	n1Outs := node1.GetOuts()

	n2Ins := node2.GetIns()
	n2Outs := node2.GetOuts()

	n3Ins := node3.GetIns()
	n3Outs := node3.GetOuts()

	// > check in messages from n1

	require.Len(t, n1Ins, 3)

	pkt := n1Ins[0]
	require.Equal(t, "ack", pkt.Msg.Type)

	pkt = n1Ins[1]
	require.Equal(t, "status", pkt.Msg.Type)

	pkt = n1Ins[2]
	require.Equal(t, "ack", pkt.Msg.Type)

	// > check out messages from n1

	require.Len(t, n1Outs, 4)

	pkt = n1Outs[0]
	require.Equal(t, "rumor", pkt.Msg.Type)

	pkt = n1Outs[1]
	require.Equal(t, "status", pkt.Msg.Type)

	pkt = n1Outs[2]
	require.Equal(t, "rumor", pkt.Msg.Type)

	pkt = n1Outs[3]
	require.Equal(t, "status", pkt.Msg.Type)

	// > check messages for the random selected node
	checkFirstSelected := func(ins, outs []transport.Packet) {
		require.Len(t, ins, 2)

		pkt = ins[0]
		require.Equal(t, "rumor", pkt.Msg.Type)

		pkt = ins[1]
		require.Equal(t, "status", pkt.Msg.Type)

		require.Len(t, outs, 1)

		pkt = outs[0]
		require.Equal(t, "ack", pkt.Msg.Type)
	}

	// > check messages for the node not selected, but that receives messages
	// thanks to the continue mongering mechanism.
	checkSecondSelected := func(ins, outs []transport.Packet) {
		require.Len(t, ins, 2)

		pkt = ins[0]
		require.Equal(t, "status", pkt.Msg.Type)

		pkt = ins[1]
		require.Equal(t, "rumor", pkt.Msg.Type)

		require.Len(t, outs, 2)

		pkt = outs[0]
		require.Equal(t, "status", pkt.Msg.Type)

		pkt = outs[1]
		require.Equal(t, "ack", pkt.Msg.Type)
	}

	// check what node was selected as the random neighbor. This node receives a
	// rumor as the first packet.

	if n2Ins[0].Msg.Type == "rumor" {
		checkFirstSelected(n2Ins, n2Outs)
		checkSecondSelected(n3Ins, n3Outs)
	} else {
		checkFirstSelected(n3Ins, n3Outs)
		checkSecondSelected(n2Ins, n2Outs)
	}

	// > node1, node2, and node3 should've executed the handlers

	status1.CheckCalled(t)
	status2.CheckCalled(t)
	status3.CheckCalled(t)
}

// 1-6
//
// Given the following topology:
//   A -> B
//     -> C
// We broadcast from A, and expect that with no ContinueMongering only B or C
// will get the rumor.
//
//   A->B: Rumor    send to a random neighbor (could be C)
//   A<-B: Ack
//
// Here A sends first to B, but it could be C, which would inverse B and C.
func Test_HW1_Messaging_Broadcast_No_ContinueMongering(t *testing.T) {

	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)
	handler2, status2 := fake.GetHandler(t)
	handler3, status3 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithContinueMongering(0))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2), z.WithContinueMongering(0))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler3), z.WithContinueMongering(0))
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())

	err := node1.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	n1Ins := node1.GetIns()
	n1Outs := node1.GetOuts()

	n2Ins := node2.GetIns()
	n2Outs := node2.GetOuts()

	n3Ins := node3.GetIns()
	n3Outs := node3.GetOuts()

	// > check in messages from n1

	require.Len(t, n1Ins, 1)

	pkt := n1Ins[0]
	require.Equal(t, "ack", pkt.Msg.Type)

	// > check out messages from n1

	require.Len(t, n1Outs, 1)

	pkt = n1Outs[0]
	require.Equal(t, "rumor", pkt.Msg.Type)

	// > check messages for the random selected node
	checkFirstSelected := func(ins, outs []transport.Packet, status z.Status) {
		require.Len(t, ins, 1)

		pkt = ins[0]
		require.Equal(t, "rumor", pkt.Msg.Type)

		require.Len(t, outs, 1)

		pkt = outs[0]
		require.Equal(t, "ack", pkt.Msg.Type)

		status.CheckCalled(t)
	}

	// > check messages for the node not selected
	checkSecondSelected := func(ins, outs []transport.Packet, status z.Status) {
		require.Len(t, ins, 0)
		require.Len(t, outs, 0)

		status.CheckNotCalled(t)
	}

	// check what node was selected as the random neighbor. This node receives a
	// rumor as the first packet.

	if len(n2Ins) == 1 {
		checkFirstSelected(n2Ins, n2Outs, status2)
		checkSecondSelected(n3Ins, n3Outs, status3)
	} else {
		checkFirstSelected(n3Ins, n3Outs, status3)
		checkSecondSelected(n2Ins, n2Outs, status2)
	}

	// > node1 should have executed the handler

	status1.CheckCalled(t)
}

// 1-7
//
// Given the following topology
//   A -> B -> C
// B is not yet started. A and C broadcast a rumor. When B is up, then all nodes
// should have the rumors from A and C.
func Test_HW1_Messaging_Broadcast_CatchUp(t *testing.T) {

	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)
	handler2, status2 := fake.GetHandler(t)
	handler3, status3 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithAntiEntropy(time.Millisecond*50))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2), z.WithAntiEntropy(time.Millisecond*50), z.WithAutostart(false))

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler3), z.WithAntiEntropy(time.Millisecond*50))
	defer node3.Stop()

	err := node1.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	err = node3.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 200)

	err = node2.Start()
	require.NoError(t, err)
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node3.GetAddr())

	time.Sleep(time.Millisecond * 500)

	// > check that each node have 2 rumors

	fakes1 := node1.GetFakes()
	fakes2 := node2.GetFakes()
	fakes3 := node3.GetFakes()

	require.Len(t, fakes1, 2)
	require.Len(t, fakes2, 2)
	require.Len(t, fakes3, 2)

	// > check that every node have the same rumors

	sort.Sort(z.FakeByContent(fakes1))
	sort.Sort(z.FakeByContent(fakes2))
	sort.Sort(z.FakeByContent(fakes3))

	require.Equal(t, fakes1, fakes2)
	require.Equal(t, fakes1, fakes3)

	// > check the handlers were called

	status1.CheckCalled(t)
	status2.CheckCalled(t)
	status3.CheckCalled(t)
}

// 1-8
//
// Given the following topology
//   A -> B
//     -> C
// If A sends a rumor to B, and B doesn't send back an ack, then A should send a
// rumor to C.
func Test_HW1_Messaging_Broadcast_Ack(t *testing.T) {
	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithAckTimeout(time.Millisecond*500))
	defer node1.Stop()

	sock2, err := transp.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock2.Close()

	sock3, err := transp.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock3.Close()

	node1.AddPeer(sock2.GetAddress())
	node1.AddPeer(sock3.GetAddress())

	err = node1.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 800)

	// will fill the getIns
	sock2.Recv(time.Millisecond * 10)
	sock3.Recv(time.Millisecond * 10)

	// to be sure there isn't additional messages
	sock2.Recv(time.Millisecond * 10)
	sock3.Recv(time.Millisecond * 10)

	// > node1 should have received no message
	n1Ins := node1.GetIns()
	require.Len(t, n1Ins, 0)

	// > node1 should have sent two messages: one for sock2, one for sock3
	n2Outs := node1.GetOuts()
	require.Len(t, n2Outs, 2)

	// > node1 should have processed the message locally
	status1.CheckCalled(t)

	// > sock2 should have received the rumor from node1
	n2Ins := sock2.GetIns()
	require.Len(t, n2Ins, 1)
	require.Equal(t, "rumor", n2Ins[0].Msg.Type)
	require.Equal(t, node1.GetAddr(), n2Ins[0].Header.Source)
	require.Equal(t, node1.GetAddr(), n2Ins[0].Header.RelayedBy)
	require.Equal(t, sock2.GetAddress(), n2Ins[0].Header.Destination)

	// > sock3 should have received the rumor from node1
	n3Ins := sock3.GetIns()
	require.Len(t, n3Ins, 1)
	require.Equal(t, "rumor", n3Ins[0].Msg.Type)
	require.Equal(t, node1.GetAddr(), n3Ins[0].Header.Source)
	require.Equal(t, node1.GetAddr(), n3Ins[0].Header.RelayedBy)
	require.Equal(t, sock3.GetAddress(), n3Ins[0].Header.Destination)
}

// 1-9
//
// Given the following topology
//   A -> B
//     -> C
// If A sends a rumor to B, and B doesn't send back an ack, then A should send a
// rumor to C after the timeout. Here we use a timeout of 0, so the message
// should only be sent to one of the node.
func Test_HW1_Messaging_Broadcast_No_Ack(t *testing.T) {
	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithAckTimeout(0), z.WithContinueMongering(0))
	defer node1.Stop()

	sock2, err := transp.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock2.Close()

	sock3, err := transp.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock3.Close()

	node1.AddPeer(sock2.GetAddress())
	node1.AddPeer(sock3.GetAddress())

	err = node1.Broadcast(fake.GetNetMsg(t))
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 100)

	// will fill the getIns
	sock2.Recv(time.Millisecond * 10)
	sock3.Recv(time.Millisecond * 10)

	// to be sure there isn't additional messages
	sock2.Recv(time.Millisecond * 10)
	sock3.Recv(time.Millisecond * 10)

	// > node1 should have received no packet.
	n1Ins := node1.GetIns()
	require.Len(t, n1Ins, 0)

	// > node1 should have sent one messages: one for sock2 or sock3.
	n2Outs := node1.GetOuts()
	require.Len(t, n2Outs, 1)

	// > node1 should have processed the message locally.
	status1.CheckCalled(t)

	// > B or C should have received the rumor from A.

	n2Ins := sock2.GetIns()
	n3Ins := sock3.GetIns()

	if len(n2Ins) == 0 {
		require.Len(t, n3Ins, 1)
		require.Equal(t, "rumor", n3Ins[0].Msg.Type)
		require.Equal(t, node1.GetAddr(), n3Ins[0].Header.Source)
		require.Equal(t, node1.GetAddr(), n3Ins[0].Header.RelayedBy)
		require.Equal(t, sock3.GetAddress(), n3Ins[0].Header.Destination)
	} else {
		require.Len(t, n2Ins, 1)
		require.Equal(t, "rumor", n2Ins[0].Msg.Type)
		require.Equal(t, node1.GetAddr(), n2Ins[0].Header.Source)
		require.Equal(t, node1.GetAddr(), n2Ins[0].Header.RelayedBy)
		require.Equal(t, sock2.GetAddress(), n2Ins[0].Header.Destination)
	}
}

// 1-10
//
// Test the sending of chat messages in rumor on a "big" network. The topology
// is generated randomly, we expect every node to receive chat messages from
// every other nodes.
func Test_HW1_Messaging_Broadcast_BigGraph(t *testing.T) {

	rand.Seed(1)

	n := 20
	chatMsg := "hi from %s"
	stopped := false

	transp := channel.NewTransport()

	nodes := make([]z.TestNode, n)

	stopNodes := func() {
		if stopped {
			return
		}

		defer func() {
			stopped = true
		}()

		wait := sync.WaitGroup{}
		wait.Add(len(nodes))

		for i := range nodes {
			go func(node z.TestNode) {
				defer wait.Done()
				node.Stop()
			}(nodes[i])
		}

		t.Log("stopping nodes...")

		done := make(chan struct{})

		go func() {
			select {
			case <-done:
			case <-time.After(time.Minute * 5):
				t.Error("timeout on node stop")
			}
		}()

		wait.Wait()
		close(done)
	}

	for i := 0; i < n; i++ {
		node := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0",
			z.WithAntiEntropy(time.Second*5),
			// since everyone is sending a rumor, there is no need to have route
			// rumors
			z.WithHeartbeat(0),
			z.WithAckTimeout(time.Second*10))

		nodes[i] = node
	}

	defer stopNodes()

	// out, err := os.Create("topology.dot")
	// require.NoError(t, err)
	graph.NewGraph(0.2).Generate(io.Discard, nodes)

	// > make each node broadcast a rumor, each node should eventually get
	// rumors from all the other nodes.

	wait := sync.WaitGroup{}
	wait.Add(len(nodes))

	for i := range nodes {
		go func(node z.TestNode) {
			defer wait.Done()

			chat := types.ChatMessage{
				Message: fmt.Sprintf(chatMsg, node.GetAddr()),
			}
			data, err := json.Marshal(&chat)
			require.NoError(t, err)

			msg := transport.Message{
				Type:    chat.Name(),
				Payload: data,
			}

			// this is a key factor: the later a message is sent, the more time
			// it takes to be propagated in the network.
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))

			err = node.Broadcast(msg)
			require.NoError(t, err)
		}(nodes[i])
	}

	time.Sleep(time.Millisecond * 3000 * time.Duration(n))

	done := make(chan struct{})

	go func() {
		select {
		case <-done:
		case <-time.After(time.Minute * 5):
			t.Error("timeout on node broadcast")
		}
	}()

	wait.Wait()
	close(done)

	stopNodes()

	// > check that each node got all the chat messages

	nodesChatMsgs := make([][]*types.ChatMessage, len(nodes))

	for i, node := range nodes {
		chatMsgs := node.GetChatMsgs()
		nodesChatMsgs[i] = chatMsgs
	}

	// > each nodes should get the same messages as the first node. We sort the
	// messages to compare them.

	expected := nodesChatMsgs[0]
	sort.Sort(types.ChatByMessage(expected))

	t.Logf("expected chat messages: %v", expected)
	require.Len(t, expected, len(nodes))

	for i := 1; i < len(nodesChatMsgs); i++ {
		compare := nodesChatMsgs[0]
		sort.Sort(types.ChatByMessage(compare))

		require.Equal(t, expected, compare)
	}

	// > every node should have an entry to every other nodes in their routing
	// tables.

	for _, node := range nodes {
		table := node.GetRoutingTable()
		require.Len(t, table, len(nodes))

		for _, otherNode := range nodes {
			_, ok := table[otherNode.GetAddr()]
			require.True(t, ok)
		}

		// uncomment the following to generate the routing table graphs
		// out, err := os.Create(fmt.Sprintf("node-%s.dot", node.GetAddr()))
		// require.NoError(t, err)

		// table.DisplayGraph(out)
	}
}

// 1-11
//
// Broadcast a rumor message containing a private message. Only the intended
// recipients should execute the message contained in the private message.
func Test_HW1_Messaging_Broadcast_Private_Message(t *testing.T) {

	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)
	handler2, status2 := fake.GetHandler(t)
	handler3, status3 := fake.GetHandler(t)
	handler4, status4 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1), z.WithAntiEntropy(time.Millisecond*50))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2), z.WithAntiEntropy(time.Millisecond*50))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler3), z.WithAntiEntropy(time.Millisecond*50))
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler4), z.WithAntiEntropy(time.Millisecond*50))
	defer node4.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())
	node1.AddPeer(node4.GetAddr())

	fakeMsg := fake.GetNetMsg(t)

	recipients := map[string]struct{}{
		node2.GetAddr(): {},
		node4.GetAddr(): {},
	}

	private := types.PrivateMessage{
		Recipients: recipients,
		Msg:        &fakeMsg,
	}

	data, err := json.Marshal(&private)
	require.NoError(t, err)

	msg := transport.Message{
		Type:    private.Name(),
		Payload: data,
	}

	err = node1.Broadcast(msg)
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	status1.CheckNotCalled(t)
	status2.CheckCalled(t)
	status3.CheckNotCalled(t)
	status4.CheckCalled(t)
}

// 1-12
//
// Send a private message with a unicast call. The message contained in the
// private message should be executed only if the targets address is in the
// recipient list.
//
// Note: Sending a unicast private message is meaningless, but the system should
// allow it if it is implemented correctly. This is a sanity check.
func Test_HW1_Messaging_Unicast_Private_Message(t *testing.T) {

	transp := channel.NewTransport()

	fake := z.NewFakeMessage(t)
	handler1, status1 := fake.GetHandler(t)
	handler2, status2 := fake.GetHandler(t)
	handler3, status3 := fake.GetHandler(t)

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler1))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler2))
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler3))
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node1.AddPeer(node3.GetAddr())

	fakeMsg := fake.GetNetMsg(t)

	recipients := map[string]struct{}{
		node2.GetAddr(): {},
	}

	private := types.PrivateMessage{
		Recipients: recipients,
		Msg:        &fakeMsg,
	}

	data, err := json.Marshal(&private)
	require.NoError(t, err)

	msg := transport.Message{
		Type:    private.Name(),
		Payload: data,
	}

	err = node1.Unicast(node2.GetAddr(), msg)
	require.NoError(t, err)

	err = node1.Unicast(node3.GetAddr(), msg)
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	status1.CheckNotCalled(t)
	status2.CheckCalled(t)
	status3.CheckNotCalled(t)
}
