package unit

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// Some tests for HW0 will use the udp network with port 0.

// 0-1
//
// Checking listen/close operations
func Test_HW0_Network_Listen_Close(t *testing.T) {
	net1 := udpFac()

	// > creating socket on a wrong address should raise an error

	_, err := net1.CreateSocket("fake")
	require.Error(t, err)

	sock1, err := net1.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)

	defer func() {
		// > closing should not raise an error
		err := sock1.Close()
		require.NoError(t, err)
	}()

	// > giving port 0 should get a random free port

	require.NotEqual(t, "127.0.0.1:0", sock1.GetAddress())

	err = sock1.Close()
	require.NoError(t, err)

	// > Closing when already closed should produce an error

	err = sock1.Close()
	require.Error(t, err)

	// > I should be able to close and create a socket again

	sock1, err = net1.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	require.NotEqual(t, "127.0.0.1:0", sock1.GetAddress())
}

// 0-2
//
// A simple send/recv
func Test_HW0_Network_Simple(t *testing.T) {
	net1 := udpFac()
	net2 := udpFac()

	sock1, err := net1.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock1.Close()

	sock2, err := net2.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock2.Close()

	// > n1 send to n2

	pkt := z.GetRandomPkt(t)

	err = sock1.Send(sock2.GetAddress(), pkt, 0)
	require.NoError(t, err)

	res, err := sock2.Recv(time.Second)
	require.NoError(t, err)

	require.EqualValues(t, pkt, res)

	// > n2 send to n1

	pkt = z.GetRandomPkt(t)

	err = sock2.Send(sock1.GetAddress(), pkt, 0)
	require.NoError(t, err)

	res, err = sock1.Recv(time.Second)
	require.NoError(t, err)

	require.EqualValues(t, pkt, res)

	// > n1 send to n1

	pkt = z.GetRandomPkt(t)

	err = sock1.Send(sock1.GetAddress(), pkt, 0)
	require.NoError(t, err)

	res, err = sock1.Recv(time.Second)
	require.NoError(t, err)

	require.EqualValues(t, pkt, res)
}

// 0-3
//
// Test multiple send/recv concurrently
func Test_HW0_Network_Multiple(t *testing.T) {
	sendingN1 := 10
	sendingN2 := 10

	net1 := udpFac()
	net2 := udpFac()

	sock1, err := net1.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock1.Close()

	sock2, err := net2.CreateSocket("127.0.0.1:0")
	require.NoError(t, err)
	defer sock2.Close()

	n1Addr := sock1.GetAddress()
	n2Addr := sock2.GetAddress()

	n1Received := []transport.Packet{}
	n2Received := []transport.Packet{}

	n1Sent := []transport.Packet{}
	n2Sent := []transport.Packet{}

	stop := make(chan struct{})

	rcvWG := sync.WaitGroup{}
	rcvWG.Add(2)

	// Receiving loop for n1
	go func() {
		defer rcvWG.Done()

		for {
			select {
			case <-stop:
				return
			default:
				pkt, err := sock1.Recv(time.Millisecond * 10)
				if errors.Is(err, transport.TimeoutErr(0)) {
					continue
				}

				require.NoError(t, err)
				n1Received = append(n1Received, pkt)
			}
		}
	}()

	// Receiving loop for n2
	go func() {
		defer rcvWG.Done()

		for {
			select {
			case <-stop:
				return
			default:
				pkt, err := sock2.Recv(time.Millisecond * 10)
				if errors.Is(err, transport.TimeoutErr(0)) {
					continue
				}

				require.NoError(t, err)
				n2Received = append(n2Received, pkt)
			}
		}
	}()

	sendWG := sync.WaitGroup{}
	sendWG.Add(2)

	// Sending loop for n1
	go func() {
		defer sendWG.Done()

		for i := 0; i < sendingN1; i++ {
			pkt := z.GetRandomPkt(t)
			n1Sent = append(n1Sent, pkt)
			err := sock1.Send(n2Addr, pkt, 0)
			require.NoError(t, err)
		}
	}()

	// Sending loop for n2
	go func() {
		defer sendWG.Done()

		for i := 0; i < sendingN2; i++ {
			pkt := z.GetRandomPkt(t)
			n2Sent = append(n2Sent, pkt)
			err := sock1.Send(n1Addr, pkt, 0)
			require.NoError(t, err)
		}
	}()

	// wait for both nodes to send their packets
	sendWG.Wait()

	time.Sleep(time.Second * 1)

	close(stop)

	// wait for listening node to finish
	rcvWG.Wait()

	sort.Sort(transport.ByPacketID(n1Received))
	sort.Sort(transport.ByPacketID(n2Received))
	sort.Sort(transport.ByPacketID(n1Sent))
	sort.Sort(transport.ByPacketID(n2Sent))

	require.Equal(t, n1Sent, n2Received)
	require.Equal(t, n2Sent, n1Received)
}

// 0-4
//
// Test the AddPeer function
func Test_HW0_Messaging_AddPeer(t *testing.T) {
	transp := channelFac()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:1")
	defer node1.Stop()

	// > at the begining the routing table should only contain the entry to
	// ourself

	table := node1.GetRoutingTable()
	require.Len(t, table, 1)
	require.Equal(t, table[node1.GetAddr()], node1.GetAddr())

	// > if I add a new peer with my address, it should be ignored

	node1.AddPeer("127.0.0.1:1")

	table = node1.GetRoutingTable()
	require.Len(t, table, 1)
	require.Equal(t, table[node1.GetAddr()], node1.GetAddr())

	// > if I add new peers, the routing table should contain the new peers

	node1.AddPeer("fake1", "fake2")
	table = node1.GetRoutingTable()
	require.Len(t, table, 3)

	entry := table["fake1"]
	require.Equal(t, "fake1", entry)

	entry = table["fake2"]
	require.Equal(t, "fake2", entry)

	require.Equal(t, table[node1.GetAddr()], node1.GetAddr())
}

// 0-9
//
// Test the SetRoutingEntry function
func Test_HW0_Messaging_SetRoutingEntry(t *testing.T) {
	transp := channelFac()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:1", z.WithAutostart(false))

	// > initial routing table should have one element

	table := node1.GetRoutingTable()
	require.Len(t, table, 1)
	require.Equal(t, node1.GetAddr(), table[node1.GetAddr()])

	// > add a new entry, checking if it is there

	node1.SetRoutingEntry("a", "b")
	table = node1.GetRoutingTable()

	require.Len(t, table, 2)
	require.Equal(t, node1.GetAddr(), table[node1.GetAddr()])
	require.Equal(t, table["a"], "b")

	// > update the routing entry

	node1.SetRoutingEntry("a", "c")
	table = node1.GetRoutingTable()

	require.Len(t, table, 2)
	require.Equal(t, node1.GetAddr(), table[node1.GetAddr()])
	require.Equal(t, table["a"], "c")

	// > delete the entry

	node1.SetRoutingEntry("a", "")
	table = node1.GetRoutingTable()

	require.Len(t, table, 1)
	require.Equal(t, node1.GetAddr(), table[node1.GetAddr()])
}

// 0-5
//
// Check that I can register a handler that is correctly called by the peer.
func Test_HW0_Service_RegisterNotify(t *testing.T) {
	// test case with a provided transport
	getTest := func(transp transport.Transport) func(*testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			fake := z.NewFakeMessage(t)
			registryHandler, status := fake.GetHandler(t)

			node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0",
				z.WithMessage(fake, registryHandler))
			defer node1.Stop()

			handlerCalled := make(chan struct{})
			handler := func(m types.Message, pkt transport.Packet) error {
				close(handlerCalled)
				return nil
			}

			node1.GetRegistry().RegisterNotify(handler)

			err := node1.Unicast(node1.GetAddr(), fake.GetNetMsg(t))
			require.NoError(t, err)

			time.Sleep(time.Millisecond * 500)

			// the handlers should've been called
			select {
			case <-handlerCalled:
			case <-time.After(time.Second):
				t.Error("handler not called after timeout")
			}

			status.CheckCalled(t)
		}
	}

	t.Run("channel transport", getTest(channelFac()))
	t.Run("UDP transport", getTest(udpFac()))
}

// 0-5
//
// If I send a unicast to a known node, then that node should receive the
// packet and execute the message handler.
func Test_HW0_Messaging_Unicast(t *testing.T) {
	// test case with a provided transport
	getTest := func(transp transport.Transport) func(*testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			fake := z.NewFakeMessage(t)
			handler, status := fake.GetHandler(t)

			node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, nil))
			defer node1.Stop()

			node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler))
			defer node2.Stop()

			node1.AddPeer(node2.GetAddr())

			err := node1.Unicast(node2.GetAddr(), fake.GetNetMsg(t))
			require.NoError(t, err)

			time.Sleep(time.Second)

			n1Outs := node1.GetOuts()
			n1Ins := node1.GetIns()

			n2Outs := node2.GetOuts()
			n2Ins := node2.GetIns()

			// > n1 should have not received any packet

			require.Len(t, n1Ins, 0)

			// > n1 should've sent only 1 packet

			require.Len(t, n1Outs, 1)
			pkt := n1Outs[0]

			require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			fake.Compare(t, pkt.Msg)

			// > n2 should have received only 1 fake packet

			require.Len(t, n2Ins, 1)
			pkt = n2Ins[0]

			require.Equal(t, node2.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			fake.Compare(t, pkt.Msg)

			// > n2 should have not sent any packet

			require.Len(t, n2Outs, 0)

			// > the handle should've been called

			status.CheckCalled(t)
		}
	}

	t.Run("channel transport", getTest(channelFac()))
	t.Run("UDP transport", getTest(udpFac()))
}

// 0-6
//
// If I send a unicast to an unknown node, then I should get an error and the
// other node should not receive the packet.
func Test_HW0_Messaging_Unicast_Fail(t *testing.T) {
	// test case with a provided transport
	getTest := func(transp transport.Transport) func(*testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
			defer node1.Stop()

			node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
			defer node2.Stop()

			chat := types.ChatMessage{
				Message: "this is my chat message",
			}
			data, err := json.Marshal(&chat)
			require.NoError(t, err)

			msg := transport.Message{
				Type:    chat.Name(),
				Payload: data,
			}

			err = node1.Unicast(node2.GetAddr(), msg)
			require.Error(t, err)

			time.Sleep(time.Second)

			n1Ins := node1.GetIns()
			n2Ins := node2.GetIns()

			// > n1 should have not received any packet

			require.Len(t, n1Ins, 0)

			// > n2 should have not received any packet

			require.Len(t, n2Ins, 0)
		}
	}

	t.Run("channel transport", getTest(channelFac()))
	t.Run("UDP transport", getTest(udpFac()))
}

// 0-7
//
// If a node receives a message with a destination address not equal to its
// address, then it should relay the message using its routing table. In this
// case node2 relays message from node1 to node3. We manually set the routing
// table with SetRoutingEntry().
func Test_HW0_Messaging_Relaying(t *testing.T) {
	// test case with a provided transport
	getTest := func(transp transport.Transport) func(*testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			fake := z.NewFakeMessage(t)
			handler, status := fake.GetHandler(t)

			node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, nil))
			defer node1.Stop()

			node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, nil))
			defer node2.Stop()

			node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithMessage(fake, handler))
			defer node3.Stop()

			//          relay
			// node1 --(node2)--> node3
			node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
			// node2 --> node3
			node2.AddPeer(node3.GetAddr())

			err := node1.Unicast(node3.GetAddr(), fake.GetNetMsg(t))
			require.NoError(t, err)

			time.Sleep(time.Second)

			n1Outs := node1.GetOuts()
			n1Ins := node1.GetIns()

			n2Outs := node2.GetOuts()
			n2Ins := node2.GetIns()

			n3Outs := node3.GetOuts()
			n3Ins := node3.GetIns()

			// > n1 should have not received any packet

			require.Len(t, n1Ins, 0)

			// > n1 should've sent only 1 packet

			require.Len(t, n1Outs, 1)
			pkt := n1Outs[0]

			require.Equal(t, node3.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			fake.Compare(t, pkt.Msg)

			// > n2 should have received only 1 fake packet

			require.Len(t, n2Ins, 1)
			pkt = n2Ins[0]

			require.Equal(t, node3.GetAddr(), pkt.Header.Destination)

			// This check is commented for the spring 2021 session because of a
			// bug found too late on the transport implementation. See PR #47.
			//
			// require.Equal(t, node1.GetAddr(), pkt.Header.RelayedBy)

			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			fake.Compare(t, pkt.Msg)

			// > n2 should have sent one packet

			require.Len(t, n2Outs, 1)
			pkt = n2Outs[0]

			require.Equal(t, node3.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node2.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			fake.Compare(t, pkt.Msg)

			// > n3 should have received only 1 fake packet

			require.Len(t, n3Ins, 1)
			pkt = n3Ins[0]

			require.Equal(t, node3.GetAddr(), pkt.Header.Destination)
			require.Equal(t, node2.GetAddr(), pkt.Header.RelayedBy)
			require.Equal(t, node1.GetAddr(), pkt.Header.Source)

			fake.Compare(t, pkt.Msg)

			// > n3 should have not sent any packet

			require.Len(t, n3Outs, 0)

			// > the handler should've been called

			status.CheckCalled(t)
		}
	}

	t.Run("channel transport", getTest(channelFac()))
	t.Run("UDP transport", getTest(udpFac()))
}
