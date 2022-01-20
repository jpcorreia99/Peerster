package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/cs438/internal/graph"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/registry/proxy"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// 1-13
//
// Make every node send a broadcast message to every other nodes, with a random
// topology.
func Test_HW1_Integration_Broadcast(t *testing.T) {

	rand.Seed(1)

	nReference := 10
	nStudent := 10

	chatMsg := "hi from %s"
	stopped := false

	referenceTransp := proxyFac()
	studentTransp := udpFac()

	antiEntropy := time.Second * 10
	ackTimeout := time.Second * 10
	waitPerNode := time.Second * 2

	if runtime.GOOS == "windows" {
		antiEntropy = time.Second * 20
		ackTimeout = time.Second * 90
		waitPerNode = time.Second * 15
	}

	nodes := make([]z.TestNode, nReference+nStudent)

	for i := 0; i < nReference; i++ {
		node := z.NewTestNode(t, referenceFac, referenceTransp, "127.0.0.1:0",
			z.WithMessageRegistry(proxy.NewRegistry()),
			z.WithAntiEntropy(antiEntropy),
			// since everyone is sending a rumor, there is no need to have route
			// rumors
			z.WithHeartbeat(0),
			z.WithAckTimeout(ackTimeout))

		nodes[i] = node
	}

	for i := nStudent; i < nReference+nStudent; i++ {
		node := z.NewTestNode(t, studentFac, studentTransp, "127.0.0.1:0",
			z.WithAntiEntropy(antiEntropy),
			// since everyone is sending a rumor, there is no need to have route
			// rumors
			z.WithHeartbeat(0),
			z.WithAckTimeout(ackTimeout))

		nodes[i] = node
	}

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

	terminateNodes := func() {
		for i := 0; i < nReference; i++ {
			n, ok := nodes[i].Peer.(z.Terminable)
			if ok {
				n.Terminate()
			}
		}
	}

	defer terminateNodes()
	defer stopNodes()

	// generate and apply a random topology
	// out, err := os.Create("topology.dot")
	// require.NoError(t, err)
	graph.NewGraph(0.2).Generate(io.Discard, nodes)

	// > make each node broadcast a rumor, each node should eventually get
	// rumors from all the other nodes.

	wait := sync.WaitGroup{}
	wait.Add(len(nodes))

	startT := time.Now()

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

	time.Sleep(waitPerNode * time.Duration(nReference+nStudent))

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

	elapsed := time.Since(startT)

	// will stop the nodes but not the proxies, because we still need to fetch
	// data.
	stopNodes()

	// > check that each node got all the chat messages

	nodesChatMsgs := make([][]*types.ChatMessage, len(nodes))

	wait = sync.WaitGroup{}
	wait.Add(len(nodes))

	out := new(strings.Builder)
	l := sync.Mutex{}
	fmt.Fprintf(out, "stats on %s\n", runtime.GOOS)
	fmt.Fprintf(out, "address, received [pkt/s], sent [pkt/s]\n")

	for i, node := range nodes {
		// fetching messages can take a bit of time with the proxy, which is why
		// we do it concurrently.
		go func(i int, node z.TestNode) {
			defer wait.Done()

			chatMsgs := node.GetChatMsgs()
			nodesChatMsgs[i] = chatMsgs

			totSent := len(node.GetOuts())
			totReceived := len(node.GetIns())

			// print stats of throughput for each node
			l.Lock()
			fmt.Fprintf(out, "%s, %f, %f\n", node.GetAddr(),
				float64(totReceived)/float64(elapsed.Seconds()),
				float64(totSent)/float64(elapsed.Seconds()))
			l.Unlock()
		}(i, node)
	}

	done2 := make(chan struct{})

	go func() {
		select {
		case <-done2:
		case <-time.After(time.Minute * 5):
			t.Error("timeout on fetch message")
		}
	}()

	wait.Wait()
	close(done2)

	t.Log(out.String())

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
		require.Len(t, table, len(nodes), node.GetAddr())

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
