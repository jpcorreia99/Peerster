package integration

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/registry/proxy"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// 0-8
//
// Make every node send a unicast message to every other nodes.
func Test_HW0_Integration_Unicast(t *testing.T) {
	nReference := 10
	nStudent := 10

	referenceTransp := proxyFac()
	studentTransp := udpFac()

	nodes := make([]z.TestNode, nReference+nStudent)

	for i := 0; i < nReference; i++ {
		node := z.NewTestNode(t, referenceFac, referenceTransp, "127.0.0.1:0", z.WithMessageRegistry(proxy.NewRegistry()))
		nodes[i] = node
	}

	for i := nReference; i < nReference+nStudent; i++ {
		node := z.NewTestNode(t, studentFac, studentTransp, "127.0.0.1:0")
		nodes[i] = node
	}

	defer func() {
		wait := sync.WaitGroup{}
		wait.Add(len(nodes))

		for _, node := range nodes {
			go func(node z.TestNode) {
				defer wait.Done()

				node.Stop()
				n, ok := node.Peer.(z.Terminable)
				if ok {
					n.Terminate()
				}
			}(node)
		}

		wait.Wait()
	}()

	// add everyone as peer
	for _, node := range nodes {
		neighbors := []string{}

		for _, otherNode := range nodes {
			neighbors = append(neighbors, otherNode.GetAddr())
		}

		node.AddPeer(neighbors...)
	}

	chat := types.ChatMessage{
		Message: "this is my chat message",
	}
	data, err := json.Marshal(&chat)
	require.NoError(t, err)

	msg := transport.Message{
		Type:    chat.Name(),
		Payload: data,
	}

	// everyone send a message to everyone, including itself

	for _, node := range nodes {
		for _, otherNode := range nodes {
			err = node.Unicast(otherNode.GetAddr(), msg)
			require.NoError(t, err)
		}
	}

	time.Sleep(time.Second * 4)

	for _, node := range nodes {
		ins := node.GetIns()
		require.Len(t, ins, len(nodes))

		outs := node.GetOuts()
		require.Len(t, outs, len(nodes))
	}
}
