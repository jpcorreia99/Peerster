package integration

import (
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/registry/proxy"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/storage/file"
)

// 3-20
//
// Call the Tag() function on multiple peers concurrently. The state should be
// consistent for all peers. Mixes student and reference peers.
func Test_HW3_Integration_Multiple_Consensus(t *testing.T) {
	numMessages := 5

	nStudent := 2
	nReference := 2
	totalNodes := nStudent + nReference

	referenceTransp := proxyFac()
	studentTransp := udpFac()

	nodes := make([]z.TestNode, totalNodes)

	for i := 0; i < nStudent; i++ {
		node := z.NewTestNode(t, studentFac, studentTransp, "127.0.0.1:0",
			z.WithTotalPeers(uint(totalNodes)),
			z.WithPaxosID(uint(i+1)))
		nodes[i] = node
	}

	for i := nStudent; i < totalNodes; i++ {
		tmpFolder, err := ioutil.TempDir("", "peerster_test")
		require.NoError(t, err)
		defer os.RemoveAll(tmpFolder)

		storage, err := file.NewPersistency(tmpFolder)
		require.NoError(t, err)

		node := z.NewTestNode(t, referenceFac, referenceTransp, "127.0.0.1:0",
			z.WithMessageRegistry(proxy.NewRegistry()),
			z.WithTotalPeers(uint(totalNodes)),
			z.WithPaxosID(uint(i+1)),
			z.WithStorage(storage))
		nodes[i] = node
	}

	stopNodes := func() {
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
			n, ok := nodes[i+nStudent].Peer.(z.Terminable)
			if ok {
				n.Terminate()
			}
		}
	}

	defer terminateNodes()
	defer stopNodes()

	for _, n1 := range nodes {
		for _, n2 := range nodes {
			n1.AddPeer(n2.GetAddr())
		}
	}

	wait := sync.WaitGroup{}
	wait.Add(totalNodes)

	for _, node := range nodes {
		go func(n z.TestNode) {
			defer wait.Done()

			for i := 0; i < numMessages; i++ {
				name := make([]byte, 12)
				rand.Read(name)

				err := n.Tag(hex.EncodeToString(name), "1")
				require.NoError(t, err)

				time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
			}
		}(node)
	}

	wait.Wait()

	time.Sleep(time.Second * 10)

	lastHashes := map[string]struct{}{}

	for i, node := range nodes {
		t.Logf("node %d", i)

		store := node.GetStorage().GetBlockchainStore()
		require.Equal(t, numMessages*totalNodes+1, store.Len())

		lastHashes[string(store.Get(storage.LastBlockKey))] = struct{}{}

		z.ValidateBlockchain(t, store)

		require.Equal(t, numMessages*totalNodes, node.GetStorage().GetNamingStore().Len())

		z.DisplayLastBlockchainBlock(t, os.Stdout, node.GetStorage().GetBlockchainStore())
	}

	// > all peers must have the same last hash
	require.Len(t, lastHashes, 1)
}
