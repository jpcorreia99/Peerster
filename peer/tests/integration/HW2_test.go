package integration

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/registry/proxy"
)

// Integration test, with the following topology where * are reference nodes:
// ┌────────────┐
// ▼            │
// C* ────► D   │
// │        │   │
// ▼        ▼   │
// A ◄────► B* ─┘
func Test_HW2_Integration_Scenario(t *testing.T) {
	referenceTransp := proxyFac()
	studentTransp := udpFac()

	chunkSize := uint(2)

	studentOpts := []z.Option{
		z.WithChunkSize(chunkSize),
		z.WithHeartbeat(time.Second * 200),
		z.WithAntiEntropy(time.Second * 5),
		z.WithAckTimeout(time.Second * 10),
	}
	refOpts := append(studentOpts, z.WithMessageRegistry(proxy.NewRegistry()))

	nodeA := z.NewTestNode(t, studentFac, studentTransp, "127.0.0.1:0", studentOpts...)
	nodeB := z.NewTestNode(t, referenceFac, referenceTransp, "127.0.0.1:0", refOpts...)
	nodeC := z.NewTestNode(t, referenceFac, referenceTransp, "127.0.0.1:0", refOpts...)
	nodeD := z.NewTestNode(t, studentFac, studentTransp, "127.0.0.1:0", studentOpts...)

	defer func() {
		nodeA.Stop()
		nodeB.Stop()
		nodeB.Peer.(z.Terminable).Terminate()
		nodeC.Stop()
		nodeC.Peer.(z.Terminable).Terminate()
		nodeD.Stop()
	}()

	nodeA.AddPeer(nodeB.GetAddr())
	nodeB.AddPeer(nodeA.GetAddr())
	nodeB.AddPeer(nodeC.GetAddr())
	nodeC.AddPeer(nodeA.GetAddr())
	nodeC.AddPeer(nodeD.GetAddr())
	nodeD.AddPeer(nodeB.GetAddr())

	// Wait for the anti-entropy to take effect, i.e. everyone gets the
	// heartbeat message from everyone else.
	time.Sleep(time.Second * 10)

	// > If I upload a file on NodeB I should be able to download it from NodeB

	fileB := []byte("lorem ipsum dolor sit amet")
	mhB, err := nodeB.Upload(bytes.NewBuffer(fileB))
	require.NoError(t, err)

	res, err := nodeB.Download(mhB)
	require.NoError(t, err)
	require.Equal(t, fileB, res)

	// Let's tag file on NodeB
	nodeB.Tag("fileB", mhB)

	// > NodeA should be able to index file from B
	names, err := nodeA.SearchAll(*regexp.MustCompile("file.*"), 3, time.Second*2)
	require.NoError(t, err)
	require.Len(t, names, 1)
	require.Equal(t, "fileB", names[0])

	// > NodeA should have added "fileB" in its naming storage
	mhB2 := nodeA.Resolve(names[0])
	require.Equal(t, mhB, mhB2)

	// > I should be able to download fileB from nodeA
	res, err = nodeA.Download(mhB)
	require.NoError(t, err)
	require.Equal(t, fileB, res)

	// > NodeC should be able to index fileB from A
	names, err = nodeC.SearchAll(*regexp.MustCompile("fileB"), 3, time.Second*4)
	require.NoError(t, err)
	require.Len(t, names, 1)
	require.Equal(t, "fileB", names[0])

	// > NodeC should have added "fileB" in its naming storage
	mhB2 = nodeC.Resolve(names[0])
	require.Equal(t, mhB, mhB2)

	// > I should be able to download fileB from nodeC
	res, err = nodeC.Download(mhB)
	require.NoError(t, err)
	require.Equal(t, fileB, res)

	// Add a file on node D
	fileD := []byte("this is the file D")
	mhD, err := nodeD.Upload(bytes.NewBuffer(fileD))
	require.NoError(t, err)
	nodeD.Tag("fileD", mhD)

	// > NodeA should be able to search for "fileD" using the expanding scheme
	conf := peer.ExpandingRing{
		Initial: 1,
		Factor:  2,
		Retry:   5,
		Timeout: time.Second * 2,
	}
	name, err := nodeA.SearchFirst(*regexp.MustCompile("fileD"), conf)
	require.NoError(t, err)
	require.Equal(t, "fileD", name)

	// > NodeA should be able to download fileD
	mhD2 := nodeA.Resolve(name)
	require.Equal(t, mhD, mhD2)

	res, err = nodeA.Download(mhD2)
	require.NoError(t, err)
	require.Equal(t, fileD, res)

	// Let's add new nodes and see if they can index the files and download them

	//             ┌───────────┐
	//             ▼           │
	// F* ──► E ─► C ────► D   │
	//             │       │   │
	//             ▼       ▼   │
	//             A ◄───► B ◄─┘

	nodeE := z.NewTestNode(t, studentFac, studentTransp, "127.0.0.1:0", studentOpts...)
	nodeF := z.NewTestNode(t, referenceFac, referenceTransp, "127.0.0.1:0", refOpts...)

	defer func() {
		nodeE.Stop()
		nodeF.Stop()
		nodeF.Peer.(z.Terminable).Terminate()
	}()

	nodeE.AddPeer(nodeC.GetAddr())
	nodeF.AddPeer(nodeE.GetAddr())

	// wait for the anti-entropy to take effect, i.e. everyone get the heartbeat
	// messages sent by nodeE and nodeF.
	time.Sleep(time.Second * 10)

	// > NodeF should be able to index all files (2)

	names, err = nodeF.SearchAll(*regexp.MustCompile(".*"), 8, time.Second*4)
	require.NoError(t, err)
	require.Len(t, names, 2)
	require.Contains(t, names, "fileB")
	require.Contains(t, names, "fileD")

	// > NodeE should be able to search for fileB
	conf = peer.ExpandingRing{
		Initial: 1,
		Factor:  2,
		Retry:   4,
		Timeout: time.Second * 2,
	}
	name, err = nodeE.SearchFirst(*regexp.MustCompile("fileB"), conf)
	require.NoError(t, err)
	require.Equal(t, "fileB", name)
}
