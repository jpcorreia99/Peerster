package unit

import (
	"bufio"
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/channel"
)

// 2-1
//
// Use the Upload() function and check if the storage is updated.
func Test_HW2_Upload_Simple(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(3), z.WithAutostart(false))
	defer node1.Stop()

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}, {'c'}}
	data := append(chunks[0], append(chunks[1], chunks[2]...)...)

	// sha256 of each chunk, computed by hand
	chunkHashes := []string{
		"9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0",
		"3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677",
		"2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6",
	}

	// metahash, computed by hand
	mh := "5a00fc30e073b095a6266136552a3da1d4622d0fdaa057f0b3135aa803321e1c"

	buf := bytes.NewBuffer(data)

	// > we should be able to store data without error
	metaHash, err := node1.Upload(buf)
	require.NoError(t, err)

	// > returned metahash should be the expected one
	require.Equal(t, mh, metaHash)

	store := node1.GetStorage().GetDataBlobStore()

	// > val should contain 3 lines, one for each hex encoded hash of chunk.
	// Recall that we set a chunk size equals to 3.
	val := store.Get(mh)
	chunkHexKeys := strings.Split(string(val), peer.MetafileSep)

	require.Len(t, chunkHexKeys, len(chunks))

	for i, chunkHash := range chunkHashes {
		require.Equal(t, chunkHash, chunkHexKeys[i])

		chunkData := store.Get(chunkHexKeys[i])
		require.Equal(t, chunks[i], chunkData)
	}
}

// 2-2
//
// Use the Upload() function with data multiple of the chunk size.
func Test_HW2_Upload_Round(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithChunkSize(3), z.WithAutostart(false))
	defer node1.Stop()

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}
	data := append(chunks[0], chunks[1]...)

	// sha256 of each chunk, computed by hand
	chunkHashes := []string{
		"9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0",
		"3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677",
	}

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	buf := bytes.NewBuffer(data)

	// > we should be able to store data without error
	metaHash, err := node1.Upload(buf)
	require.NoError(t, err)

	// > returned metahash should be the expected one
	require.Equal(t, mh, metaHash)

	store := node1.GetStorage().GetDataBlobStore()

	// > val should contain 3 lines, one for each hex encoded hash of chunk
	val := store.Get(mh)
	scanner := bufio.NewScanner(bytes.NewBuffer(val))

	for i, chunkHash := range chunkHashes {
		ok := scanner.Scan()
		require.True(t, ok)

		require.Equal(t, chunkHash, scanner.Text())

		chunkData := store.Get(scanner.Text())
		require.Equal(t, chunks[i], chunkData)
	}

	// > there should be no additional lines

	ok := scanner.Scan()
	require.False(t, ok)
}

// 2-3
//
// Check the GetCatalog() and UpdateCatalog()
func Test_HW2_Catalog(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	// > the catalog should be empty

	catalog := node1.GetCatalog()
	require.Len(t, catalog, 0)

	// > add a new element to the catalog

	key := "aef123"
	peer := "A"
	node1.UpdateCatalog(key, peer)

	// > the catalog should have the new element

	catalog = node1.GetCatalog()
	require.Len(t, catalog, 1)
	require.Len(t, catalog[key], 1)
	require.Contains(t, catalog[key], peer)
}

// 2-4
//
// Check the Download() function
func Test_HW2_Download_Fail_inexistent_File(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	// > If I try to download something that the node doesn't have and doesn't
	// exist in its catalog, then I should get an error.

	key := "aef123"

	_, err := node1.Download(string(key))
	require.Error(t, err)
}

// 2-5
//
// Download a file that the peer has completely locally.
func Test_HW2_Download_Local(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	// Setting a file (chunks + metahash) in the node's storage.

	storage := node1.GetStorage().GetDataBlobStore()

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}
	data := append(chunks[0], chunks[1]...)

	// sha256 of each chunk, computed by hand
	chunkHashes := []string{
		"9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0",
		"3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677",
	}

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	storage.Set(chunkHashes[0], chunks[0])
	storage.Set(chunkHashes[1], chunks[1])
	storage.Set(mh, []byte(fmt.Sprintf("%s%s%s", chunkHashes[0], peer.MetafileSep, chunkHashes[1])))

	// > If the node has the file in its storage storage, I should be able
	// to download it.

	buf, err := node1.Download(mh)
	require.NoError(t, err)
	require.Equal(t, data, buf)
}

// 2-6
//
// The peer A doesn't have the file locally and has no neighbor. Even it A's
// catalog is telling that B has the file, it can't get it because B is not in
// the routing table of A.
func Test_HW2_Download_Remote_No_Routing(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	// Setting a file (chunks + metahash) in the node2's storage.

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}

	// sha256 of each chunk, computed by hand
	c1 := "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"
	c2 := "3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677"

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	storage := node2.GetStorage().GetDataBlobStore()
	storage.Set(c1, chunks[0])
	storage.Set(c2, chunks[1])
	storage.Set(mh, []byte(fmt.Sprintf("%s\n%s", c1, c2)))

	// telling node1 that node2 has the data

	node1.UpdateCatalog(c1, node2.GetAddr())
	node1.UpdateCatalog(c2, node2.GetAddr())
	node1.UpdateCatalog(string(mh), node2.GetAddr())

	// > Node1 should produce an error because Node 2 is unknown to him.

	_, err := node1.Download(mh)
	require.Error(t, err)
}

// 2-7
//
// A fetches a file from B, but doesn't work because B doesn't know A and can't
// send back the reply.
func Test_HW2_Download_Remote_OneWay_Routing(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithDataRequestBackoff(time.Millisecond*100, 2, 2))
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())

	// Setting a file (chunks + metahash) in the node2's storage.

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}

	// sha256 of each chunk, computed by hand
	c1 := "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"
	c2 := "3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677"

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	storage := node2.GetStorage().GetDataBlobStore()
	storage.Set(c1, chunks[0])
	storage.Set(c2, chunks[1])
	storage.Set(mh, []byte(fmt.Sprintf("%s\n%s", c1, c2)))

	// telling node1 that node2 has the data

	node1.UpdateCatalog(c1, node2.GetAddr())
	node1.UpdateCatalog(c2, node2.GetAddr())
	node1.UpdateCatalog(string(mh), node2.GetAddr())

	// > Node1 should produce an error because Node 2 doesn't know Node 1. So
	// Node 2 won't be able to send back the reply.

	_, err := node1.Download(mh)
	require.Error(t, err)
}

// 2-8
//
// Download a file that the peer doesn't have locally.
// With the following topology:
// A <-> B
func Test_HW2_Download_Remote(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// Setting a file (chunks + metahash) in the node2's storage.

	storage := node2.GetStorage().GetDataBlobStore()

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}
	data := append(chunks[0], chunks[1]...)

	// sha256 of each chunk, computed by hand
	c1 := "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"
	c2 := "3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677"

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	storage.Set(c1, chunks[0])
	storage.Set(c2, chunks[1])
	storage.Set(mh, []byte(fmt.Sprintf("%s\n%s", c1, c2)))

	// telling node1 that node2 has the data

	node1.UpdateCatalog(c1, node2.GetAddr())
	node1.UpdateCatalog(c2, node2.GetAddr())
	node1.UpdateCatalog(string(mh), node2.GetAddr())

	// > Node1 should get the data

	buf, err := node1.Download(mh)
	require.NoError(t, err)
	require.Equal(t, data, buf)

	// > Node1 should have sent 3 data requests

	n1outs := node1.GetOuts()

	require.Len(t, n1outs, 3)

	msg := z.GetDataRequest(t, n1outs[0].Msg)
	require.Equal(t, string(mh), msg.Key)
	reqID1 := msg.RequestID
	require.Equal(t, node2.GetAddr(), n1outs[0].Header.Destination)
	require.Equal(t, node1.GetAddr(), n1outs[0].Header.Source)
	require.Equal(t, node1.GetAddr(), n1outs[0].Header.RelayedBy)

	msg = z.GetDataRequest(t, n1outs[1].Msg)
	require.Equal(t, c1, msg.Key)
	reqID2 := msg.RequestID
	require.Equal(t, node2.GetAddr(), n1outs[1].Header.Destination)
	require.Equal(t, node1.GetAddr(), n1outs[1].Header.Source)
	require.Equal(t, node1.GetAddr(), n1outs[1].Header.RelayedBy)

	msg = z.GetDataRequest(t, n1outs[2].Msg)
	require.Equal(t, c2, msg.Key)
	reqID3 := msg.RequestID
	require.Equal(t, node2.GetAddr(), n1outs[2].Header.Destination)
	require.Equal(t, node1.GetAddr(), n1outs[2].Header.Source)
	require.Equal(t, node1.GetAddr(), n1outs[2].Header.RelayedBy)

	// > Node1 should have received 3 data replies

	n1ins := node1.GetIns()

	require.Len(t, n1ins, 3)

	msg2 := z.GetDataReply(t, n1ins[0].Msg)
	require.Equal(t, string(mh), msg2.Key)
	require.Equal(t, fmt.Sprintf("%s\n%s", c1, c2), string(msg2.Value))
	require.Equal(t, reqID1, msg2.RequestID)
	require.Equal(t, node1.GetAddr(), n1ins[0].Header.Destination)
	require.Equal(t, node2.GetAddr(), n1ins[0].Header.Source)
	require.Equal(t, node2.GetAddr(), n1ins[0].Header.RelayedBy)

	msg2 = z.GetDataReply(t, n1ins[1].Msg)
	require.Equal(t, c1, msg2.Key)
	require.Equal(t, chunks[0], msg2.Value)
	require.Equal(t, reqID2, msg2.RequestID)
	require.Equal(t, node1.GetAddr(), n1ins[1].Header.Destination)
	require.Equal(t, node2.GetAddr(), n1ins[1].Header.Source)
	require.Equal(t, node2.GetAddr(), n1ins[1].Header.RelayedBy)

	msg2 = z.GetDataReply(t, n1ins[2].Msg)
	require.Equal(t, c2, msg2.Key)
	require.Equal(t, chunks[1], msg2.Value)
	require.Equal(t, reqID3, msg2.RequestID)
	require.Equal(t, node1.GetAddr(), n1ins[2].Header.Destination)
	require.Equal(t, node2.GetAddr(), n1ins[2].Header.Source)
	require.Equal(t, node2.GetAddr(), n1ins[2].Header.RelayedBy)

	// > Node2 should have received 3 data requests

	n2ins := node2.GetIns()

	require.Len(t, n2ins, 3)

	msg = z.GetDataRequest(t, n2ins[0].Msg)
	require.Equal(t, string(mh), msg.Key)
	reqID1 = msg.RequestID
	require.Equal(t, node2.GetAddr(), n2ins[0].Header.Destination)
	require.Equal(t, node1.GetAddr(), n2ins[0].Header.Source)
	require.Equal(t, node1.GetAddr(), n2ins[0].Header.RelayedBy)

	msg = z.GetDataRequest(t, n2ins[1].Msg)
	require.Equal(t, c1, msg.Key)
	reqID2 = msg.RequestID
	require.Equal(t, node2.GetAddr(), n2ins[1].Header.Destination)
	require.Equal(t, node1.GetAddr(), n2ins[1].Header.Source)
	require.Equal(t, node1.GetAddr(), n2ins[1].Header.RelayedBy)

	msg = z.GetDataRequest(t, n2ins[2].Msg)
	require.Equal(t, c2, msg.Key)
	reqID3 = msg.RequestID
	require.Equal(t, node2.GetAddr(), n2ins[2].Header.Destination)
	require.Equal(t, node1.GetAddr(), n2ins[2].Header.Source)
	require.Equal(t, node1.GetAddr(), n2ins[2].Header.RelayedBy)

	// > Node2 should have sent 3 data replies

	n2outs := node2.GetOuts()

	require.Len(t, n2outs, 3)

	msg2 = z.GetDataReply(t, n2outs[0].Msg)
	require.Equal(t, string(mh), msg2.Key)
	require.Equal(t, fmt.Sprintf("%s\n%s", c1, c2), string(msg2.Value))
	require.Equal(t, reqID1, msg2.RequestID)
	require.Equal(t, node1.GetAddr(), n2outs[0].Header.Destination)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.RelayedBy)

	msg2 = z.GetDataReply(t, n2outs[1].Msg)
	require.Equal(t, c1, msg2.Key)
	require.Equal(t, chunks[0], msg2.Value)
	require.Equal(t, reqID2, msg2.RequestID)
	require.Equal(t, node1.GetAddr(), n2outs[1].Header.Destination)
	require.Equal(t, node2.GetAddr(), n2outs[1].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[1].Header.RelayedBy)

	msg2 = z.GetDataReply(t, n2outs[2].Msg)
	require.Equal(t, c2, msg2.Key)
	require.Equal(t, chunks[1], msg2.Value)
	require.Equal(t, reqID3, msg2.RequestID)
	require.Equal(t, node1.GetAddr(), n2outs[2].Header.Destination)
	require.Equal(t, node2.GetAddr(), n2outs[2].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[2].Header.RelayedBy)
}

// 2-9
//
// Download a file that the peer has partially locally.
// A <-> B
func Test_HW2_Download_Remote_And_Local(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// Setting a file (chunks + metahash) in the node1's storage. Chunk n°2 will
	// only be available on node 2.

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}
	data := append(chunks[0], chunks[1]...)

	// sha256 of each chunk, computed by hand
	c1 := "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"
	c2 := "3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677"

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	storage := node1.GetStorage().GetDataBlobStore()
	storage.Set(c1, chunks[0])
	storage.Set(mh, []byte(fmt.Sprintf("%s\n%s", c1, c2)))

	storage = node2.GetStorage().GetDataBlobStore()
	storage.Set(c2, chunks[1])

	// telling node1 that node2 has the data.

	node1.UpdateCatalog(c1, node2.GetAddr())
	node1.UpdateCatalog(c2, node2.GetAddr())
	node1.UpdateCatalog(string(mh), node2.GetAddr())

	// > Node1 should get the data

	buf, err := node1.Download(mh)
	require.NoError(t, err)
	require.Equal(t, data, buf)

	// > Node1 should have sent one request

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 1)
	msg := z.GetDataRequest(t, n1outs[0].Msg)
	require.Equal(t, c2, msg.Key)

	// > Node2 should have sent one reply

	n2outs := node2.GetOuts()
	require.Len(t, n2outs, 1)
	msg2 := z.GetDataReply(t, n2outs[0].Msg)
	require.Equal(t, c2, msg2.Key)
	require.Equal(t, chunks[1], msg2.Value)
}

// 2-10
//
// Download a file that the peer has partially locally. C1 is in A and C2 in C:
// A <-> B <-> C
func Test_HW2_Download_Remote_And_Local_With_relay(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	node1.SetRoutingEntry(node3.GetAddr(), node2.GetAddr())
	node3.SetRoutingEntry(node1.GetAddr(), node2.GetAddr())

	// Setting a file (chunks + metahash) in the node1's storage. Chunk n°2 will
	// only be available on node 3.

	chunks := [][]byte{{'a', 'a', 'a'}, {'b', 'b', 'b'}}
	data := append(chunks[0], chunks[1]...)

	// sha256 of each chunk, computed by hand
	c1 := "9834876dcfb05cb167a5c24953eba58c4ac89b1adf57f28f2f9d09af107ee8f0"
	c2 := "3e744b9dc39389baf0c5a0660589b8402f3dbb49b89b3e75f2c9355852a3c677"

	// metahash, computed by hand
	mh := "6a0b1d67884e58786e97bc51544cbba4cc3e1279d8ff46da2fa32bcdb44a053e"

	storage := node1.GetStorage().GetDataBlobStore()
	storage.Set(c1, chunks[0])
	storage.Set(mh, []byte(fmt.Sprintf("%s\n%s", c1, c2)))

	storage = node3.GetStorage().GetDataBlobStore()
	storage.Set(c2, chunks[1])

	// telling node1 that node3 has the data.

	node1.UpdateCatalog(c1, node3.GetAddr())
	node1.UpdateCatalog(c2, node3.GetAddr())
	node1.UpdateCatalog(string(mh), node3.GetAddr())

	// > Node1 should get the data

	buf, err := node1.Download(mh)
	require.NoError(t, err)
	require.Equal(t, data, buf)

	// > Node1 should have sent one request

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 1)
	msg := z.GetDataRequest(t, n1outs[0].Msg)
	require.Equal(t, c2, msg.Key)

	// > Node3 should have sent one reply

	n3outs := node3.GetOuts()
	require.Len(t, n3outs, 1)
	msg2 := z.GetDataReply(t, n3outs[0].Msg)
	require.Equal(t, c2, msg2.Key)
	require.Equal(t, chunks[1], msg2.Value)
}

// 2-11
//
// Test the Tag function
func Test_HW2_Tag(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	mh := "aef123"
	name := "abc"

	err := node1.Tag(name, mh)
	require.NoError(t, err)

	storage := node1.GetStorage().GetNamingStore()
	buf := storage.Get(name)
	require.Equal(t, mh, string(buf))
}

// 2-12
//
// Test the resolve function
func Test_HW2_Resolve(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	mh := "aef123"
	name := "abc"

	storage := node1.GetStorage().GetNamingStore()
	storage.Set(name, []byte(mh))

	mh2 := node1.Resolve(name)
	require.Equal(t, mh, mh2)

	// > if the name doesn't exist it should return an empty string

	mh2 = node1.Resolve("dummy")
	require.Empty(t, mh2)
}

// 2-13
//
// Test the combination of Tag + Resolve
func Test_HW2_Tag_Resolve(t *testing.T) {
	mhs := []string{
		"x1", "x2", "x3", "x4", "x5",
	}

	names := []string{
		"a", "b", "c", "d", "e",
	}

	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	for i, mh := range mhs {
		node1.Tag(names[i], mh)
	}

	for i, name := range names {
		mh := node1.Resolve(name)
		require.Equal(t, mhs[i], mh)
	}

	storage := node1.GetStorage().GetNamingStore()
	storage.ForEach(func(key string, val []byte) bool {
		require.Contains(t, mhs, string(val))
		require.Contains(t, names, key)
		return true
	})

	require.Equal(t, len(names), storage.Len())
}

// 2-14
//
// Searching on a peer that doesn't have anything should return an empty result
func Test_HW2_SearchFirst_Empty(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	result, err := node1.SearchAll(*regexp.MustCompile(".*"), 10, time.Millisecond*100)
	require.NoError(t, err)

	// > should not return anything. No neighbors and the name storage is empty

	require.Len(t, result, 0)

	// > catalog should be empty

	require.Len(t, node1.GetCatalog(), 0)
}

// 2-15
//
// Checks SearchAll on a single peer
func Test_HW2_SearchAll_Local(t *testing.T) {
	mhs := []string{
		"x1", "x2", "x3", "x4", "x5",
	}

	names := []string{
		"a", "b", "c", "d", "e",
	}

	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	// setting entries in the name storage
	nameStore := node1.GetStorage().GetNamingStore()

	for i, n := range names {
		nameStore.Set(n, []byte(mhs[i]))
	}

	// > expecting the search to return all names

	result, err := node1.SearchAll(*regexp.MustCompile(".*"), 10, time.Millisecond*100)
	require.NoError(t, err)

	require.Len(t, result, len(mhs))
	for _, r := range result {
		require.Contains(t, names, r)
	}

	// > catalog should be empty

	require.Len(t, node1.GetCatalog(), 0)
}

// 2-16
//
// Given the following topology
//   A <-> B
// B has some entries in the name storage, but not the corresponding metahash in
// the blob store. So it should not return anything.
func Test_HW2_SearchAll_Remote_Empty(t *testing.T) {
	mhs := []string{
		"x1", "x2", "x3", "x4", "x5",
	}

	names := []string{
		"a", "b", "c", "d", "e",
	}

	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// setting entries in the name storage of node 2
	nameStore := node2.GetStorage().GetNamingStore()

	for i, n := range names {
		nameStore.Set(n, []byte(mhs[i]))
	}

	time.Sleep(time.Millisecond * 50)

	result, err := node1.SearchAll(*regexp.MustCompile(".*"), 10, time.Millisecond*1000)
	require.NoError(t, err)

	// > result should be empty

	require.Len(t, result, 0)

	// > node1 should have sent 1 message

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 1)

	msg := z.GetSearchRequest(t, n1outs[0].Msg)
	require.Equal(t, node1.GetAddr(), n1outs[0].Header.Source)
	require.Equal(t, node1.GetAddr(), n1outs[0].Header.RelayedBy)
	require.Equal(t, node2.GetAddr(), n1outs[0].Header.Destination)
	require.Equal(t, uint(10), msg.Budget)
	require.Equal(t, node1.GetAddr(), msg.Origin)
	require.Equal(t, ".*", msg.Pattern)
	requestID := msg.RequestID

	// > node2 should have sent 1 message

	n2outs := node2.GetOuts()
	require.Len(t, n2outs, 1)

	msg2 := z.GetSearchReply(t, n2outs[0].Msg)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), n2outs[0].Header.Destination)
	require.Equal(t, requestID, msg2.RequestID)
	require.Len(t, msg2.Responses, 0)

	// > catalog should be empty

	require.Len(t, node1.GetCatalog(), 0)
}

// 2-17
//
// Given the following topology
//   A <-> B
// A should be able to index files that match from B.
func Test_HW2_SearchAll_Remote_Response(t *testing.T) {
	mhs := []string{
		"mh1", "mh2", "mh3", "mh4", "mh5",
	}

	names := []string{
		"nameA", "nameB", "nameC", "nameD", "nameE",
	}

	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// setting entries in the name storage of node 2
	nameStore := node2.GetStorage().GetNamingStore()

	for i, n := range names {
		nameStore.Set(n, []byte(mhs[i]))
	}

	// setting entries in the blob storage of node 2
	blobStore := node2.GetStorage().GetDataBlobStore()

	for _, mh := range mhs {
		// there are two chunks, c1 and c2, but only c1 is available
		blobStore.Set(mh, []byte("chunk-1\nchunk-2"))
		blobStore.Set("chunk-1", []byte("data"))
	}

	time.Sleep(time.Millisecond * 50)

	result, err := node1.SearchAll(*regexp.MustCompile("name[A-D]"), 10, time.Millisecond*1000)
	require.NoError(t, err)

	require.Len(t, result, 4)
	for _, r := range result {
		require.Contains(t, names, r)
	}

	// > node2 should have sent 1 message with the partial files

	n2outs := node2.GetOuts()
	require.Len(t, n2outs, 1)

	msg2 := z.GetSearchReply(t, n2outs[0].Msg)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), n2outs[0].Header.Destination)
	require.Len(t, msg2.Responses, 4)

	for _, e := range msg2.Responses {
		require.Len(t, e.Chunks, 2)
		require.Equal(t, []byte("chunk-1"), e.Chunks[0])
		require.Empty(t, e.Chunks[1])
	}

	// > catalog should be updated as follow:

	// "chunk-1": {node2:{}}, "mh1": {"node2:{}"}, "mh2": {"node2:{}"},
	// "mh3": {"node2:{}"}, "mh4": {"node2:{}"}

	catalog := node1.GetCatalog()

	require.Len(t, catalog, 5)
	require.Contains(t, catalog, "chunk-1")
	require.Contains(t, catalog, "mh1")
	require.Contains(t, catalog, "mh2")
	require.Contains(t, catalog, "mh3")
	require.Contains(t, catalog, "mh4")

	for _, v := range catalog {
		require.Len(t, v, 1)
		require.Contains(t, v, node2.GetAddr())
	}

	// > name store should be updated as follow:

	// nameA: mh1, nameB: mh2, nameC: mh3, nameD: mh4

	nameStore = node1.GetStorage().GetNamingStore()
	require.Equal(t, 4, nameStore.Len())

	require.Equal(t, []byte("mh1"), nameStore.Get("nameA"))
	require.Equal(t, []byte("mh2"), nameStore.Get("nameB"))
	require.Equal(t, []byte("mh3"), nameStore.Get("nameC"))
	require.Equal(t, []byte("mh4"), nameStore.Get("nameD"))
}

// 2-18
//
// Given the following topology
//   A <-> B <-> C <-> D
// If A performs a search with budget 2, A should index all files that match the
// regex on B, C, but not D
func Test_HW2_SearchAll_Remote_Relay(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node4.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())
	node3.AddPeer(node4.GetAddr())
	node4.AddPeer(node3.GetAddr())

	// setting entries in the name storage of node 1
	nameStore := node1.GetStorage().GetNamingStore()
	nameStore.Set("filenameA", []byte("mhA"))

	// setting entry on node 2
	nameStore = node2.GetStorage().GetNamingStore()
	blobStore := node2.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1B"))

	// setting entry on node 3
	nameStore = node3.GetStorage().GetNamingStore()
	blobStore = node3.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameC", []byte("mhC"))
	blobStore.Set("mhC", []byte("c1C"))
	blobStore.Set("c1C", []byte("xxx"))
	// it happens that node 3 also have filenameB
	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1B"))

	// setting entry on node 4
	nameStore = node4.GetStorage().GetNamingStore()
	blobStore = node4.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameD", []byte("mhD"))
	blobStore.Set("mhD", []byte("c1D"))

	time.Sleep(time.Millisecond * 50)

	result, err := node1.SearchAll(*regexp.MustCompile(".*"), 2, time.Millisecond*1000)
	require.NoError(t, err)

	expected := []string{"filenameA", "filenameB", "filenameC"}

	require.Len(t, result, len(expected))
	for _, r := range result {
		require.Contains(t, expected, r)
	}

	// node 4 should have received no message

	n4ins := node4.GetIns()
	require.Len(t, n4ins, 0)

	// > catalog should be updated as follow:

	// mhB: {node2:{}, node3:{}}, mhC: {node3:{}}, c1C: {node3:{}}

	catalog := node1.GetCatalog()

	require.Len(t, catalog, 3)
	require.Contains(t, catalog, "mhB")
	require.Contains(t, catalog, "mhC")
	require.Contains(t, catalog, "c1C")

	require.Len(t, catalog["mhB"], 2)
	require.Contains(t, catalog["mhB"], node2.GetAddr())
	require.Contains(t, catalog["mhB"], node3.GetAddr())

	require.Len(t, catalog["mhC"], 1)
	require.Contains(t, catalog["mhC"], node3.GetAddr())

	require.Len(t, catalog["c1C"], 1)
	require.Contains(t, catalog["c1C"], node3.GetAddr())

	// > name store should be updated as follow:

	// filenameA: mhA, filenameB: mhB, filenameC: mhC

	nameStore = node1.GetStorage().GetNamingStore()
	require.Equal(t, 3, nameStore.Len())

	require.Equal(t, []byte("mhA"), nameStore.Get("filenameA"))
	require.Equal(t, []byte("mhB"), nameStore.Get("filenameB"))
	require.Equal(t, []byte("mhC"), nameStore.Get("filenameC"))
}

// 2-19
//
// Given the following topology
//   A <-> B
//     <-> C <-> D
//           <-> E
// If A performs a search with budget 4, A should index all files that match the
// regex on B, C, and one of D or E. It should also update the name store.
func Test_HW2_SearchAll_Remote_Budget(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node4 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node4.Stop()

	node5 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node5.Stop()

	// A <-> B
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	// A <-> C
	node1.AddPeer(node3.GetAddr())
	node3.AddPeer(node1.GetAddr())
	// C <-> D
	node3.AddPeer(node4.GetAddr())
	node4.AddPeer(node3.GetAddr())
	// C <-> E
	node3.AddPeer(node5.GetAddr())
	node5.AddPeer(node3.GetAddr())

	// setting entries in the name storage of node 1
	nameStore := node1.GetStorage().GetNamingStore()
	nameStore.Set("filenameA", []byte("mhA"))

	// setting entry on node 2
	nameStore = node2.GetStorage().GetNamingStore()
	blobStore := node2.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1B"))

	// setting entry on node 3
	nameStore = node3.GetStorage().GetNamingStore()
	blobStore = node3.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameC", []byte("mhC"))
	blobStore.Set("mhC", []byte("c1C"))

	// setting entry on node 4
	nameStore = node4.GetStorage().GetNamingStore()
	blobStore = node4.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameD", []byte("mhD"))
	blobStore.Set("mhD", []byte("c1D"))

	// setting entry on node 5
	nameStore = node5.GetStorage().GetNamingStore()
	blobStore = node5.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameE", []byte("mhE"))
	blobStore.Set("mhE", []byte("c1E"))

	time.Sleep(time.Millisecond * 50)

	result, err := node1.SearchAll(*regexp.MustCompile(".*"), 4, time.Millisecond*1000)
	require.NoError(t, err)

	require.Len(t, result, 4)

	require.Contains(t, result, "filenameA")
	require.Contains(t, result, "filenameB")
	require.Contains(t, result, "filenameC")

	// one of node 4 or 5 should have received no message

	n4ins := node4.GetIns()
	n5ins := node5.GetIns()
	if len(n4ins) == 0 {
		require.Len(t, n5ins, 1)
		require.Contains(t, result, "filenameE")
	} else {
		require.Len(t, n4ins, 1)
		require.Contains(t, result, "filenameD")
	}

	// > catalog should be updated as follow:

	// mhB: {node2:{}, mhC: {node3:{}}, -- mhD: {node4:{}} OR mhE: {node5:{}} --

	catalog := node1.GetCatalog()

	require.Len(t, catalog, 3)
	require.Contains(t, catalog, "mhB")
	require.Contains(t, catalog, "mhC")

	if len(n4ins) == 0 {
		require.Contains(t, catalog, "mhE")
		require.Len(t, catalog["mhE"], 1)
		require.Contains(t, catalog["mhE"], node5.GetAddr())
	} else {
		require.Contains(t, catalog, "mhD")
		require.Len(t, catalog["mhD"], 1)
		require.Contains(t, catalog["mhD"], node4.GetAddr())
	}

	require.Len(t, catalog["mhB"], 1)
	require.Contains(t, catalog["mhB"], node2.GetAddr())

	require.Len(t, catalog["mhC"], 1)
	require.Contains(t, catalog["mhC"], node3.GetAddr())

	// > name store should be updated as follow:

	// filenameA: mhA, filenameB: mhB, filenameC: mhC, -- filenameD: mhD OR
	// filenameE: mhE --

	nameStore = node1.GetStorage().GetNamingStore()
	require.Equal(t, 4, nameStore.Len())

	require.Equal(t, []byte("mhA"), nameStore.Get("filenameA"))
	require.Equal(t, []byte("mhB"), nameStore.Get("filenameB"))
	require.Equal(t, []byte("mhC"), nameStore.Get("filenameC"))
	if len(n4ins) == 0 {
		require.Equal(t, []byte("mhE"), nameStore.Get("filenameE"))
	} else {
		require.Equal(t, []byte("mhD"), nameStore.Get("filenameD"))
	}
}

// 2-20
//
// Test a SearchFirst with no neighbor.
func Test_HW2_SearchFirst_No_Neighbor(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithAutostart(false))
	defer node1.Stop()

	expandingConf := peer.ExpandingRing{
		Initial: 1,
		Factor:  2,
		Retry:   2,
		Timeout: time.Millisecond * 100,
	}

	res, err := node1.SearchFirst(*regexp.MustCompile(".*"), expandingConf)
	require.NoError(t, err)

	require.Empty(t, res)
}

// 2-21
//
// With no result the search function should use the expanding-ring scheme.
func Test_HW2_SearchFirst_Expanding(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	expandingConf := peer.ExpandingRing{
		Initial: 1,
		Factor:  3,
		Retry:   2,
		Timeout: time.Millisecond * 100,
	}

	res, err := node1.SearchFirst(*regexp.MustCompile(".*"), expandingConf)
	require.NoError(t, err)

	require.Empty(t, res)

	// > node 1 should have sent 2 search requests at 100ms interval

	n1outs := node1.GetOuts()

	require.Len(t, n1outs, 2)

	msg := z.GetSearchRequest(t, n1outs[0].Msg)

	require.Equal(t, node1.GetAddr(), n1outs[0].Header.Source)
	require.Equal(t, node1.GetAddr(), n1outs[0].Header.RelayedBy)
	require.Equal(t, node2.GetAddr(), n1outs[0].Header.Destination)
	require.Equal(t, node1.GetAddr(), msg.Origin)
	require.Equal(t, ".*", msg.Pattern)
	require.Equal(t, uint(1), msg.Budget)

	msg = z.GetSearchRequest(t, n1outs[1].Msg)

	require.Equal(t, node1.GetAddr(), n1outs[1].Header.Source)
	require.Equal(t, node1.GetAddr(), n1outs[1].Header.RelayedBy)
	require.Equal(t, node2.GetAddr(), n1outs[1].Header.Destination)
	require.Equal(t, node1.GetAddr(), msg.Origin)
	require.Equal(t, ".*", msg.Pattern)
	require.Equal(t, uint(3), msg.Budget)

	// > node 2 should have sent two requests

	n2outs := node2.GetOuts()

	require.Len(t, n1outs, 2)

	msg2 := z.GetSearchReply(t, n2outs[0].Msg)

	require.Equal(t, node2.GetAddr(), n2outs[0].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[0].Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), n2outs[0].Header.Destination)
	require.Len(t, msg2.Responses, 0)

	msg2 = z.GetSearchReply(t, n2outs[1].Msg)

	require.Equal(t, node2.GetAddr(), n2outs[1].Header.Source)
	require.Equal(t, node2.GetAddr(), n2outs[1].Header.RelayedBy)
	require.Equal(t, node1.GetAddr(), n2outs[1].Header.Destination)
	require.Len(t, msg2.Responses, 0)

	// > catalog should be empty

	require.Empty(t, node1.GetCatalog())

	// > name storage should be empty

	require.Equal(t, 0, node1.GetStorage().GetNamingStore().Len())
}

// 2-22
//
// Do a SearchFirst where the local peer has a total match. In that case it
// shouldn't send any search request.
func Test_HW2_SearchFirst_Local(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// setting a file in node1
	nameStore := node1.GetStorage().GetNamingStore()
	blobStore := node1.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameA", []byte("mhA"))
	blobStore.Set("mhA", []byte("c1"))
	blobStore.Set("c1", []byte("data"))

	expandingConf := peer.ExpandingRing{
		Initial: 1,
		Factor:  3,
		Retry:   2,
		Timeout: time.Millisecond * 100,
	}

	res, err := node1.SearchFirst(*regexp.MustCompile(".*"), expandingConf)
	require.NoError(t, err)

	require.Equal(t, "filenameA", res)

	// > node 1 should have not sent any request since it has a total match
	// locally.

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 0)

	// > catalog should be empty

	require.Empty(t, node1.GetCatalog())

	// > name storage should be untouched
	require.Equal(t, 1, nameStore.Len())
	require.Equal(t, []byte("mhA"), nameStore.Get("filenameA"))
}

// 2-23
//
// If the node doesn't have all chunks then it shouldn't return a result.
func Test_HW2_SearchFirst_Local_Partial(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// setting a partial file in node1
	nameStore := node1.GetStorage().GetNamingStore()
	blobStore := node1.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameA", []byte("mhA"))
	blobStore.Set("mhA", []byte("c1a"))

	// setting a partial file in node2
	nameStore = node2.GetStorage().GetNamingStore()
	blobStore = node2.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1b"))

	expandingConf := peer.ExpandingRing{
		Initial: 1,
		Factor:  3,
		Retry:   2,
		Timeout: time.Millisecond * 100,
	}

	res, err := node1.SearchFirst(*regexp.MustCompile(".*"), expandingConf)
	require.NoError(t, err)
	require.Empty(t, res)

	// > node 1 should have sent two requests

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 2)

	// > catalog should be updated

	catalog := node1.GetCatalog()
	require.Len(t, catalog, 1)
	require.Len(t, catalog["mhB"], 1)
	require.Contains(t, catalog["mhB"], node2.GetAddr())

	// > name storage should be updated

	nameStore = node1.GetStorage().GetNamingStore()
	require.Equal(t, 2, nameStore.Len())
	require.Equal(t, []byte("mhA"), nameStore.Get("filenameA"))
	require.Equal(t, []byte("mhB"), nameStore.Get("filenameB"))
}

// 2-24
//
// Get a total file in B from A:
//   A <-> B
func Test_HW2_SearchFirst_Remote(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	// setting a partial file in node1
	nameStore := node1.GetStorage().GetNamingStore()
	blobStore := node1.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameA", []byte("mhA"))
	blobStore.Set("mhA", []byte("c1a"))

	// setting a total file in node2
	nameStore = node2.GetStorage().GetNamingStore()
	blobStore = node2.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1b"))
	blobStore.Set("c1b", []byte("data"))

	expandingConf := peer.ExpandingRing{
		Initial: 1,
		Factor:  3,
		Retry:   2,
		Timeout: time.Millisecond * 100,
	}

	res, err := node1.SearchFirst(*regexp.MustCompile(".*"), expandingConf)
	require.NoError(t, err)
	require.Equal(t, "filenameB", res)

	// > node 1 should have sent one request

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 1)

	// > catalog should reference the data blobs

	catalog := node1.GetCatalog()

	require.Len(t, catalog, 2)

	require.Len(t, catalog["mhB"], 1)
	require.Contains(t, catalog["mhB"], node2.GetAddr())

	require.Len(t, catalog["c1b"], 1)
	require.Contains(t, catalog["c1b"], node2.GetAddr())

	// > name storage should contain a new entry

	nameStore = node1.GetStorage().GetNamingStore()
	require.Equal(t, 2, nameStore.Len())
	require.Equal(t, nameStore.Get("filenameB"), []byte("mhB"))
}

// 2-25
//
// Given the following topology
//   A <-> B <-> C
// A should be able to get a total file in C, using the expanding scheme and an
// initial budget of 1.
func Test_HW2_SearchFirst_Remote_Expanding(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()

	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())
	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	// Setting the following data on the nodes ("{-}"" = missing chunk):
	//
	// node 1: filenameA - mhA {-}
	// node 2: filenameB - mhB {-}{c2b}
	//         filenameC - mhC {c1c}{-}
	// node 3: filenameB - mhB {c1b}{-}
	//         filenameC - mhC {c1c}{c2c}

	// setting a partial file in node1
	nameStore := node1.GetStorage().GetNamingStore()
	blobStore := node1.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameA", []byte("mhA"))
	blobStore.Set("mhA", []byte("c1a"))

	// setting two partial files in node2
	nameStore = node2.GetStorage().GetNamingStore()
	blobStore = node2.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1b"+peer.MetafileSep+"c2b"))
	blobStore.Set("c2b", []byte("data"))

	nameStore.Set("filenameC", []byte("mhC"))
	blobStore.Set("mhC", []byte("c1c"+peer.MetafileSep+"c2c"))
	blobStore.Set("c1c", []byte("data"))

	// setting a total file and a partial file in node3
	nameStore = node3.GetStorage().GetNamingStore()
	blobStore = node3.GetStorage().GetDataBlobStore()

	nameStore.Set("filenameC", []byte("mhC"))
	blobStore.Set("mhC", []byte("c1c"+peer.MetafileSep+"c2c"))
	blobStore.Set("c1c", []byte("data"))
	blobStore.Set("c2c", []byte("data"))

	nameStore.Set("filenameB", []byte("mhB"))
	blobStore.Set("mhB", []byte("c1b"+peer.MetafileSep+"c2b"))
	blobStore.Set("c1b", []byte("data"))

	expandingConf := peer.ExpandingRing{
		Initial: 1,
		Factor:  2,
		Retry:   2,
		Timeout: time.Millisecond * 200,
	}

	res, err := node1.SearchFirst(*regexp.MustCompile(".*"), expandingConf)
	require.NoError(t, err)
	require.Equal(t, "filenameC", res)

	// We are expecting the following flow:
	//
	// A -> B      : query request
	//   A <- B    : query reply
	// A -> B      : query request
	//   A <- B    : query reply
	//     B -> C  : (forward) query request
	//   B <- C    : query reply
	// A <- B      : (relay) query reply

	// > node 1 should have sent 2 requests

	n1outs := node1.GetOuts()
	require.Len(t, n1outs, 2)

	// > node 2 should have sent 4 requests

	n2outs := node2.GetOuts()
	require.Len(t, n2outs, 4)

	// > node 3 should have sent 1 request

	n3outs := node3.GetOuts()
	require.Len(t, n3outs, 1)

	// > catalog should reference the data blobs

	catalog := node1.GetCatalog()

	require.Len(t, catalog, 6)

	require.Len(t, catalog["mhB"], 2)
	require.Contains(t, catalog["mhB"], node2.GetAddr())
	require.Contains(t, catalog["mhB"], node3.GetAddr())

	require.Len(t, catalog["c1b"], 1)
	require.Contains(t, catalog["c1b"], node3.GetAddr())

	require.Len(t, catalog["c2b"], 1)
	require.Contains(t, catalog["c2b"], node2.GetAddr())

	require.Len(t, catalog["mhC"], 2)
	require.Contains(t, catalog["mhC"], node2.GetAddr())
	require.Contains(t, catalog["mhC"], node3.GetAddr())

	require.Len(t, catalog["c1c"], 2)
	require.Contains(t, catalog["c1c"], node2.GetAddr())
	require.Contains(t, catalog["c1c"], node3.GetAddr())

	require.Len(t, catalog["c2c"], 1)
	require.Contains(t, catalog["c2c"], node3.GetAddr())

	// > name storage should contain a new entry

	nameStore = node1.GetStorage().GetNamingStore()
	require.Equal(t, 3, nameStore.Len())
	require.Equal(t, nameStore.Get("filenameA"), []byte("mhA"))
	require.Equal(t, nameStore.Get("filenameB"), []byte("mhB"))
	require.Equal(t, nameStore.Get("filenameC"), []byte("mhC"))
}

// 2-26
//
// Scenario test, with the following topology:
// ┌───────────┐
// ▼           │
// C ────► D   │
// │       │   │
// ▼       ▼   │
// A ◄───► B ──┘
func Test_HW2_Scenario(t *testing.T) {

	getTest := func(transp transport.Transport) func(*testing.T) {
		return func(t *testing.T) {
			chunkSize := uint(2)

			opts := []z.Option{
				z.WithChunkSize(chunkSize),
				// at least every peer will send a heartbeat message on start,
				// which will make everyone to have an entry in its routing
				// table to every one else, thanks to the antientropy.
				z.WithHeartbeat(time.Second * 200),
				z.WithAntiEntropy(time.Second * 5),
				z.WithAckTimeout(time.Second * 10),
			}

			nodeA := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
			defer nodeA.Stop()

			nodeB := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
			defer nodeB.Stop()

			nodeC := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
			defer nodeC.Stop()

			nodeD := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
			defer nodeD.Stop()

			nodeA.AddPeer(nodeB.GetAddr())
			nodeB.AddPeer(nodeA.GetAddr())
			nodeB.AddPeer(nodeC.GetAddr())
			nodeC.AddPeer(nodeA.GetAddr())
			nodeC.AddPeer(nodeD.GetAddr())
			nodeD.AddPeer(nodeB.GetAddr())

			// Wait for the anti-entropy to take effect, i.e. everyone gets the
			// heartbeat message from everyone else.
			time.Sleep(time.Second * 10)

			// > If I upload a file on NodeB I should be able to download it
			// from NodeB

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

			// > NodeA should be able to search for "fileD" using the expanding
			// scheme
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

			// Let's add new nodes and see if they can index the files and
			// download them

			// 	           ┌───────────┐
			//             ▼           │
			// F ──► E ──► C ────► D   │
			//             │       │   │
			// 	           ▼       ▼   │
			// 	           A ◄───► B ◄─┘

			nodeE := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
			defer nodeE.Stop()

			nodeF := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", opts...)
			defer nodeF.Stop()

			nodeE.AddPeer(nodeC.GetAddr())
			nodeF.AddPeer(nodeE.GetAddr())

			// wait for the anti-entropy to take effect, i.e. everyone get the
			// heartbeat messages sent by nodeE and nodeF.
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
	}

	t.Run("channel transport", getTest(channelFac()))
	t.Run("UDP transport", getTest(udpFac()))
}
