package impl

import (
	"crypto"
	"encoding/hex"
	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"io"
	"regexp"
	"strings"
	"time"
)

// ------------------ HW 2 --------------------
// functions responsible for sharing files between peers

/*
	1. Separate the uploades file into chunks sized by n.configuration.ChunkSize
	2. For each chunbk calculate it's hash and encode it in hex
	3. Calculate metafile by concatenating all the chunks hashes and adding a separator
	4. Calculate metakey by encoding the concatenation of all the hashes

*/
func (n *node) Upload(data io.Reader) (metahash string, err error) {
	var chunks [][]byte
	var hashes [][]byte
	var chunksHexNashes []string

	for {
		buffer := make([]byte, n.configuration.ChunkSize)
		bytesRead, err := data.Read(buffer)
		if err != nil && err != io.EOF { // when the data reaches the end, it returns an EOF error
			return "", err
		}

		if bytesRead == 0 {
			break
		}

		chunk := buffer[0:bytesRead]
		h := crypto.SHA256.New()
		h.Write(chunk)
		chunkHash := h.Sum(nil)
		chunkHashHex := hex.EncodeToString(chunkHash)

		chunks = append(chunks, chunk)
		hashes = append(hashes, chunkHash)
		chunksHexNashes = append(chunksHexNashes, chunkHashHex)
	}

	var metaFile string
	var hashesConcatenaded []byte
	blobStore := n.configuration.Storage.GetDataBlobStore()

	for i, chunk := range chunks {
		blobStore.Set(chunksHexNashes[i], chunk)

		metaFile += chunksHexNashes[i]
		if i != len(chunks)-1 { // do not add separator for the last chunk
			metaFile += peer.MetafileSep
		}

		hashesConcatenaded = append(hashesConcatenaded, hashes[i]...)
	}

	h := crypto.SHA256.New()
	h.Write(hashesConcatenaded)
	metaHashPreHex := h.Sum(nil)
	metaHash := hex.EncodeToString(metaHashPreHex)
	blobStore.Set(metaHash, []byte(metaFile))

	return metaHash, nil
}

func (n *node) GetCatalog() peer.Catalog {
	return n.catalog.getCatalog()
}

func (n *node) UpdateCatalog(key string, peer string) {
	n.catalog.add(key, peer)
}

func (n *node) Download(metahash string) ([]byte, error) {
	blobStore := n.configuration.Storage.GetDataBlobStore()
	metafile := blobStore.Get(metahash)

	if metafile == nil {
		remotePeers := n.catalog.get(metahash)
		if remotePeers == nil {
			return nil, xerrors.New("Metahash not found locally or remotely")
		}

		err := n.requestRemoteData(metahash, remotePeers[0])
		if err != nil {
			return nil, err
		}

		metafile = blobStore.Get(metahash)
		if metafile == nil {
			return nil, xerrors.Errorf("Couldn't get metafile associated with metahash %v", metahash)
		}
	}

	chunksHashes := strings.Split(string(metafile), peer.MetafileSep)

	var file []byte
	for _, chunkHash := range chunksHashes {
		chunk := blobStore.Get(chunkHash)
		if chunk == nil {
			remotePeers := n.catalog.get(chunkHash)
			if remotePeers == nil {
				return nil, xerrors.Errorf("No remote peer for chunk with hash %v", chunkHash)
			}
			err := n.requestRemoteData(chunkHash, remotePeers[0])
			if err != nil {
				return nil, err
			}
			chunk = blobStore.Get(chunkHash)
			if chunk == nil {
				return nil, xerrors.Errorf("Couldn't get chunk associated with hash %v", chunkHash)
			}
		}
		file = append(file, chunk...)
	}
	return file, nil
}

func (n *node) Resolve(name string) string {
	namingStore := n.configuration.Storage.GetNamingStore()
	return string(namingStore.Get(name))
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	requestID := xid.New().String()
	var filenames []string

	if len(n.routingTable.neighbourList) != 0 {
		n.searchStorer.createEntry(requestID)
		err := n.sendBudgetedRequests(requestID, budget, reg)
		if err != nil {
			return filenames, err
		}
	}

	time.Sleep(timeout)
	filenamesSet := make(map[string]bool)

	internalFilenames, _ := n.searchNamingStore(reg, false)
	externalFilenames := n.searchStorer.get(requestID)

	for _, filename := range internalFilenames {
		filenamesSet[filename] = true
	}

	for _, filename := range externalFilenames {
		filenamesSet[filename] = true
	}

	for filename := range filenamesSet {
		filenames = append(filenames, filename)
	}

	return filenames, nil
}

// SearchFirst uses an expanding ring configuration and returns a name as
// soon as it finds a peer that "fully matches" a data blob. It makes the
// peer update its catalog and name storage according to the
// SearchReplyMessages received. Returns an empty string if nothing was
// found.
func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (string, error) {
	internalFile := n.searchNamingStoreFullFile(pattern)
	if internalFile != "" {
		return internalFile, nil
	}

	requestID := xid.New().String()
	filenameChannel := n.searchMap.createEntry(requestID)

	defer n.searchMap.closeChannel(requestID)
	retransmissions := uint(0)
	budget := conf.Initial
	err := n.sendBudgetedRequests(requestID, budget, pattern)
	if err != nil {
		return "", err
	}

	ticker := time.NewTicker(conf.Timeout)
	for {
		select {
		case filename, open := <-*filenameChannel: // triggered when a file is written, or it is closed by the peer
			if open {
				return filename, nil
			} else { // channel was closed
				return "", nil
			}
		case <-ticker.C:
			retransmissions++
			if retransmissions >= conf.Retry {
				return "", nil
			}
			budget = budget * conf.Factor

			err = n.sendBudgetedRequests(requestID, budget, pattern)
			if err != nil {
				return "", err
			}

			ticker = time.NewTicker(conf.Timeout)
		}
	}
}

func (n *node) sendBudgetedRequests(requestID string, budget uint, reg regexp.Regexp) error {
	neighbourList := n.routingTable.neighbourList
	if len(neighbourList) == 0 {
		return nil
	}

	if int(budget) <= len(neighbourList) {
		for _, neighbourAddress := range neighbourList {
			pkt, err := n.createSearchRequestPacket(requestID, 1, reg, neighbourAddress, n.socket.GetAddress())
			if err != nil {
				return err
			}

			err = n.socket.Send(neighbourAddress, pkt, 0)
			if err != nil {
				return err
			}

			budget--
			if budget <= 0 {
				break
			}
		}
	} else {
		budgets := make([]uint, len(neighbourList))
		i := 0
		for budget > 0 {
			i = i % len(neighbourList)
			budgets[i]++
			budget--
			i++
		}

		for i, neighbourAddress := range neighbourList {
			pkt, err := n.createSearchRequestPacket(requestID, budgets[i], reg, neighbourAddress, n.socket.GetAddress())
			if err != nil {
				return err
			}

			err = n.socket.Send(neighbourAddress, pkt, 0)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// sends a data request and blocks until the respective channel is triggered
func (n *node) requestRemoteData(hexHash string, peerAddress string) error {
	requestID := xid.New().String()

	dataRequest := types.DataRequestMessage{
		RequestID: requestID,
		Key:       hexHash,
	}

	dataRequestMarshaled, err := n.registry.MarshalMessage(dataRequest)
	if err != nil {
		return err
	}

	dataRequestHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), peerAddress, 0)
	dataRequestPacket := transport.Packet{
		Header: &dataRequestHeader,
		Msg:    &dataRequestMarshaled,
	}
	addressToSend, exists := n.routingTable.get(peerAddress)
	if !exists {
		return xerrors.Errorf("No routing table entry for %v", peerAddress)
	}

	requestChannelPtr := n.requestMap.createEntry(requestID)
	err = n.socket.Send(addressToSend, dataRequestPacket, 0)
	if err != nil {
		return err
	}

	retransmissions := uint(0)
	timeInterval := n.configuration.BackoffDataRequest.Initial
	ticker := time.NewTicker(timeInterval)
	for {
		select {
		case <-*requestChannelPtr: // triggered when channel is closed, indicating the data was received or the node must shut down
			return nil
		case <-ticker.C:
			retransmissions++
			if retransmissions > n.configuration.BackoffDataRequest.Retry {
				return xerrors.Errorf("Number of retries exceeded when requesting ID %v", requestID)
			}

			err = n.socket.Send(peerAddress, dataRequestPacket, 0)
			if err != nil {
				return err
			}

			timeInterval = timeInterval * time.Duration(n.configuration.BackoffDataRequest.Factor)
			ticker = time.NewTicker(timeInterval)
		}
	}
}

func (n *node) createSearchRequestPacket(requestID string, budget uint, reg regexp.Regexp, peerAddress string, origin string) (transport.Packet, error) {
	searchRequest := types.SearchRequestMessage{
		RequestID: requestID,
		Origin:    origin,
		Pattern:   reg.String(),
		Budget:    budget,
	}

	searchRequestMarshaled, err := n.registry.MarshalMessage(searchRequest)
	if err != nil {
		return transport.Packet{}, err
	}

	searchRequestHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), peerAddress, 0)
	return transport.Packet{
		Header: &searchRequestHeader,
		Msg:    &searchRequestMarshaled,
	}, nil
}

// returns filenames and respective hashes that match the expession
// checkIfFileIsStored is used to diferentiate between internal searches and serches when answering someone
func (n *node) searchNamingStore(reg regexp.Regexp, checkIfMetafileIsStored bool) ([]string, [][]byte) {
	var filenames []string
	var hashes [][]byte

	namingStore := n.configuration.Storage.GetNamingStore()
	namingStore.ForEach(func(key string, val []byte) bool {
		if reg.MatchString(key) {
			blobStore := n.configuration.Storage.GetDataBlobStore()
			if checkIfMetafileIsStored {
				if blobStore.Get(string(val)) != nil {
					filenames = append(filenames, key)
					hashes = append(hashes, val)
				}
			} else {
				filenames = append(filenames, key)
				hashes = append(hashes, val)
			}
		}

		return true
	})

	return filenames, hashes
}

// used internally on searchfirst to check if there's any internal file that
// matches the regex and all chunks are available
func (n *node) searchNamingStoreFullFile(reg regexp.Regexp) string {
	file := ""
	namingStore := n.configuration.Storage.GetNamingStore()
	namingStore.ForEach(func(key string, val []byte) bool {
		if reg.MatchString(key) {

			blobStore := n.configuration.Storage.GetDataBlobStore()
			metafile := blobStore.Get(string(val))
			if metafile != nil {
				chunksHashes := strings.Split(string(metafile), peer.MetafileSep)
				fileExists := true
				for _, chunkHask := range chunksHashes {
					if blobStore.Get(chunkHask) == nil {
						fileExists = false
						break
					}
				}
				if fileExists {
					file = key
					return false
				}
			}
		}

		return true
	})

	return file
}
