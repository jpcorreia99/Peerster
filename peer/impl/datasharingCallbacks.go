package impl

import (
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"regexp"
	"strings"
)

func (n *node) DataRequestMessageCallback(msg types.Message, pkt transport.Packet) error {
	dataRequestMsg, castWasSuccessful := msg.(*types.DataRequestMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Data Request Message failed")
	}

	hexHash := dataRequestMsg.Key
	blobStore := n.configuration.Storage.GetDataBlobStore()
	requestedData := blobStore.Get(hexHash)

	dataReplyMessage := types.DataReplyMessage{
		RequestID: dataRequestMsg.RequestID,
		Key:       dataRequestMsg.Key,
		Value:     requestedData,
	}

	dataReplyMessageMarshaled, err := n.registry.MarshalMessage(dataReplyMessage)
	if err != nil {
		return err
	}

	return n.Unicast(pkt.Header.Source, dataReplyMessageMarshaled)
}

func (n *node) DataReplyMessageCallback(msg types.Message, pkt transport.Packet) error {
	dataReplyMsg, castWasSuccessful := msg.(*types.DataReplyMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Data Request Message failed")
	}

	if dataReplyMsg.Value != nil {
		blobStore := n.configuration.Storage.GetDataBlobStore()
		blobStore.Set(dataReplyMsg.Key, dataReplyMsg.Value)
	}

	n.requestMap.closeChannel(dataReplyMsg.RequestID)
	return nil
}

func (n *node) SearchRequestMessageCallback(msg types.Message, pkt transport.Packet) error {
	searchRequestMsg, castWasSuccessful := msg.(*types.SearchRequestMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Search Request Message failed")
	}

	if searchRequestMsg.Budget > 0 { // check is used to avoid uint overflow
		searchRequestMsg.Budget--
	}

	filenames, hashes := n.searchNamingStore(*regexp.MustCompile(searchRequestMsg.Pattern), true)

	err := n.sendSearchReply(*searchRequestMsg, filenames, hashes, pkt.Header.RelayedBy)
	if err != nil {
		return err
	}

	return n.relaySearchRequest(*searchRequestMsg, pkt)
}

func (n *node) SearchReplyMessageCallback(msg types.Message, pkt transport.Packet) error {
	searchReplyMsg, castWasSuccessful := msg.(*types.SearchReplyMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Search Reply Message failed")
	}

	if n.searchStorer.contains(searchReplyMsg.RequestID) {
		return n.findAllSearchReplyCallback(*searchReplyMsg, pkt)
	} else if n.searchMap.contains(searchReplyMsg.RequestID) {
		return n.findFirstSearchReplyCallBack(*searchReplyMsg, pkt)
	}

	return nil
}

func (n *node) findAllSearchReplyCallback(searchReplyMsg types.SearchReplyMessage, pkt transport.Packet) error {
	namingStore := n.configuration.Storage.GetNamingStore()
	for _, response := range searchReplyMsg.Responses {
		n.searchStorer.registerFile(searchReplyMsg.RequestID, response.Name)
		n.catalog.add(response.Metahash, pkt.Header.Source)
		namingStore.Set(response.Name, []byte(response.Metahash))

		for _, chunkHash := range response.Chunks {
			if chunkHash != nil {
				n.catalog.add(string(chunkHash), pkt.Header.Source)
			}
		}
	}

	return nil
}

func (n *node) findFirstSearchReplyCallBack(searchReplyMsg types.SearchReplyMessage, pkt transport.Packet) error {
	namingStore := n.configuration.Storage.GetNamingStore()
	fullFileFound := false
	fullFileFilename := ""

	for _, response := range searchReplyMsg.Responses {
		n.searchStorer.registerFile(searchReplyMsg.RequestID, response.Name)
		n.catalog.add(response.Metahash, pkt.Header.Source)
		namingStore.Set(response.Name, []byte(response.Metahash))

		allChunksPresent := true
		for _, chunkHash := range response.Chunks {
			if chunkHash != nil {
				n.catalog.add(string(chunkHash), pkt.Header.Source)
			}
			allChunksPresent = allChunksPresent && chunkHash != nil
		}

		if allChunksPresent && !fullFileFound {
			fullFileFound = true
			fullFileFilename = response.Name
		}
	}

	if fullFileFound {
		n.searchMap.signalChannel(searchReplyMsg.RequestID, fullFileFilename)
	}

	return nil
}

func (n *node) sendSearchReply(msg types.SearchRequestMessage, filenames []string, hashes [][]byte, relayerAddress string) error {
	var responses []types.FileInfo

	for i, filename := range filenames {
		chunks, err := n.getLocalChunksIDs(string(hashes[i]))
		if err != nil {
			return err
		}

		response := types.FileInfo{
			Name:     filename,
			Metahash: string(hashes[i]),
			Chunks:   chunks,
		}
		responses = append(responses, response)
	}

	searchReply := types.SearchReplyMessage{
		RequestID: msg.RequestID,
		Responses: responses,
	}

	searchReplyMarshalled, err := n.registry.MarshalMessage(searchReply)
	if err != nil {
		return err
	}

	searchReplyHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), msg.Origin, 0)

	searchReplyPacket := transport.Packet{
		Header: &searchReplyHeader,
		Msg:    &searchReplyMarshalled,
	}

	return n.socket.Send(relayerAddress, searchReplyPacket, 0)
}

// returns the local chunks hashes, returns nil in indices where the chunk is not locally present
func (n *node) getLocalChunksIDs(metahash string) ([][]byte, error) {
	blobStore := n.configuration.Storage.GetDataBlobStore()
	metafile := blobStore.Get(metahash)

	chunksHashes := strings.Split(string(metafile), peer.MetafileSep)

	chunks := make([][]byte, len(chunksHashes))
	for i, chunkHash := range chunksHashes {
		chunk := blobStore.Get(chunkHash)
		if chunk != nil {
			chunks[i] = []byte(chunkHash)
		}
	}

	return chunks, nil
}

/* First filter neighbours to not include the one that just sent the message and the original source
 */
func (n *node) relaySearchRequest(searchRequestMsg types.SearchRequestMessage, pkt transport.Packet) error {
	if searchRequestMsg.Budget == 0 {
		return nil
	}

	neighbourList := n.routingTable.getNeighboursList()
	var validNeighbourList []string
	for _, neighbourAddress := range neighbourList {
		if neighbourAddress != pkt.Header.Source && neighbourAddress != searchRequestMsg.Origin {
			validNeighbourList = append(validNeighbourList, neighbourAddress)
		}
	}

	if len(validNeighbourList) == 0 {
		return nil
	}

	remainingBudget := searchRequestMsg.Budget
	reg := *regexp.MustCompile(searchRequestMsg.Pattern)
	requestID := searchRequestMsg.RequestID

	if int(remainingBudget) <= len(neighbourList) {
		for _, neighbourAddress := range validNeighbourList {
			pkt, err := n.createSearchRequestPacket(requestID, 1, reg, neighbourAddress, searchRequestMsg.Origin)
			if err != nil {
				return err
			}

			err = n.socket.Send(neighbourAddress, pkt, 0)
			if err != nil {
				return err
			}

			remainingBudget--
			if remainingBudget <= 0 {
				break
			}
		}
	} else {
		budgets := make([]uint, len(validNeighbourList))
		i := 0
		for remainingBudget > 0 {
			i = i % len(neighbourList)
			budgets[i]++
			remainingBudget--
		}

		for i, neighbourAddress := range validNeighbourList {
			pkt, err := n.createSearchRequestPacket(requestID, budgets[i], reg, neighbourAddress, searchRequestMsg.Origin)
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
