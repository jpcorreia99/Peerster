package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"log"
	"math/rand"
)

func (n *node) ChatMessageCallback(msg types.Message, pkt transport.Packet) error {
	chatMsg, ok := msg.(*types.ChatMessage)
	if !ok {
		return xerrors.Errorf("Conversion to Chat Message failed")
	}

	log.Println("From", pkt.Header.Source, chatMsg.String())

	return nil
}

/*
	1. if the source is equal to the destination -> process locally
	2. call n.statusManager.processRumors which updtes the sequenceTable and returns if there's a new rumor
	3. Send ack back to the RumorsPacket source
	4. If any rumor was new, send the rumorsPacket to a new neighbour
*/
func (n *node) RumorsMessageCallback(msg types.Message, pkt transport.Packet) error {

	// if it's a self process
	if pkt.Header.Source == pkt.Header.Destination {
		return n.selfRumorsMessageCallback(msg, pkt)
	}

	//else: the package was received externally
	rumorsMsg, castWasSuccessful := msg.(*types.RumorsMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Rumors Message failed")
	}

	newRumor, err := n.statusManager.processRumors(rumorsMsg, &pkt, n)
	if err != nil {
		return err
	}

	err = n.sendAckBack(pkt)
	if err != nil {
		return err
	}

	// if the packet contains at least a new rumour, send it to a new neighbour
	if newRumor {
		neighbourAddress := ""
		for _, v := range n.routingTable.getNeighboursList() {
			if v != pkt.Header.Source && v != pkt.Header.RelayedBy {
				neighbourAddress = v
				break
			}
		}

		// if a neighbour was found
		if neighbourAddress != "" {
			pkt.Header.RelayedBy = n.socket.GetAddress()
			pkt.Header.Destination = neighbourAddress
			err := n.socket.Send(neighbourAddress, pkt, 0)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (n *node) EmptyMessageCallback(types.Message, transport.Packet) error { return nil }

func (n *node) AckMessageCallback(msg types.Message, pkt transport.Packet) error {

	ackMsg, castWasSuccessful := msg.(*types.AckMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Ack Message failed")
	}

	n.ackMap.closeChannel(pkt.Header.PacketID)

	return n.StatusMessageCallback(&ackMsg.Status, pkt)
}

func (n *node) StatusMessageCallback(msg types.Message, pkt transport.Packet) error {
	statusMsg, castWasSuccessful := msg.(*types.StatusMessage)

	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Rumors Message failed")
	}

	// get all the rumours that are missing on the remote peer
	missingRumoursList := n.statusManager.getMissingRumors(sequenceTable(*statusMsg))

	if len(missingRumoursList) > 0 {
		missingRumours := types.RumorsMessage{Rumors: missingRumoursList}
		missingRumoursMsg, err := n.registry.MarshalMessage(missingRumours)
		if err != nil {
			return err
		}

		missingRumoursHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), pkt.Header.Source, 0)
		missingRumoursPkt := transport.Packet{
			Header: &missingRumoursHeader,
			Msg:    &missingRumoursMsg,
		}

		err = n.socket.Send(pkt.Header.Source, missingRumoursPkt, 0)
		if err != nil {
			return err
		}
	}

	// if the remote peer has received rumours that we have not
	peerHasMissingRumours := n.statusManager.isMissingRumors(sequenceTable(*statusMsg))
	if peerHasMissingRumours {
		err := n.sendStatusMsg(pkt.Header.Source)
		if err != nil {
			return err
		}
	}

	// if both peers have the same status, send a status msg to another random peer
	if !peerHasMissingRumours && len(missingRumoursList) == 0 {
		if rand.Float64() < n.configuration.ContinueMongering {
			neighbourAddress := ""
			for _, v := range n.routingTable.getNeighboursList() {
				if v != pkt.Header.Source {
					neighbourAddress = v
					break
				}
			}

			if neighbourAddress != "" {
				err := n.sendStatusMsg(neighbourAddress)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (n *node) PrivateMessageCallback(msg types.Message, pkt transport.Packet) error {
	privateMsg, castWasSuccessful := msg.(*types.PrivateMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Rumors Message failed")
	}

	_, ownAddressIsContained := privateMsg.Recipients[n.socket.GetAddress()]

	if ownAddressIsContained {
		enclosedPkt := transport.Packet{ // enclose the rumor in a packet so that the registry can process it
			Header: pkt.Header,
			Msg:    privateMsg.Msg,
		}

		return n.registry.ProcessPacket(enclosedPkt)
	}

	return nil
}

// function to be called when a RumorsMessage is being processed locally
func (n *node) selfRumorsMessageCallback(msg types.Message, pkt transport.Packet) error {
	rumorsMsg, castWasSuccessful := msg.(*types.RumorsMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Rumors Message failed")
	}

	for _, rumor := range rumorsMsg.Rumors {
		enclosedRumor := transport.Packet{ // enclose the rumor in a packet so that the registry can process it
			Header: pkt.Header,
			Msg:    rumor.Msg,
		}

		err := n.registry.ProcessPacket(enclosedRumor)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *node) sendAckBack(pkt transport.Packet) error {
	ackHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), pkt.Header.Source, 0)

	status := types.StatusMessage(n.statusManager.getSequenceTable())

	ack := types.Message(&types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        status,
	})

	ackMsg, err := n.registry.MarshalMessage(ack)
	if err != nil {
		return err
	}

	ackPacket := transport.Packet{
		Header: &ackHeader,
		Msg:    &ackMsg,
	}

	return n.socket.Send(ackHeader.Destination, ackPacket, 0)
}

func (n *node) sendStatusMsg(destination string) error {
	status := types.StatusMessage(n.statusManager.getSequenceTable())
	responseStatusMsg, err := n.registry.MarshalMessage(status)
	if err != nil {
		return err
	}
	statusHeader := transport.NewHeader(n.socket.GetAddress(), n.socket.GetAddress(), destination, 0)
	statusPkt := transport.Packet{
		Header: &statusHeader,
		Msg:    &responseStatusMsg,
	}

	err = n.socket.Send(destination, statusPkt, 0)
	return err
}
