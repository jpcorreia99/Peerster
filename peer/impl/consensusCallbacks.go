package impl

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) PaxosPrepareMessageCallback(msg types.Message, _ transport.Packet) error {
	paxosPrepareMsg, castWasSuccessful := msg.(*types.PaxosPrepareMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Paxos Prepare Message failed")
	}

	paxosPromiseMsg, err := n.paxosController.prepareCallback(*paxosPrepareMsg)
	if err != nil || paxosPromiseMsg == nil {
		return err
	}

	paxosPromiseMsgMarshaled, err := n.registry.MarshalMessage(paxosPromiseMsg)
	if err != nil {
		return err
	}

	recipients := make(map[string]struct{})
	recipients[paxosPrepareMsg.Source] = struct{}{}
	privateMsg := types.PrivateMessage{
		Recipients: recipients,
		Msg:        &paxosPromiseMsgMarshaled,
	}

	privateMsgMarshalled, err := n.registry.MarshalMessage(privateMsg)
	if err != nil {
		return err
	}

	err = n.Broadcast(privateMsgMarshalled)
	return err
}

func (n *node) PaxosProposeMessageCallback(msg types.Message, _ transport.Packet) error {
	paxosProposeMsg, castWasSuccessful := msg.(*types.PaxosProposeMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Paxos Prepare Propose failed")
	}

	paxosAcceptMsg := n.paxosController.proposeCallback(*paxosProposeMsg)
	if paxosAcceptMsg == nil {
		return nil
	}
	paxosAcceptMsgMarshaled, err := n.registry.MarshalMessage(paxosAcceptMsg)
	if err != nil {
		return err
	}

	err = n.Broadcast(paxosAcceptMsgMarshaled)
	return err
}

func (n *node) PaxosPromiseMessageCallback(msg types.Message, _ transport.Packet) error {
	paxosPromiseMsg, castWasSuccessful := msg.(*types.PaxosPromiseMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Paxos Promise Message failed")
	}

	n.paxosController.promiseCallback(*paxosPromiseMsg)

	return nil
}

func (n *node) PaxosAcceptMessageCallback(msg types.Message, _ transport.Packet) error {
	paxosAcceptMsg, castWasSuccessful := msg.(*types.PaxosAcceptMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to Paxos Accept Message failed")
	}

	tlcMsg, err := n.paxosController.acceptCallback(*paxosAcceptMsg, n)
	if err != nil || tlcMsg == nil {
		return err
	}

	tlcMarshaled, err := n.registry.MarshalMessage(tlcMsg)
	if err != nil {
		return err
	}
	err = n.Broadcast(tlcMarshaled)
	return err
}

func (n *node) TlCMessageCallback(msg types.Message, _ transport.Packet) error {
	tlcMsg, castWasSuccessful := msg.(*types.TLCMessage)
	if !castWasSuccessful {
		return xerrors.Errorf("Conversion to TLC Message failed")
	}

	rebroadcast, err := n.paxosController.tLCCallback(*tlcMsg, n)
	if err != nil {
		return err
	}

	if rebroadcast {
		tlcMarshaled, err := n.registry.MarshalMessage(tlcMsg)
		if err != nil {
			return err
		}
		err = n.Broadcast(tlcMarshaled)
		if err != nil {
			return err
		}
	}

	return err
}
