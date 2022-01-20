package impl

import (
	"github.com/rs/xid"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
	"time"
)

// ------------------ HW 3 --------------------
// functions responsible for reaching consensus on a file name

// Tag creates a mapping between a (file)name and a metahash.
func (n *node) Tag(name string, mh string) error {
	namingStore := n.configuration.Storage.GetNamingStore()
	previousValue := namingStore.Get(name)
	if previousValue != nil {
		return xerrors.Errorf("Name %v is already in the naming store", name)
	}

	if n.configuration.TotalPeers <= 1 {
		namingStore.Set(name, []byte(mh))
		return nil
	}

	// if there's already a paxos instance running, wait for it to finish
	instanceAlreadyActive, paxosTerminationChannel := n.paxosController.setAsProposer()
	for instanceAlreadyActive {
		select {
		case <-paxosTerminationChannel:
			instanceAlreadyActive, paxosTerminationChannel = n.paxosController.setAsProposer()
		case <-n.terminationChannel: // the peer called close()
			return nil
		}
	}

	initialStep := n.paxosController.getStep()
	for {
		currentStep := n.paxosController.getStep()
		if initialStep != currentStep { // signal that somehow consensus was reached in between loops, very unlikely scenario
			previousValue := namingStore.Get(name)
			if previousValue != nil {
				return nil
			} else {
				return n.Tag(name, mh)
			}
		}

		// execute phase 1 and get which should the value to propose
		valueToPropose, err := n.paxosPhase1(name, mh)
		if err != nil || valueToPropose == nil { // second part of condition only for when the node is shut down
			return err
		}

		paxosProposeMsg := n.paxosController.buildProposeMsg(*valueToPropose)
		paxosProposeMsgMarshaled, err := n.registry.MarshalMessage(&paxosProposeMsg)
		if err != nil {
			return err
		}
		err = n.Broadcast(paxosProposeMsgMarshaled)
		if err != nil {
			return err
		}

		// ask the paxos controler for a channel where the value reached through consensus will be written
		paxosAgreedValueChannel := n.paxosController.getAgreedValueChannel()
		// also set up a timeout timer
		ticker := time.NewTicker(n.configuration.PaxosProposerRetry)
		select {
		case <-n.terminationChannel:
			return nil
		case decidedValue, open := <-paxosAgreedValueChannel: // a value was reached through consensus
			if !open { // node was closed
				return nil
			}

			// if the agreed value was our value, just exit, everything was concluded with success
			if decidedValue.Filename == name && decidedValue.Metahash == mh {
				return nil
			}

			// ELSE: the value reached through consensus was not our value
			time.Sleep(1 * time.Second)
			// I wish I could explain why I have this check here instead of just calling Tag again, but I can't remember
			// and it is needed to run properly
			previousValue := namingStore.Get(name)
			if previousValue != nil {
				return nil
			}
			return n.Tag(name, mh)

		case <-ticker.C: // timeout
			// since SELECT does not let you define a preference of which case to chose if
			// both channels are triggered (paxosAgreedValueChannel and ticker.C a) we still need to check if consensus was reached
			if (n.paxosController.decidedValue != types.PaxosValue{}) { // if this value is not nil, it means consensus was reached
				decidedValue := n.paxosController.decidedValue
				if decidedValue.Filename == name && decidedValue.Metahash == mh {
					return nil
				}

				// ELSE: the value reached through consensus was not our value
				continue
			}
			// increments the proposer ID and returns to the beginning of the loop
			n.paxosController.incrementProposerID()
		}
	}
}

// executes the first phase of paxos as the proposer
func (n *node) paxosPhase1(name string, mh string) (*types.PaxosValue, error) {
	for {
		selfProposedValue := types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		}

		//returns the channel to signal the number of promises has been reached and the msg to broadcast
		// this channel returns which value should be actually be proposed in phase 2
		valueToProposeChannel, paxosPrepareMsg := n.paxosController.proposer_setSelfProposedValue(selfProposedValue)
		paxosPrepareMsg.Source = n.socket.GetAddress()

		paxosPrepareMessageMarshaled, err := n.registry.MarshalMessage(&paxosPrepareMsg)
		if err != nil {
			return nil, err
		}

		err = n.Broadcast(paxosPrepareMessageMarshaled)

		if err != nil {
			return nil, err
		}
		ticker := time.NewTicker(n.configuration.PaxosProposerRetry)
		select {
		case valueToPropose := <-valueToProposeChannel:
			return &valueToPropose, nil
		case <-ticker.C: // timeout, increase proposed ID and restart loop
			n.paxosController.incrementProposerID()
		case <-n.terminationChannel: // node was closed
			return nil, nil
		}
	}
}
