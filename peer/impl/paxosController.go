package impl

import (
	"crypto"
	"encoding/hex"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"strconv"
	"sync"
)

type paxosAcceptorProcessingStructure struct {
	maxID         uint // max id of a paxos prepare message it has received
	acceptedID    uint // id of the request whose value it has accepted
	acceptedValue types.PaxosValue
	mutex         sync.Mutex
}

func newPaxosAcceptorProcessingStructure() *paxosAcceptorProcessingStructure {
	return &paxosAcceptorProcessingStructure{
		maxID:         0,
		acceptedID:    0,
		acceptedValue: types.PaxosValue{},
		mutex:         sync.Mutex{},
	}
}

type paxosProposerProcessingStructure struct {
	phase                   uint
	currentID               uint
	numberOfPeers           uint
	threshold               uint
	promiseCount            uint
	promiseThresholdChannel chan types.PaxosValue
	highestCollectedID      uint             // from the promise messages, if any of them returns a value, chose the one with highest ID
	highestCollectedValue   types.PaxosValue // from the promise messages, if any of them returns a value, chose the one with highest ID
	selfProposedValue       types.PaxosValue // value to be proposed by the
}

func newPaxosProposerProcessingStructure(configuration peer.Configuration) *paxosProposerProcessingStructure {
	return &paxosProposerProcessingStructure{
		phase:                   1,
		numberOfPeers:           configuration.TotalPeers,
		currentID:               configuration.PaxosID,
		threshold:               uint(configuration.PaxosThreshold(configuration.TotalPeers)),
		promiseCount:            0,
		promiseThresholdChannel: make(chan types.PaxosValue, 1),
		highestCollectedValue:   types.PaxosValue{},
		selfProposedValue:       types.PaxosValue{},
	}
}

type valueMap map[types.PaxosValue]uint
type paxosController struct {
	configuration                    peer.Configuration
	active                           bool
	step                             uint
	threshold                        uint
	IsProposer                       bool // if the node is also a proposer in this instance
	paxosAcceptorProcessingStructure *paxosAcceptorProcessingStructure
	paxosProposerProcessingStructure *paxosProposerProcessingStructure
	valueMap                         valueMap              // paxosValue, count, used to check if a threshold of accept messages for one value has been reached
	paxosAgreedValueChannel          chan types.PaxosValue // returns the value agreed upon
	decidedValue                     types.PaxosValue
	paxosTerminationChannel          chan struct{}
	mutex                            sync.Mutex
	tlcMap                           map[uint][]types.TLCMessage // list of tlc messages received for each step
}

func newPaxosController(configuration peer.Configuration) *paxosController {
	return &paxosController{
		configuration:                    configuration,
		active:                           false,
		step:                             0,
		IsProposer:                       false,
		threshold:                        uint(configuration.PaxosThreshold(configuration.TotalPeers)),
		paxosAcceptorProcessingStructure: newPaxosAcceptorProcessingStructure(),
		paxosProposerProcessingStructure: newPaxosProposerProcessingStructure(configuration),
		valueMap:                         make(map[types.PaxosValue]uint),
		paxosAgreedValueChannel:          make(chan types.PaxosValue, 1), // make it non blocking
		paxosTerminationChannel:          make(chan struct{}),
		mutex:                            sync.Mutex{},
		tlcMap:                           make(map[uint][]types.TLCMessage),
	}
}

func (pc *paxosController) resetPaxosController() {
	pc.IsProposer = false
	pc.active = false
	pc.paxosAcceptorProcessingStructure = newPaxosAcceptorProcessingStructure()
	pc.paxosProposerProcessingStructure = newPaxosProposerProcessingStructure(pc.configuration)
	pc.valueMap = make(map[types.PaxosValue]uint)
	pc.paxosAgreedValueChannel <- pc.decidedValue
	close(pc.paxosAgreedValueChannel)
	pc.paxosAgreedValueChannel = make(chan types.PaxosValue, 1)
	close(pc.paxosTerminationChannel)
	pc.paxosTerminationChannel = make(chan struct{})
}

// tries setting up the process as a proposer in the paxos round
// returns a boolean indicating if there's already an instance going on
// returns a termination channel if there's already an instance going on
func (pc *paxosController) setAsProposer() (bool, chan struct{}) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	// there's already an instance running
	if pc.active {
		return true, pc.paxosTerminationChannel
	}

	// succesfully started a new paxos instance
	pc.active = true
	pc.IsProposer = true
	return false, nil
}

func (pc *paxosController) getStep() uint {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	return pc.step
}

func (pc *paxosController) buildProposeMsg(value types.PaxosValue) types.PaxosProposeMessage {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	return types.PaxosProposeMessage{
		Step:  pc.step,
		ID:    pc.paxosProposerProcessingStructure.currentID,
		Value: value,
	}
}

// no lock since it's only used internally
// reaches consensus, adding a new block to the blockchain
// returns a TLC message to broadcast
func (pc *paxosController) processConsensus(agreedValue types.PaxosValue, peer *node) (*types.TLCMessage, error) {
	pc.decidedValue = agreedValue
	blockchain := peer.configuration.Storage.GetBlockchainStore()

	prevHash := blockchain.Get(storage.LastBlockKey)
	if prevHash == nil {
		prevHash = make([]byte, 32)
	}

	valueToHash := append([]byte(strconv.Itoa(int(pc.step))), []byte(agreedValue.UniqID)...)
	valueToHash = append(valueToHash, []byte(agreedValue.Filename)...)
	valueToHash = append(valueToHash, []byte(agreedValue.Metahash)...)
	valueToHash = append(valueToHash, prevHash...)

	h := crypto.SHA256.New()
	h.Write(valueToHash)
	blockHash := h.Sum(nil)

	block := types.BlockchainBlock{
		Index:    pc.step, // because it was already updated
		Hash:     blockHash,
		Value:    agreedValue,
		PrevHash: prevHash,
	}

	err := pc.addBlock(block, peer)
	if err != nil {
		return nil, err
	}

	tlc := types.TLCMessage{
		Step:  pc.step,
		Block: block,
	}

	pc.step++

	err = pc.processFutureTlCSteps(peer)

	return &tlc, err
}

func (pc *paxosController) processFutureTlCSteps(peer *node) error {
	for uint(len(pc.tlcMap[pc.step])) >= pc.threshold { // check if there are enough tlc messages on future steps to also broadcast
		tlcMsg := pc.tlcMap[pc.step][0]
		err := pc.addBlock(tlcMsg.Block, peer)
		if err != nil {
			return err
		}
		pc.step++

	}

	return nil
}

func (pc *paxosController) addBlock(block types.BlockchainBlock, peer *node) error {
	blockHash := block.Hash
	blockchain := peer.configuration.Storage.GetBlockchainStore()

	key := hex.EncodeToString(blockHash)

	marshaledBlock, err := block.Marshal()
	if err != nil {
		return err
	}
	blockchain.Set(key, marshaledBlock) // store the block

	blockchain.Set(storage.LastBlockKey, blockHash) // store the hash of this block has the last's block hash

	namingStore := peer.configuration.Storage.GetNamingStore()
	namingStore.Set(block.Value.Filename, []byte(block.Value.Metahash))

	return nil
}

func (pc *paxosController) getAgreedValueChannel() chan types.PaxosValue {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	return pc.paxosAgreedValueChannel
}

// --------------proxies: proposer -------------

func (pc *paxosController) incrementProposerID() {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.paxosProposerProcessingStructure.currentID += pc.paxosProposerProcessingStructure.numberOfPeers

}

// sets the value as the proposed one, returns the channel to signal the number of promises has been reached and the msg to broadcasr
func (pc *paxosController) proposer_setSelfProposedValue(value types.PaxosValue) (chan types.PaxosValue, types.PaxosPrepareMessage) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.paxosProposerProcessingStructure.selfProposedValue = value
	pc.paxosProposerProcessingStructure.phase = 1
	pc.paxosProposerProcessingStructure.promiseCount = 0

	paxosPrepareMessage := types.PaxosPrepareMessage{
		Step:   pc.step,
		ID:     pc.paxosProposerProcessingStructure.currentID,
		Source: "",
	}

	return pc.paxosProposerProcessingStructure.promiseThresholdChannel, paxosPrepareMessage
}

// ---------- proxies: callbacks ------------------
// returns the promise msg to be sent as an answer
func (pc *paxosController) prepareCallback(paxosPrepareMsg types.PaxosPrepareMessage) (*types.PaxosPromiseMessage, error) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	step := pc.step
	if step != paxosPrepareMsg.Step {
		return nil, nil
	}

	maxID := pc.paxosAcceptorProcessingStructure.maxID
	if paxosPrepareMsg.ID <= 0 || paxosPrepareMsg.ID <= maxID {
		return nil, nil
	}

	pc.active = true
	pc.paxosAcceptorProcessingStructure.maxID = paxosPrepareMsg.ID

	// may be nil and 0 if the node hasn't accepted anything yet
	currentAcceptedValue := pc.paxosAcceptorProcessingStructure.acceptedValue
	acceptedID := pc.paxosAcceptorProcessingStructure.acceptedID

	var acceptedValuePtr *types.PaxosValue
	if acceptedID != 0 {
		acceptedValuePtr = &currentAcceptedValue
	}

	// NOTE: AcceptedValue will in most cases be nil, since the node hasn't accepted any value yet
	paxosPromiseMsg := types.PaxosPromiseMessage{
		Step:          step,
		ID:            paxosPrepareMsg.ID,
		AcceptedID:    acceptedID,
		AcceptedValue: acceptedValuePtr,
	}

	return &paxosPromiseMsg, nil
}

func (pc *paxosController) promiseCallback(paxosPromiseMsg types.PaxosPromiseMessage) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	step := pc.step
	if step != paxosPromiseMsg.Step {
		return
	}

	phase := pc.paxosProposerProcessingStructure.phase
	if phase != 1 {
		return
	}

	pc.paxosProposerProcessingStructure.promiseCount++

	// if we receive a promise message with a value AND the id of that msg is bigger
	// than any other promise message that contained a value, switch the proposed value to that one
	if paxosPromiseMsg.AcceptedID > pc.paxosProposerProcessingStructure.highestCollectedID {
		pc.paxosProposerProcessingStructure.highestCollectedID = paxosPromiseMsg.ID
		pc.paxosProposerProcessingStructure.highestCollectedValue = *paxosPromiseMsg.AcceptedValue
	}

	// advance to phase 2 if promise count hits threshold
	if pc.paxosProposerProcessingStructure.promiseCount == pc.paxosProposerProcessingStructure.threshold {
		pc.paxosProposerProcessingStructure.phase = 2

		// time to signal which value to propose.
		// If we received any promiseMsg with a value inside ( which can be noticed if the highestCollectedID is not 0)
		// we must propose it, otherwise propose our own value
		if pc.paxosProposerProcessingStructure.highestCollectedID != 0 {
			pc.paxosProposerProcessingStructure.promiseThresholdChannel <- pc.paxosProposerProcessingStructure.highestCollectedValue
		} else {
			pc.paxosProposerProcessingStructure.promiseThresholdChannel <- pc.paxosProposerProcessingStructure.selfProposedValue
		}
	}
}

// return an accept message if step and Id was correct
func (pc *paxosController) proposeCallback(paxosProposeMsg types.PaxosProposeMessage) *types.PaxosAcceptMessage {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	if pc.step != paxosProposeMsg.Step {
		return nil
	}

	pc.active = true
	// if the propose msg id does not correspond to the maximum ID the node has seen from a prepare message
	if paxosProposeMsg.ID != pc.paxosAcceptorProcessingStructure.maxID {
		return nil
	}

	if pc.paxosAcceptorProcessingStructure.acceptedID == 0 { // no value from other node accepted before
		pc.paxosAcceptorProcessingStructure.acceptedID = paxosProposeMsg.ID
		pc.paxosAcceptorProcessingStructure.acceptedValue = paxosProposeMsg.Value
	}

	paxosAcceptMsg := types.PaxosAcceptMessage{
		Step:  pc.step,
		ID:    paxosProposeMsg.ID,
		Value: paxosProposeMsg.Value,
	}

	return &paxosAcceptMsg
}

// returns a tlc message if consensus was reached
func (pc *paxosController) acceptCallback(paxosAcceptMsg types.PaxosAcceptMessage, peer *node) (*types.TLCMessage, error) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	if paxosAcceptMsg.Step != pc.step {
		return nil, nil
	}

	pc.active = true

	pc.valueMap[paxosAcceptMsg.Value]++
	if pc.valueMap[paxosAcceptMsg.Value] == pc.threshold {
		tlcMsg, err := pc.processConsensus(paxosAcceptMsg.Value, peer)
		if err != nil {
			return nil, err
		}
		pc.resetPaxosController() // to prepare it for the next paxos instance
		return tlcMsg, err
	}
	return nil, nil
}

// returns true if the TLC message should be rebroadcasted
func (pc *paxosController) tLCCallback(paxosTlcMsg types.TLCMessage, peer *node) (bool, error) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	if paxosTlcMsg.Step < pc.step {
		return false, nil
	}

	pc.tlcMap[paxosTlcMsg.Step] = append(pc.tlcMap[paxosTlcMsg.Step], paxosTlcMsg)

	if paxosTlcMsg.Step == pc.step {
		pc.active = true
	}

	if paxosTlcMsg.Step == pc.step && uint(len(pc.tlcMap[pc.step])) == pc.threshold {
		err := pc.addBlock(paxosTlcMsg.Block, peer)
		pc.decidedValue = paxosTlcMsg.Block.Value
		if err != nil {
			return false, err
		}
		pc.step++

		err = pc.processFutureTlCSteps(peer)
		pc.resetPaxosController()
		return true, err
	} else {
		return false, nil
	}
}
