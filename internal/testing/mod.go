package testing

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"

	"strconv"
	"testing"

	"time"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/registry/standard"

	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/storage/inmemory"

	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewFakeMessage return a new fake message.
func NewFakeMessage(t *testing.T) FakeMessage {
	content := make([]byte, 12)

	_, err := rand.Read(content)
	require.NoError(t, err)

	return FakeMessage{
		Content: content,
	}
}

// FakeMessage defines a fake message that can be used over the network.
//
// - implements types.Message.
type FakeMessage struct {
	Content []byte
}

// NewEmpty implements types.Message.
func (m FakeMessage) NewEmpty() types.Message {
	return &FakeMessage{}
}

// Name implements types.Message.
func (m FakeMessage) Name() string {
	return "fake"
}

// String implements types.Message.
func (m FakeMessage) String() string {
	return fmt.Sprintf("{fake:%x}", m.Content)
}

// HTML implements types.Message.
func (m FakeMessage) HTML() string {
	return m.String()
}

// GetNetMsg return the net.Message representation of the message.
func (m FakeMessage) GetNetMsg(t *testing.T) transport.Message {
	buf, err := json.Marshal(&m)
	require.NoError(t, err)

	return transport.Message{
		Type:    m.Name(),
		Payload: buf,
	}
}

// Compare compare the fake message to a net.Message.
func (m FakeMessage) Compare(t *testing.T, msg *transport.Message) {
	require.Equal(t, m.Name(), msg.Type)

	var newMsg FakeMessage

	err := json.Unmarshal(msg.Payload, &newMsg)
	require.NoError(t, err)

	require.Equal(t, m.Content, newMsg.Content)
}

// GetHandler returns a handler that check the content of the received message
// and closes the channel.
func (m FakeMessage) GetHandler(t *testing.T) (registry.Exec, Status) {
	status := NewStatus()

	return func(msg types.Message, pkt transport.Packet) error {
		defer func() {
			status.Call()
		}()

		fake, ok := msg.(*FakeMessage)
		require.True(t, ok)

		require.Equal(t, m.Content, fake.Content)

		return nil
	}, status
}

// FakeByContent sorts fake message by content
type FakeByContent []*FakeMessage

func (r FakeByContent) Len() int {
	return len(r)
}

func (r FakeByContent) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r FakeByContent) Less(i, j int) bool {
	return bytes.Compare(r[i].Content, r[j].Content) < 0
}

type configTemplate struct {
	messages []types.Message
	handlers []registry.Exec

	registry registry.Registry

	withWatcher bool
	autoStart   bool

	AntiEntropyInterval time.Duration
	HeartbeatInterval   time.Duration

	AckTimeout        time.Duration
	ContinueMongering float64

	chunkSize uint

	storage storage.Storage

	dataRequestBackoff peer.Backoff

	totalPeers         uint
	paxosThreshold     func(uint) int
	paxosID            uint
	paxosProposerRetry time.Duration
}

func newConfigTemplate() configTemplate {
	return configTemplate{
		withWatcher: false,
		autoStart:   true,

		messages: make([]types.Message, 0),
		handlers: make([]registry.Exec, 0),

		registry: standard.NewRegistry(),

		AntiEntropyInterval: 0,
		HeartbeatInterval:   0,

		AckTimeout:        time.Second * 3,
		ContinueMongering: 0.5,

		chunkSize: 8192,

		storage: inmemory.NewPersistency(),

		dataRequestBackoff: peer.Backoff{
			Initial: time.Second * 2,
			Factor:  2,
			Retry:   5,
		},

		totalPeers: 1,
		paxosThreshold: func(u uint) int {
			return int(u/2 + 1)
		},
		paxosID:            0,
		paxosProposerRetry: time.Second * 5,
	}
}

// Option is the type of option when creating a test node.
type Option func(*configTemplate)

// WithAutostart sets the autostart option.
func WithAutostart(autostart bool) Option {
	return func(ct *configTemplate) {
		ct.autoStart = autostart
	}
}

// WithMessage will register the provided message and handler on the node.
func WithMessage(m types.Message, handler registry.Exec) Option {
	return func(ct *configTemplate) {
		ct.messages = append(ct.messages, m)
		ct.handlers = append(ct.handlers, handler)
	}
}

// WithMessageRegistry sets a specific message registry. Used to pass a proxy registry.
func WithMessageRegistry(r registry.Registry) Option {
	return func(ct *configTemplate) {
		ct.registry = r
	}
}

// WithAntiEntropy specifies the antientropy interval.
func WithAntiEntropy(d time.Duration) Option {
	return func(ct *configTemplate) {
		ct.AntiEntropyInterval = d
	}
}

// WithHeartbeat defines the heartbeat interval.
func WithHeartbeat(d time.Duration) Option {
	return func(ct *configTemplate) {
		ct.HeartbeatInterval = d
	}
}

// WithContinueMongering sets the ContinueMongering option.
func WithContinueMongering(c float64) Option {
	return func(ct *configTemplate) {
		ct.ContinueMongering = c
	}
}

// WithAckTimeout sets the AckTimeout option.
func WithAckTimeout(d time.Duration) Option {
	return func(ct *configTemplate) {
		ct.AckTimeout = d
	}
}

// WithChunkSize sets a specific chunk size.
func WithChunkSize(chunkSize uint) Option {
	return func(ct *configTemplate) {
		ct.chunkSize = chunkSize
	}
}

// WithDataRequestBackoff sets a specific data request backoff.
func WithDataRequestBackoff(initial time.Duration, factor uint, retry uint) Option {
	return func(ct *configTemplate) {
		ct.dataRequestBackoff = peer.Backoff{
			Initial: initial,
			Factor:  factor,
			Retry:   retry,
		}
	}
}

// WithStorage sets a specific storage
func WithStorage(storage storage.Storage) Option {
	return func(ct *configTemplate) {
		ct.storage = storage
	}
}

// WithTotalPeers sets a specific TotalPeer value.
func WithTotalPeers(t uint) Option {
	return func(ct *configTemplate) {
		ct.totalPeers = t
	}
}

// WithPaxosID sets a specific paxosID value.
func WithPaxosID(id uint) Option {
	return func(ct *configTemplate) {
		ct.paxosID = id
	}
}

// WithPaxosThreshold sets a specific paxosID value.
func WithPaxosThreshold(f func(uint) int) Option {
	return func(ct *configTemplate) {
		ct.paxosThreshold = f
	}
}

// WithPaxosProposerRetry sets a specific paxosID value.
func WithPaxosProposerRetry(d time.Duration) Option {
	return func(ct *configTemplate) {
		ct.paxosProposerRetry = d
	}
}

// NewTestNode returns a new test node.
func NewTestNode(t *testing.T, f peer.Factory, trans transport.Transport,
	addr string, opts ...Option) TestNode {

	template := newConfigTemplate()
	for _, opt := range opts {
		opt(&template)
	}

	socket, err := trans.CreateSocket(addr)
	require.NoError(t, err)

	config := peer.Configuration{}

	config.Socket = socket
	config.MessageRegistry = template.registry
	config.AntiEntropyInterval = template.AntiEntropyInterval
	config.HeartbeatInterval = template.HeartbeatInterval
	config.ContinueMongering = template.ContinueMongering
	config.AckTimeout = template.AckTimeout
	config.Storage = template.storage
	config.ChunkSize = template.chunkSize
	config.BackoffDataRequest = template.dataRequestBackoff
	config.TotalPeers = template.totalPeers
	config.PaxosThreshold = template.paxosThreshold
	config.PaxosID = template.paxosID
	config.PaxosProposerRetry = template.paxosProposerRetry

	node := f(config)

	require.Equal(t, len(template.messages), len(template.handlers))
	for i, msg := range template.messages {
		config.MessageRegistry.RegisterMessageCallback(msg, template.handlers[i])
	}

	if template.autoStart {
		err := node.Start()
		require.NoError(t, err)
	}

	return TestNode{
		Peer:   node,
		config: config,
		socket: socket,
		t:      t,
	}
}

// TestNode defines a test node. It overides peer.Peer with additional functions
// for testing.
type TestNode struct {
	peer.Peer
	config peer.Configuration
	socket transport.ClosableSocket
	t      *testing.T
}

// GetAddr returns the node's socket address
func (t *TestNode) GetAddr() string {
	return t.socket.GetAddress()
}

// StopAll stops the peer and socket.
func (t *TestNode) StopAll() {
	t.Peer.Stop()
	err := t.socket.Close()
	require.NoError(t.t, err)
}

// GetIns returns all the messages received so far.
func (t TestNode) GetIns() []transport.Packet {
	return t.socket.GetIns()
}

// GetOuts returns all the messages sent so far.
func (t TestNode) GetOuts() []transport.Packet {
	return t.socket.GetOuts()
}

// GetRegistry returns the node's registry
func (t TestNode) GetRegistry() registry.Registry {
	return t.config.MessageRegistry
}

// GetFakes filters out all the processed messages of type FakeMessage
func (t TestNode) GetFakes() []*FakeMessage {
	msgs := t.config.MessageRegistry.GetMessages()

	fakes := make([]*FakeMessage, 0)

	for _, msg := range msgs {
		fake, ok := msg.(*FakeMessage)
		if ok {
			fakes = append(fakes, fake)
		}
	}

	return fakes
}

// GetChatMsgs filters out all the processed messages of type ChatMessage
func (t TestNode) GetChatMsgs() []*types.ChatMessage {
	msgs := t.config.MessageRegistry.GetMessages()

	chatMsgs := make([]*types.ChatMessage, 0)

	for _, msg := range msgs {
		chatMsg, ok := msg.(*types.ChatMessage)
		if ok {
			chatMsgs = append(chatMsgs, chatMsg)
		}
	}

	return chatMsgs
}

// GetStorage returns the storage provided to the node.
func (t TestNode) GetStorage() storage.Storage {
	return t.config.Storage
}

// Status allows to check if something has been called or not.
type Status struct {
	called chan struct{}
}

// NewStatus return a new initialized Status.
func NewStatus() Status {
	return Status{
		called: make(chan struct{}),
	}
}

// Call notifies that the status has been called.
func (s Status) Call() {
	select {
	case <-s.called:
	default:
		close(s.called)
	}
}

// CheckCalled checks if the status has been called.
func (s Status) CheckCalled(t *testing.T) {
	select {
	case <-s.called:
	default:
		t.Error("has not been called")
	}
}

// CheckNotCalled checks if the status has been called.
func (s Status) CheckNotCalled(t *testing.T) {
	select {
	case <-s.called:
		t.Error("has been called")
	default:
	}
}

// GetChat returns the ChatMessage associated to the transport.Message.
func GetChat(t *testing.T, msg *transport.Message) types.ChatMessage {
	require.Equal(t, "chat", msg.Type)

	var chatMessage types.ChatMessage

	err := json.Unmarshal(msg.Payload, &chatMessage)
	require.NoError(t, err)

	return chatMessage
}

// GetRumor returns the rumor associated to the transport.Message.
func GetRumor(t *testing.T, msg *transport.Message) types.RumorsMessage {
	require.Equal(t, "rumor", msg.Type)

	var rumor types.RumorsMessage

	err := json.Unmarshal(msg.Payload, &rumor)
	require.NoError(t, err)

	return rumor
}

// GetAck returns the Ack associated to the transport.Message.
func GetAck(t *testing.T, msg *transport.Message) types.AckMessage {
	require.Equal(t, "ack", msg.Type)

	var ack types.AckMessage

	err := json.Unmarshal(msg.Payload, &ack)
	require.NoError(t, err)

	return ack
}

// GetStatus returns the Status associated to the transport.Message.
func GetStatus(t *testing.T, msg *transport.Message) types.StatusMessage {
	require.Equal(t, "status", msg.Type)

	var status types.StatusMessage

	err := json.Unmarshal(msg.Payload, &status)
	require.NoError(t, err)

	return status
}

// GetEmpty returns the EmptyMessage associated to the transport.Message.
func GetEmpty(t *testing.T, msg *transport.Message) types.EmptyMessage {
	require.Equal(t, "empty", msg.Type)

	var emptyMessage types.EmptyMessage

	err := json.Unmarshal(msg.Payload, &emptyMessage)
	require.NoError(t, err)

	return emptyMessage
}

// GetDataRequest returns the DataRequest associated to the transport.Message.
func GetDataRequest(t *testing.T, msg *transport.Message) types.DataRequestMessage {
	require.Equal(t, "datarequest", msg.Type)

	var dataRequestMessage types.DataRequestMessage

	err := json.Unmarshal(msg.Payload, &dataRequestMessage)
	require.NoError(t, err)

	return dataRequestMessage
}

// GetDataReply returns the DataReply associated to the transport.Message.
func GetDataReply(t *testing.T, msg *transport.Message) types.DataReplyMessage {
	require.Equal(t, "datareply", msg.Type)

	var dataReplyMessage types.DataReplyMessage

	err := json.Unmarshal(msg.Payload, &dataReplyMessage)
	require.NoError(t, err)

	return dataReplyMessage
}

// GetSearchRequest returns the SearchRequest associated to the transport.Message.
func GetSearchRequest(t *testing.T, msg *transport.Message) types.SearchRequestMessage {
	require.Equal(t, "searchrequest", msg.Type)

	var searchRequestMessage types.SearchRequestMessage

	err := json.Unmarshal(msg.Payload, &searchRequestMessage)
	require.NoError(t, err)

	return searchRequestMessage
}

// GetSearchReply returns the SearchReply associated to the transport.Message.
func GetSearchReply(t *testing.T, msg *transport.Message) types.SearchReplyMessage {
	require.Equal(t, "searchreply", msg.Type)

	var searchReplyMessage types.SearchReplyMessage

	err := json.Unmarshal(msg.Payload, &searchReplyMessage)
	require.NoError(t, err)

	return searchReplyMessage
}

// GetPaxosPrepare returns the PaxosPrepare associated to the transport.Message.
func GetPaxosPrepare(t *testing.T, msg *transport.Message) types.PaxosPrepareMessage {
	require.Equal(t, "paxosprepare", msg.Type)

	var paxosPrepareMessage types.PaxosPrepareMessage

	err := json.Unmarshal(msg.Payload, &paxosPrepareMessage)
	require.NoError(t, err)

	return paxosPrepareMessage
}

// GetPaxosPromise returns the PaxosPromise associated to the transport.Message.
func GetPaxosPromise(t *testing.T, msg *transport.Message) types.PaxosPromiseMessage {
	require.Equal(t, "paxospromise", msg.Type)

	var paxosPromiseMessage types.PaxosPromiseMessage

	err := json.Unmarshal(msg.Payload, &paxosPromiseMessage)
	require.NoError(t, err)

	return paxosPromiseMessage
}

// GetPaxosPropose returns the PaxosPropose associated to the transport.Message.
func GetPaxosPropose(t *testing.T, msg *transport.Message) types.PaxosProposeMessage {
	require.Equal(t, "paxospropose", msg.Type)

	var paxosProposeMessage types.PaxosProposeMessage

	err := json.Unmarshal(msg.Payload, &paxosProposeMessage)
	require.NoError(t, err)

	return paxosProposeMessage
}

// GetPaxosAccept returns the PaxosAccept associated to the transport.Message.
func GetPaxosAccept(t *testing.T, msg *transport.Message) types.PaxosAcceptMessage {
	require.Equal(t, "paxosaccept", msg.Type)

	var paxosAcceptMessage types.PaxosAcceptMessage

	err := json.Unmarshal(msg.Payload, &paxosAcceptMessage)
	require.NoError(t, err)

	return paxosAcceptMessage
}

// GetTLC returns the TLC associated to the transport.Message.
func GetTLC(t *testing.T, msg *transport.Message) types.TLCMessage {
	require.Equal(t, "tlc", msg.Type)

	var tlcMessage types.TLCMessage

	err := json.Unmarshal(msg.Payload, &tlcMessage)
	require.NoError(t, err)

	return tlcMessage
}

// GetPrivate returns the Private message associated to the transport.Message.
func GetPrivate(t *testing.T, msg *transport.Message) types.PrivateMessage {
	require.Equal(t, "private", msg.Type)

	var privateMessage types.PrivateMessage

	err := json.Unmarshal(msg.Payload, &privateMessage)
	require.NoError(t, err)

	return privateMessage
}

// DisplayBlokchainBlocks writes a string representation of all blocks store in
// the storage.
func DisplayBlokchainBlocks(t *testing.T, out io.Writer, store storage.Store) {
	lastBlockHashHex := hex.EncodeToString(store.Get(storage.LastBlockKey))
	endBlockHasHex := hex.EncodeToString(make([]byte, 32))

	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := store.Get(string(lastBlockHashHex))

		var lastBlock types.BlockchainBlock

		err := lastBlock.Unmarshal(lastBlockBuf)
		require.NoError(t, err)

		lastBlock.DisplayBlock(out)

		lastBlockHashHex = hex.EncodeToString(lastBlock.PrevHash)
	}
}

// DisplayLastBlockchainBlock writes the string representation of the last
// blockchain block.
func DisplayLastBlockchainBlock(t *testing.T, out io.Writer, store storage.Store) {
	lastBlockHashHex := hex.EncodeToString(store.Get(storage.LastBlockKey))
	lastBlockBuf := store.Get(string(lastBlockHashHex))

	var lastBlock types.BlockchainBlock

	err := lastBlock.Unmarshal(lastBlockBuf)
	require.NoError(t, err)

	lastBlock.DisplayBlock(out)
}

// ValidateBlockchain parses the whole blockchain and checks the hash of each
// block.
func ValidateBlockchain(t *testing.T, store storage.Store) {
	lastBlockHashHex := hex.EncodeToString(store.Get(storage.LastBlockKey))

	endBlockHasHex := hex.EncodeToString(make([]byte, 32))
	var block types.BlockchainBlock

	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := store.Get(string(lastBlockHashHex))

		err := block.Unmarshal(lastBlockBuf)
		require.NoError(t, err)

		h := sha256.New()

		h.Write([]byte(strconv.Itoa(int(block.Index))))
		h.Write([]byte(block.Value.UniqID))
		h.Write([]byte(block.Value.Filename))
		h.Write([]byte(block.Value.Metahash))
		h.Write(block.PrevHash)

		blockHash := h.Sum(nil)

		require.Equal(t, blockHash, block.Hash)

		lastBlockHashHex = hex.EncodeToString(block.PrevHash)
	}

	require.Equal(t, uint(0), block.Index)
}

// GetRandBytes returns random bytes.
func GetRandBytes(t *testing.T) []byte {
	res := make([]byte, 12)

	_, err := rand.Read(res)
	require.NoError(t, err)

	return res
}

// GetRandString returns a random string.
func GetRandString() string {
	charset := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	res := make([]byte, 12)
	for i := range res {
		res[i] = charset[rand.Intn(len(charset))]
	}

	return string(res)
}

// GetRandomPkt return a packet containing randomly filled fields.
func GetRandomPkt(t *testing.T) transport.Packet {
	pkt := transport.Packet{
		Header: &transport.Header{
			PacketID:    GetRandString(),
			TTL:         uint(rand.Int()),
			Timestamp:   rand.Int63(),
			Source:      GetRandString(),
			RelayedBy:   GetRandString(),
			Destination: GetRandString(),
		},
		Msg: &transport.Message{
			Type:    GetRandString(),
			Payload: []byte(fmt.Sprintf(`{"data":"%s"}`, GetRandString())),
		},
	}

	return pkt
}

// GetSocket is a handy function to create a socket and check for error
func GetSocket(t *testing.T, transp transport.Transport, addr string) transport.ClosableSocket {
	sock, err := transp.CreateSocket(addr)
	require.NoError(t, err)

	return sock
}

// Terminable describes a peer that have a terminate function. Which is the case
// if this is a binnode.
type Terminable interface {
	Terminate() error
}
