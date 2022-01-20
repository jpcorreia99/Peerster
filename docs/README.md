# Peerster design

Peerster refers to a gossip-based P2P system composed of multiple *peers*
interacting with each other. A peer refers to an autonomous entity in the
Peerster system. Note that we use the terms "peer", "node", "gossiper", and
"participant" interchangeably, but they all refer to the same notion of "peer".

## Architecture

A peer is defined by a combination of interfaces, which will be added
incrementally to augment the set of features throughout homeworks. We define a
`Peer` interface in `/peer/mod.go` that is composed of multiple interfaces
defined in the same folder. As homeworks progress, we augment the `Peer`
interface with new additions.

The following stack diagram pictures the architecture of the system at the end
of the course. This diagram provides the big picture of the system, which can
help in understanding the expectations. However, it is not required, nor even
expected, to understand it until the last homework. Furthermore, it only
represents one of the possible architecture. Every implementation can use its
own architecture, as long as it implements correctly the Peer interface.

```
┌──────┐  ┌───────────┬───────────┬───────────────────┐
│      │  │           │           │                   │
│      │  │           │           │ P2P Data naming   │
│      │  │           │           │                   │
│      │  │ P2P text  │ P2P data  ├───────────────────┤
│      │  │ messaging │ sharing   │                   │
│      │  │           │           │ TLC + Blockchain  │
│ Sto- │  │           │           │                   │
│ rage │  │           │           ├───────────────────┤
│      │  │           │           │                   │
│      │  │           │           │ Paxos             │
│      │  │           │           │                   │
│      │  ├───────────┴───────────┴───────────────────┤
│      │  │                                           │
│      │  │ Gossip protocol                           │
│      │  │                                           │
└──────┘  ├───────────────────────────────────────────┤
          │                                           │
          │ Network                                   │
          │                                           │
          └───────────────────────────────────────────┘
```

## Scope of the implementation

You won't be responsible for implementing all aspects of the peer. Your peer
will be passed a `peer.Configuration` object containing some elements that you
will be asked to use. For example, you won't be responsible for handling the
messages serialization yourself. Instead, the peer is provided a
`registry.Registry` object that you must use to process messages and marshal
them. This is the same for the provided `transport.Socket`, which provides
function to send/receive message. We talk more about those later.

## File organization

Emoji points to locations of particular interest for students.

```
├─docs    Documentation
|
├─gui
|  ├─httpnode    Wraps a peer with a REST API (handle a node with HTTP requests)
|  └─web         A Web front-end for an httpnode
|
├─internal       Used for testing/debugging
|
├─peer
|  ├─impl      👉👉👉 Where you will be working. Contains the peer implementation
|  ├─tests           All the tests
|  mod.go         🔎 The peer interface
|  *.go              Definition of interfaces
|
├─registry    
|  ├─proxy           Needed by the binnode for the integrations tests
|  ├─standard        Provided to the peer to handle messages
|  mod.go            The registry interface
|
├─transport     Defines the mean to exchange messages between peers
|  ├─channel    A socket implementation based on channels
|  ├─proxy      A special socket needed by the binnode for the integration tests
|  ├─udp     👉 Socket implementation, based on UDP
|  mod.go    🔎 Contains the Socket interface and packet/message definitions
|
├─types              Defines the types of messages exchanged between peers.
|  *_def.go        🔎 Message types sent/received between peers
|  registry.go        The message registry
```

## Networking

A peer is not responsible for managing its connection from/to the other peers.
For that purpose, the peer receives a `transport.Socket` object as an input
argument, which precisely serves that purpose.

The socket object is defined in `/transport/mod.go`. A peer uses it to receive
and send data to/from other peers. The first job of HW0 is to provide a
socket implementation based on UDP. There is a provided socket implementation
using go channels in `/transport/channel/`. This implementation is used in some
tests.

You'll remark that `Send()` and `Recv()` both have timeouts. Those timeouts can
be used to periodically (for example every second) unblock send/recv operations
in order to perform some checks. For example, in case the node is shutting down,
the node should stop receiving messages.

### Marshal/Unmarshal packets

A socket works with `transport.Packet` elements. However, only raw bytes can be
sent over the network. This is why `transport.Packet` offers marshal/unmarshal
functions that must be used to transform a packet to/from a `[]byte` buffer:

```go
pkt := transport.Packet{}

buf, err := pkt.Marshal()
if err != nil {...}

var newPkt transport.Packet

err = pkt.Unmarshal()
if err != nil {...}
```

### Peer's address

A peer's address is defined by its socket, which is provided in the
configuration the peer receives:

```go
func NewPeer(conf peer.Configuration) peer.Peer {
    ...
    myAddr := conf.Socket.GetAddress()
    ...
}
```

Note that when we refer the the peer's address, we actually mean the peer's
socket address. For simplicity we most of the time don't add that extra
precision. Using port 0 makes the system randomly choose a free port.
`127.0.0.1:0` is extensively used in tests.

## Data over the network

In order to support interoperability between implementations we provide the
strict definition of data exchanged on the network. Each element sent/received
on the network is defined as a `transport.Packet` element. It is the combination
of a `transport.Header` and a `transport.Message` (see `/transport/mod.go`):

```
transport.Packet
┌─────────────────────┐
│                     │
│ transport.Header    │
│ ┌────────────────┐  │
│ │                │  │
│ └────────────────┘  │
│                     │
│ transport.Message   │
│ ┌────────────────┐  │
│ │                │  │
│ └────────────────┘  │
│                     │
└─────────────────────┘
```

A `transport.Message` contains the marshaled representation of a `types.Message`
(see `/types/mod.go`). Two representations are needed to support a message being
sent on the network. `types.Message` (`/types/mod.go`) is the "rich"
representation used by the peer internally, and `transport.Message`
(`/transport/mod.go`) is used over the network.

To make the conversion from a `transport.Message` to its corresponding
`types.Message` you must use a registry. See next section.

The following illustration shows what type of message each element of the system
use:

```
Peer A                                                    Peer B
┌──────────────┐         Socket A       Socket B          ┌──────────────┐
│              │         ┌───────┐      ┌───────┐         │              │
│              │◄───────►│       │◄────►│       │◄───────►│              │
│              │         └───────┘      └───────┘         │              │
└──────────────┘                                          └──────────────┘

│            │                 │          │                 │            │
└────────────┴─────────────────┴──────────┴─────────────────┴────────────┘
types.Message  transport.Packet   []byte   transport.Packet  types.Message
                      ---                         ---
               transport.Header            transport.Header
               transport.Message           transport.Message
```

## Message registry

When a `transport.Packet` is received, the `transport.Message` it contains needs
to be unmarshalled from a buffer (the `Payload: json.RawMessage`) to an actual
meaningful `types.Message` message (ie. transform a `json.RawMessage` payload
to, for example, a `types.ChatMessage`). To ease this process, we provide a
Registry, which automatically maps a packet (`transport.Packet`) to a callback
that will be invoked when using a provided function on the registry. This
registry is provided as input argument to a peer and is defined in
`/registry/mod.go`.

When you create your gossiper, you must call `RegisterMessageCallback` for all
types of different (`types.Message`) messages you can receive. Each homework
will tell you the new messages to include. The `Exec` argument is a function
that is part of your peer implementation. When you receive a packet from the
socket, you must call `ProcessPacket` with the packet received, which will call
the callback registered with `RegisterMessageCallback` for that message. Here is
an example:

```go
// The handler. This function will be called when a chat message is received.
func (m XXX) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
    // cast the message to its actual type. You assume it is the right type.
    chatMsg, ok := msg.(*types.ChatMessage)
    if !ok {
        return xerrors.Errorf("wrong type: %T", msg)
    }

    // do your stuff here with chatMsg...
    
    return nil
}

// The part where you register the handler. Must be done when you initialize
// your peer with every type of message expected.
conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, XXX.ExecChatMessage)

// The part where you call the handler. When you receive a message on the socket
// and want to process it on your peer, use the registry.
pkt, err := conf.Socket.Recv(time.Second * 1)
if err {...}

// if this is a chat message, calls ExecChatMessage()
conf.MessageRegistry.ProcessPacket(pkt)
```

When you need to do the opposite, ie. transform a `types.Message` to a
`transport.Message`, you must use `MarshalMessage()` from the registry and pass
a pointer to the `types.Message` message you want to marshal:

```go
// I want to send a chat message to a peer.
msg := types.ChatMessage{...}

remotePeer := XXX
myAddr := conf.Socket.GetAddress()

transpMsg, err := conf.MessageRegistry.MarshalMessage(&msg)
if err != nil {...}

header := transport.NewHeader(
    myAddr,   // source
    myAddr,   // relay
    neighbor, // destination
    0,        // TTL
)
pkt := transport.Packet{
    Header: &header,
    Msg:    &transpMsg,
}

err = socket.Send(remotePeer, pkt)
if err != nil {...}
```

## REST API

`/gui/httpnode` contains an http server that implements a REST API for a peer.
It allows one to use the peer functionalities via HTTP requests. To start the
http server there is a CLI in `/gui/mod.go`. To use this CLI, go to `/gui/` and
execute `go run mod.go start -h` to see the help.

```
CLI                   HTTP node          REST API
┌────────┐            ┌────────┐         ┌─────────┐
│        │ wrap(peer) │        │ listen  │         │
│        ├───────────►│        ├────────►│         │
└────────┘            └────────┘         └─────────┘
```

## Binary node

`/internal/binnode` contains the implementation of a peer that uses an http
binary (`/gui/mod.go`). The binnode starts an http node and wraps the peer
functionalities with HTTP requests. This binary node is used in integration
tests to mix the student's implementation with some reference implementations
and check the interoperability.

```
 Binnode           CLI binary          HTTP node
┌─────────┐        ┌─────────┐         ┌─────────┐
│         │ 1:call │         │ 2:start │         │
│         ├───────►│         ├────────►│         │
└────┬────┘        └─────────┘         └─────┬───┘
     │                                       │
     │                                       │
     │            REST API                   │
     │ 4:use      ┌─────────┐    3:listen    │
     └───────────►│         │◄───────────────┘
                  │         │
                  └─────────┘
```
