package transport

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/rs/xid"
)

// Factory defines the general function to create a network.
type Factory func() Transport

// Transport defines the primitives to handler a layer 4 transport.
type Transport interface {
	CreateSocket(address string) (ClosableSocket, error)
}

// Socket describes the primitives for a socket communication element
//
// - Implemented in HW0
type Socket interface {
	// Send sends a msg to the destination. If the timeout is reached without
	// message received, returns a TimeoutErr. A value of 0 means no timeout.
	Send(dest string, pkt Packet, timeout time.Duration) error

	// Recv blocks until a packet is received, or the timeout is reached. In the
	// case the timeout is reached, returns a TimeoutErr. A value of 0 means no
	// timeout.
	Recv(timeout time.Duration) (Packet, error)

	// GetAddress returns the address assigned. Can be useful in the case one
	// provided a :0 address, which makes the system use a random free port.
	GetAddress() string

	// GetIns must return all the messages received so far.
	GetIns() []Packet

	// GetOuts must return all the messages sent so far.
	GetOuts() []Packet
}

// ClosableSocket augments the Socket interface with a close function. We
// differentiate it because a gossiper shouldn't have access to the close
// function of a socket.
type ClosableSocket interface {
	Socket

	// Close closes the connection. It returns an error if the socket is already
	// closed.
	Close() error
}

// TimeoutErr is a type of error used by the network interface if a timeout is
// reached when receiving a packet.
type TimeoutErr time.Duration

// Error implements error. Returns the error string.
func (err TimeoutErr) Error() string {
	return fmt.Sprintf("timeout reached after %d", err)
}

// Is implements error.
func (TimeoutErr) Is(err error) bool {
	_, ok := err.(TimeoutErr)
	return ok
}

// NewHeader returns a new header with initialized fields. If the ttl value is 0
// then it uses the maximum possible ttl value.
func NewHeader(source, relay, destination string, ttl uint) Header {
	if ttl == 0 {
		ttl = math.MaxUint64
	}

	return Header{
		PacketID:    xid.New().String(),
		TTL:         ttl,
		Timestamp:   time.Now().UnixNano(),
		Source:      source,
		RelayedBy:   relay,
		Destination: destination,
	}
}

// Packet is a type of message sent over the network
type Packet struct {
	Header *Header

	Msg *Message
}

// Marshal transforms a packet to something that can be sent over the network.
func (p Packet) Marshal() ([]byte, error) {
	return json.Marshal(&p)
}

// Unmarshal transforms a marshaled packet to an actual packet. Example
// creating a new packet out of a buffer:
//   var packet Packet
//   packet.Unmarshal(buf)
func (p *Packet) Unmarshal(buf []byte) error {
	return json.Unmarshal(buf, p)
}

// Copy returns a copy of the packet
func (p Packet) Copy() Packet {
	h := p.Header.Copy()
	m := p.Msg.Copy()

	return Packet{
		Header: &h,
		Msg:    &m,
	}
}

// Header contains the metadata of a packet needed for its transport.
type Header struct {
	// PacketID is a unique packet identifier. Used for debug purposes.
	PacketID string

	// TTL is the Time To Live of a packet, which is set when the packet is
	// created and decremented by each relayer. A packet is dropped when its TTL
	// is equal to 0.
	TTL uint

	// Timestamp is the creation timetamp of the packet, in nanosecond.
	Timestamp int64

	// Source is the address of the packet's creator.
	Source string

	// RelayedBy is the address of the node that sends the packet. It can be the
	// originator of the packet, in which case Source==RelayedBy, or the address
	// of a node relaying the packet. Each node should update this field when it
	// relays a packet.
	//
	// - Implemented in HW1
	RelayedBy string

	// Destination is empty in the case of a broadcast, otherwise contains the
	// destination address.
	Destination string
}

func (h Header) String() string {
	out := new(strings.Builder)

	timeStr := time.Unix(0, h.Timestamp).Format("04:04:05.000000000")

	fmt.Fprintf(out, "ID: %s\n", h.PacketID)
	fmt.Fprintf(out, "Timestamp: %s\n", timeStr)
	fmt.Fprintf(out, "Source: %s\n", h.Source)
	fmt.Fprintf(out, "RelayedBy: %s\n", h.RelayedBy)
	fmt.Fprintf(out, "Destination: %s\n", h.Destination)
	fmt.Fprintf(out, "TTL: %d\n", h.TTL)

	return out.String()
}

// Copy returns the copy of header
func (h Header) Copy() Header {
	return h
}

// HTML returns an HTML representation of a header.
func (h Header) HTML() string {
	return strings.ReplaceAll(h.String(), "\n", "<br/>")
}

// Message defines the type of message sent over the network. Payload should be
// a json marshalled representation of a types.Message, and Type the
// corresponding message name, available with types.Message.Name().
type Message struct {
	Type    string
	Payload json.RawMessage
}

// Copy returns a copy of the message
func (m Message) Copy() Message {
	return Message{
		Type:    m.Type,
		Payload: append([]byte{}, m.Payload...),
	}
}

// ByPacketID define a type to sort packets by ByPacketID
type ByPacketID []Packet

func (p ByPacketID) Len() int {
	return len(p)
}

func (p ByPacketID) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p ByPacketID) Less(i, j int) bool {
	return p[i].Header.PacketID < p[j].Header.PacketID
}
