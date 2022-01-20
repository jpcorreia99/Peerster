package registry

import (
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// Registry defines a registry to process messages and transform them to be used
// on the network. It also adds functions to get the activity and status of the
// registry.
type Registry interface {
	// RegisterMessageCallback registers a function that will be executed for
	// that particular type of message by the ProcessPacket function.
	RegisterMessageCallback(types.Message, Exec)

	// ProcessPacket executes the registered callback based on the pkt.Message.
	ProcessPacket(pkt transport.Packet) error

	// MarshalMessage transforms the message to a transport.Message. The message
	// MUST be a pointer.
	MarshalMessage(types.Message) (transport.Message, error)

	// UnmarshalMessage transforms a transport.Message to its corresponding
	// types.Message.
	UnmarshalMessage(*transport.Message, types.Message) error

	// RegisterNotify registers an Exec function that will be called each time
	// Process packet is called. The return error of Exec is not taken into
	// account.
	RegisterNotify(Exec)

	// GetMessages must returns all the messages processed so far with the
	// ProcessPacket function.
	GetMessages() []types.Message
}

// Exec is the type of function executed as a handler on a message.
type Exec func(types.Message, transport.Packet) error
