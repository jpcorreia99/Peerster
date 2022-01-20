package types

import "fmt"

// -----------------------------------------------------------------------------
// PaxosPrepareMessage

// NewEmpty implements types.Message.
func (p PaxosPrepareMessage) NewEmpty() Message {
	return &PaxosPrepareMessage{}
}

// Name implements types.Message.
func (p PaxosPrepareMessage) Name() string {
	return "paxosprepare"
}

// String implements types.Message.
func (p PaxosPrepareMessage) String() string {
	return fmt.Sprintf("{paxosprepare %d - %d}", p.Step, p.ID)
}

// HTML implements types.Message.
func (p PaxosPrepareMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PaxosPromiseMessage

// NewEmpty implements types.Message.
func (p PaxosPromiseMessage) NewEmpty() Message {
	return &PaxosPromiseMessage{}
}

// Name implements types.Message.
func (p PaxosPromiseMessage) Name() string {
	return "paxospromise"
}

// String implements types.Message.
func (p PaxosPromiseMessage) String() string {
	return fmt.Sprintf("{paxospromise %d - %d (%d: %v)}", p.Step, p.ID,
		p.AcceptedID, p.AcceptedValue)
}

// HTML implements types.Message.
func (p PaxosPromiseMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PaxosProposeMessage

// NewEmpty implements types.Message.
func (p PaxosProposeMessage) NewEmpty() Message {
	return &PaxosProposeMessage{}
}

// Name implements types.Message.
func (p PaxosProposeMessage) Name() string {
	return "paxospropose"
}

// String implements types.Message.
func (p PaxosProposeMessage) String() string {
	return fmt.Sprintf("{paxospropose %d - %d (%v)}", p.Step, p.ID, p.Value)
}

// HTML implements types.Message.
func (p PaxosProposeMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// PaxosAcceptMessage

// NewEmpty implements types.Message.
func (p PaxosAcceptMessage) NewEmpty() Message {
	return &PaxosAcceptMessage{}
}

// Name implements types.Message.
func (p PaxosAcceptMessage) Name() string {
	return "paxosaccept"
}

// String implements types.Message.
func (p PaxosAcceptMessage) String() string {
	return fmt.Sprintf("{paxosaccept %d - %d (%v)}", p.Step, p.ID, p.Value)
}

// HTML implements types.Message.
func (p PaxosAcceptMessage) HTML() string {
	return p.String()
}

// -----------------------------------------------------------------------------
// TLCMessage

// NewEmpty implements types.Message.
func (p TLCMessage) NewEmpty() Message {
	return &TLCMessage{}
}

// Name implements types.Message.
func (p TLCMessage) Name() string {
	return "tlc"
}

// String implements types.Message.
func (p TLCMessage) String() string {
	return fmt.Sprintf("{tlc %d - (%v)}", p.Step, p.Block)
}

// HTML implements types.Message.
func (p TLCMessage) HTML() string {
	return p.String()
}
