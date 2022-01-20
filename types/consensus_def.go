package types

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// PaxosPrepareMessage defines a prepare message in Paxos
//
// - implements types.Message
// - implemented in HW3
type PaxosPrepareMessage struct {
	Step uint
	ID   uint
	// Source is the address of the peer that sends the prepare
	Source string
}

// PaxosPromiseMessage defines a promise message in Paxos
//
// - implements types.Message
// - implemented in HW3
type PaxosPromiseMessage struct {
	Step uint
	ID   uint

	// Irrelevant if the proposer hasn't accepted any value
	AcceptedID uint
	// Must be nil if the proposer hasn't accepted any value
	AcceptedValue *PaxosValue
}

// PaxosProposeMessage defines a propose message in Paxos
//
// - implements types.Message
// - implemented in HW3
type PaxosProposeMessage struct {
	Step  uint
	ID    uint
	Value PaxosValue
}

// PaxosAcceptMessage defines an accept message in Paxos
//
// - implements types.Message
// - implemented in HW3
type PaxosAcceptMessage struct {
	Step  uint
	ID    uint
	Value PaxosValue
}

// TLCMessage defines a TLC message
//
// - implements types.Message
// - implemented in HW3
type TLCMessage struct {
	Step  uint
	Block BlockchainBlock
}

// PaxosValue defines the value on which Paxos makes a consensus.
type PaxosValue struct {
	// UniqID is used to group and count same values. Use xid.New().String() to
	// generate it.
	UniqID string

	Filename string
	Metahash string
}

// String returns a string representation.
func (p PaxosValue) String() string {
	return fmt.Sprintf("{paxosvalue: %s - %s=>%s}", p.UniqID, p.Filename, p.Metahash)
}

// BlockchainBlock defines the content of a block in the blockchain.
type BlockchainBlock struct {
	// Index is the index of the block in the blockchain, starting at 0 for the
	// first block.
	Index uint

	// Hash is SHA256(Index || v.UniqID || v.Filename || v.Metahash || Prevhash)
	// use crypto/sha256
	Hash []byte

	Value PaxosValue

	// PrevHash is the SHA256 hash of the previous block
	PrevHash []byte
}

// Marshal marshals the BlobkchainBlock into a byte representation. Must be used
// to store blocks in the blockchain store.
func (b *BlockchainBlock) Marshal() ([]byte, error) {
	return json.Marshal(b)
}

// Unmarshal unmarshals the data into the current instance. To unmarshal a
// block:
//
//   var block BlockchainBlock
//   err := block.Unmarshal(buf)
func (b *BlockchainBlock) Unmarshal(data []byte) error {
	return json.Unmarshal(data, b)
}

// String returns a string representation of a blokchain block.
func (b BlockchainBlock) String() string {
	return fmt.Sprintf("{block n°%d H(%x) - %v - %x", b.Index, b.Hash[:4],
		b.Value, b.PrevHash[:4])
}

// DisplayBlock writes a rich string representation of a block
func (b BlockchainBlock) DisplayBlock(out io.Writer) {
	crop := func(s string) string {
		if len(s) > 10 {
			return s[:8] + "..."
		}
		return s
	}

	max := func(s ...string) int {
		max := 0
		for _, se := range s {
			if len(se) > max {
				max = len(se)
			}
		}
		return max
	}

	pad := func(n int, s ...*string) {
		for _, se := range s {
			*se = fmt.Sprintf("%-*s", n, *se)
		}
	}

	row1 := fmt.Sprintf("%d | %x", b.Index, b.Hash[:6])
	row2 := fmt.Sprintf("I | %s", crop(b.Value.UniqID))
	row3 := fmt.Sprintf("F | %s", crop(b.Value.Filename))
	row4 := fmt.Sprintf("M | %s", crop(b.Value.Metahash))
	row5 := fmt.Sprintf("<- %x", b.PrevHash[:6])

	m := max(row1, row2, row3, row4, row5)
	pad(m, &row1, &row2, &row3, &row4, &row5)

	fmt.Fprintf(out, "\n┌%s┐\n", strings.Repeat("─", m+2))
	fmt.Fprintf(out, "│ %s │\n", row1)
	fmt.Fprintf(out, "│%s│\n", strings.Repeat("─", m+2))
	fmt.Fprintf(out, "│ %s │\n", row2)
	fmt.Fprintf(out, "│ %s │\n", row3)
	fmt.Fprintf(out, "│ %s │\n", row4)
	fmt.Fprintf(out, "│%s│\n", strings.Repeat("─", m+2))
	fmt.Fprintf(out, "│ %s │\n", row5)
	fmt.Fprintf(out, "└%s┘\n", strings.Repeat("─", m+2))
}
