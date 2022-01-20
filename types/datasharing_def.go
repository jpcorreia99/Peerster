package types

// DataRequestMessage describes a message sent to request a data blob.
//
// - implements types.Message
// - implemented in HW2
type DataRequestMessage struct {
	// RequestID must be a unique identifier. Use xid.New().String() to generate
	// it.
	RequestID string

	// Key can be either a hex-encoded metahash or chunk hash
	Key string
}

// DataReplyMessage describes the response to a data request.
//
// - implements types.Message
// - implemented in HW2
type DataReplyMessage struct {
	// RequestID must be the same as the RequestID set in the
	// DataRequestMessage.
	RequestID string

	// Key must be the same as the Key set in the DataRequestMessage.
	Key string
	// Value can be nil in case the responsing peer does not have data
	// associated with the key.
	Value []byte
}

// SearchRequestMessage describes a request to search for a data blob.
//
// - implements types.Message
// - implemented in HW2
type SearchRequestMessage struct {
	// RequestID must be a unique identifier. Use xid.New().String() to generate
	// it.
	RequestID string
	// Origin is the address of the peer that initiated the search request.
	Origin string

	// use regexp.MustCompile(Pattern) to convert a string to a regexp.Regexp
	Pattern string
	Budget  uint
}

// SearchReplyMessage describes the response of a search request.
//
// - implements types.Message
// - implemented in HW2
type SearchReplyMessage struct {
	// RequestID must be the same as the RequestID set in the
	// SearchRequestMessage.
	RequestID string

	Responses []FileInfo
}

// FileInfo is used in a search reply to tell about a data blob ("file")
// being available on a peer.
//
// - implements types.Message
// - implemented in HW2
type FileInfo struct {
	Name     string
	Metahash string

	// len(Chunks) must be of the total number of chunks for that file and
	// chunks must be in order. Missing chunk must be set to nil. For example:
	//
	//   [ []byte("aa"), nil, []byte("cc"), []byte("dd"), nil, nil ]
	//
	// Means that the file has 6 chunks, but the peer only has 3.
	Chunks [][]byte
}
