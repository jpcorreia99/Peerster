package types

import (
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------------
// DataRequestMessage

// NewEmpty implements types.Message.
func (d DataRequestMessage) NewEmpty() Message {
	return &DataRequestMessage{}
}

// Name implements types.Message.
func (d DataRequestMessage) Name() string {
	return "datarequest"
}

// String implements types.Message.
func (d DataRequestMessage) String() string {
	return fmt.Sprintf("datarequest{key:%s, id:%s}", d.Key[:8], d.RequestID)
}

// HTML implements types.Message.
func (d DataRequestMessage) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
// DataReplyMessage

// NewEmpty implements types.Message.
func (d DataReplyMessage) NewEmpty() Message {
	return &DataReplyMessage{}
}

// Name implements types.Message.
func (d DataReplyMessage) Name() string {
	return "datareply"
}

// String implements types.Message.
func (d DataReplyMessage) String() string {
	return fmt.Sprintf("datareply{key:%s, id:%s}", d.Key[:8], d.RequestID)
}

// HTML implements types.Message.
func (d DataReplyMessage) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
// SearchRequestMessage

// NewEmpty implements types.Message.
func (s SearchRequestMessage) NewEmpty() Message {
	return &SearchRequestMessage{}
}

// Name implements types.Message.
func (s SearchRequestMessage) Name() string {
	return "searchrequest"
}

// String implements types.Message.
func (s SearchRequestMessage) String() string {
	return fmt.Sprintf("searchrequest{%s %d}", s.Pattern, s.Budget)
}

// HTML implements types.Message.
func (s SearchRequestMessage) HTML() string {
	return s.String()
}

// -----------------------------------------------------------------------------
// SearchReplyMessage

// NewEmpty implements types.Message.
func (s SearchReplyMessage) NewEmpty() Message {
	return &SearchReplyMessage{}
}

// Name implements types.Message.
func (s SearchReplyMessage) Name() string {
	return "searchreply"
}

// String implements types.Message.
func (s SearchReplyMessage) String() string {
	return fmt.Sprintf("searchreply{%v}", s.Responses)
}

// HTML implements types.Message.
func (s SearchReplyMessage) HTML() string {
	return fmt.Sprintf("searchreply %s", s.RequestID)
}

func (f FileInfo) String() string {
	out := new(strings.Builder)

	out.WriteString("fileinfo{")
	if len(f.Metahash) > 8 {
		fmt.Fprintf(out, "%s,%s,", f.Metahash[:8], f.Name)
	} else {
		fmt.Fprintf(out, "%s,%s,", f.Metahash, f.Name)
	}

	chunks := make([]string, len(f.Chunks))
	for i, c := range f.Chunks {
		if len(c) > 8 {
			chunks[i] = string(c)[:8]
		} else {
			chunks[i] = string(c)
		}

	}

	fmt.Fprintf(out, "{%s}}", strings.Join(chunks, ","))

	return out.String()
}
