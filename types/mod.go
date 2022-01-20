package types

// Message defines the type of message that can be marshalled/unmarshalled over
// the network.
type Message interface {
	NewEmpty() Message
	Name() string
	String() string
	HTML() string
}
