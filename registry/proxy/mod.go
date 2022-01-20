package proxy

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewRegistry creates a new initialized proxy registry.
func NewRegistry() registry.Registry {
	return &Registry{
		registry: make(map[string]types.Message),
	}
}

// Registry defines a proxy registry used by the binnode. This registry can only
// be used for a restricted set of functions. It uses the REST API from an HTTP
// node. SetProxyAddress must be called before an other action.
type Registry struct {
	proxyAddr string

	registry map[string]types.Message
}

// RegisterMessageCallback implements registry.Registry.
func (r *Registry) RegisterMessageCallback(types.Message, registry.Exec) {
	panic("not supported")
}

// ProcessPacket implements registry.Registry.
func (r *Registry) ProcessPacket(pkt transport.Packet) error {
	panic("not supported")
}

// MarshalMessage implements registry.Registry.
func (r *Registry) MarshalMessage(types.Message) (transport.Message, error) {
	panic("not supported")
}

// UnmarshalMessage implements registry.Registry.
func (r *Registry) UnmarshalMessage(*transport.Message, types.Message) error {
	panic("not supported")
}

// RegisterNotify implements registry.Registry.
func (r *Registry) RegisterNotify(registry.Exec) {
	panic("not supported")
}

// GetMessages implements registry.Registry.
func (r *Registry) GetMessages() []types.Message {
	endpoint := "http://" + r.proxyAddr + "/registry/messages"

	resp, err := http.Get(endpoint)
	if err != nil {
		panic("failed to get messages: " + err.Error())
	}

	// we can't just marshal types.Messages and send them. That's why we convert
	// them from/to transport.Messages.

	var transpMsgs []transport.Message

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("failed to read messages: " + err.Error())
	}

	err = json.Unmarshal(content, &transpMsgs)
	if err != nil {
		panic("wrong data from get messages: " + err.Error())
	}

	result := make([]types.Message, len(transpMsgs))

	for i, transpMsg := range transpMsgs {
		msg, err := registry.GlobalRegistry.GetMessage(&transpMsg)
		if err != nil {
			panic("unexpected message: " + transpMsg.Type)
		}

		result[i] = msg
	}

	return result
}

// SetProxyAddress sets the proxy address, to which we can get the messages.
func (r *Registry) SetProxyAddress(addr string) {
	r.proxyAddr = addr
}
