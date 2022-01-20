package registry

import (
	"encoding/json"
	"sync"

	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// GlobalRegistry maintains a global registry of messages. Is is used by the
// proxy registry and the traffic utility.
var GlobalRegistry = globalRegistry{
	RWMutex:  new(sync.RWMutex),
	messages: map[string]types.Message{},
}

func init() {
	GlobalRegistry.Add(types.ChatMessage{})
	GlobalRegistry.Add(types.AckMessage{})
	GlobalRegistry.Add(types.EmptyMessage{})
	GlobalRegistry.Add(types.StatusMessage{})
	GlobalRegistry.Add(types.RumorsMessage{})
	GlobalRegistry.Add(types.PrivateMessage{})
}

type globalRegistry struct {
	*sync.RWMutex
	messages map[string]types.Message
}

// Add adds a new message to the registry. It overwrites if the message has
// already been added.
func (r globalRegistry) Add(m types.Message) {
	r.Lock()
	defer r.Unlock()

	r.messages[m.Name()] = m
}

// GetMessage uses the global registry to transform a transport.Message to a
// types.Message.
func (r globalRegistry) GetMessage(transpMsg *transport.Message) (types.Message, error) {
	r.RLock()
	defer r.RUnlock()

	m, ok := r.messages[transpMsg.Type]
	if !ok {
		return nil, xerrors.Errorf("message not registered: %s", transpMsg.Type)
	}

	// we create a new one because we don't want to update the registry map,
	// which is only used to get an empty message that we can fill.
	msg := m.NewEmpty()

	err := json.Unmarshal(transpMsg.Payload, &msg)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal payload: %v", err)
	}

	return msg, nil
}
