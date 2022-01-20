package standard

import (
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// NewRegistry returns a new initialized registry.
func NewRegistry() registry.Registry {
	return &Registry{
		handlers: make(map[string]registry.Exec),
		notif:    notifications{},
		msgs:     messages{},
	}
}

// Registry defines the standard and default registry.
//
// - implements registry.Registry
type Registry struct {
	sync.RWMutex

	handlers map[string]registry.Exec
	notif    notifications
	msgs     messages
}

// RegisterMessageCallback implements registry.Registry.
func (r *Registry) RegisterMessageCallback(m types.Message, exec registry.Exec) {
	registry.GlobalRegistry.Add(m)

	r.Lock()
	r.handlers[m.Name()] = exec
	r.Unlock()
}

// ProcessPacket implements registry.Registry.
func (r *Registry) ProcessPacket(pkt transport.Packet) error {
	msg, err := registry.GlobalRegistry.GetMessage(pkt.Msg)
	if err != nil {
		return xerrors.Errorf("failed to get message: %v", err)
	}

	r.RLock()
	h, ok := r.handlers[pkt.Msg.Type]
	if !ok {
		return xerrors.Errorf("handler not found for %s", pkt.Msg.Type)
	}
	r.RUnlock()

	wait := make(chan interface{})

	// we use a goroutine to catch a panic from a handler. This is convenient
	// for the test, where we perform some assertions that make panics.
	go func() {
		defer func() {
			res := recover()

			if res != nil {
				wait <- res
			}

			close(wait)
		}()

		err := h(msg, pkt)
		if err != nil {
			wait <- err
		}
	}()

	r.msgs.add(msg)

	res := <-wait
	if res != nil {
		return xerrors.Errorf("failed to call handler: %v", res)
	}

	// call the registered notification handlers, with a timeout
	done := r.notif.ExecAll(msg, pkt)

	select {
	case <-done:
	case <-time.After(time.Second * 20):
		return xerrors.Errorf("notification handlers took too long")
	}

	return nil
}

// MarshalMessage implements registry.Registry.
func (r *Registry) MarshalMessage(msg types.Message) (transport.Message, error) {
	buf, err := json.Marshal(msg)
	if err != nil {
		return transport.Message{}, xerrors.Errorf("failed to marshal: %v", err)
	}

	return transport.Message{
		Type:    msg.Name(),
		Payload: buf,
	}, nil
}

// UnmarshalMessage implements registry.Registry.
func (r *Registry) UnmarshalMessage(transpMsg *transport.Message, msg types.Message) error {
	if reflect.ValueOf(msg).Kind() != reflect.Ptr {
		return xerrors.Errorf("types.Message must be a pointer")
	}

	return json.Unmarshal(transpMsg.Payload, msg)
}

// RegisterNotify implements registry.Registry.
func (r *Registry) RegisterNotify(exec registry.Exec) {
	r.notif.Add(exec)
}

// GetMessages implements registry.Registry.
func (r *Registry) GetMessages() []types.Message {
	return r.msgs.get()
}

type notifications struct {
	sync.Mutex

	h []registry.Exec
}

func (h *notifications) Add(exec registry.Exec) {
	h.Lock()
	defer h.Unlock()

	h.h = append(h.h, exec)
}

// ExecAll executes all the registered notification handler. Note that we don't
// take into account errors returned by those handlers.
func (h *notifications) ExecAll(msg types.Message, pkt transport.Packet) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		h.Lock()
		defer h.Unlock()

		for _, handler := range h.h {
			handler(msg, pkt)
		}

		close(done)
	}()

	return done
}

type messages struct {
	sync.Mutex
	msgs []types.Message
}

func (m *messages) add(msg types.Message) {
	m.Lock()
	defer m.Unlock()

	m.msgs = append(m.msgs, msg)
}

func (m *messages) get() []types.Message {
	m.Lock()
	defer m.Unlock()

	return append([]types.Message{}, m.msgs...)
}
