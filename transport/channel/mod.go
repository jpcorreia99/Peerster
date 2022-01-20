package channel

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.dedis.ch/cs438/internal/traffic"
	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
)

var counter uint32 = 0

// NewTransport returns a channel-based transport service.
func NewTransport() transport.Transport {
	return &Transport{
		incomings: make(map[string]chan transport.Packet),
		traffic:   traffic.NewTraffic(),
	}
}

// Transport is a simple transport implementation using channels
//
// - implements transport.Transport
// - implements transport.ClosableSocket
type Transport struct {
	sync.RWMutex
	incomings map[string]chan transport.Packet
	traffic   *traffic.Traffic
}

// CreateSocket implements transport.Transport
func (t *Transport) CreateSocket(address string) (transport.ClosableSocket, error) {
	t.Lock()
	if strings.HasSuffix(address, ":0") {
		address = address[:len(address)-2]
		port := atomic.AddUint32(&counter, 1)
		address = fmt.Sprintf("%s:%d", address, port)
	}
	t.incomings[address] = make(chan transport.Packet, 100)
	t.Unlock()

	return &Socket{
		Transport: t,
		myAddr:    address,

		ins:  packets{},
		outs: packets{},
	}, nil
}

// MustCreate returns a socket and panic if something goes wrong. This function
// is mostly usefull in tests.
func (t *Transport) MustCreate(address string) transport.ClosableSocket {
	socket, err := t.CreateSocket(address)
	if err != nil {
		panic("failed to create socket: " + err.Error())
	}

	return socket
}

// Socket provide a network layer using channels.
//
// - implements transport.Socket
type Socket struct {
	*Transport
	myAddr string

	ins  packets
	outs packets
}

// Close implements transport.Socket
func (s *Socket) Close() error {
	s.Lock()
	defer s.Unlock()

	delete(s.incomings, s.myAddr)

	return nil
}

// Send implements transport.Socket.
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	s.RLock()
	to, ok := s.incomings[dest]

	if !ok {
		s.RUnlock()
		return xerrors.Errorf("%s is not listening", dest)
	}
	s.RUnlock()

	if timeout == 0 {
		timeout = math.MaxInt64
	}

	select {
	case to <- pkt.Copy():
	case <-time.After(timeout):
		return transport.TimeoutErr(timeout)
	}

	s.outs.add(pkt)
	s.traffic.LogSent(pkt.Header.RelayedBy, dest, pkt)

	return nil
}

// Recv implements transport.Socket.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	s.RLock()
	myChan := s.incomings[s.myAddr]
	s.RUnlock()

	select {
	case <-time.After(timeout):
		return transport.Packet{}, transport.TimeoutErr(timeout)
	case pkt := <-myChan:
		s.traffic.LogRecv(pkt.Header.RelayedBy, s.myAddr, pkt)
		s.ins.add(pkt)
		return pkt, nil
	}
}

// GetAddress implements transport.Socket.
func (s *Socket) GetAddress() string {
	return s.myAddr
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt.Copy())
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}
