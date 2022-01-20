package udp

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	connection, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return nil, err
	}

	return &Socket{
		address:         connection.LocalAddr().String(),
		connection:      connection,
		receivedPackets: packets{},
		sentPackets:     packets{},
		mutex: sync.Mutex{},
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	address string
	connection *net.UDPConn
	receivedPackets packets
	sentPackets packets
	mutex 	sync.Mutex
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	return s.connection.Close()
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	dstUdpAddress, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return  err
	}

	data, err := pkt.Marshal()
	if err != nil {
		return err
	}

	if timeout != 0{
		err = s.connection.SetWriteDeadline(time.Now().Add(timeout))
	}else{
		err = s.connection.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		return  err
	}

	_, err = s.connection.WriteToUDP(data, dstUdpAddress)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded){
			err = transport.TimeoutErr(timeout)
		}
		return err
	}

	s.sentPackets.add(pkt)

	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	buffer := make([]byte, bufSize)

	var err error = nil
	if timeout != 0{
		err = s.connection.SetReadDeadline(time.Now().Add(timeout))
	}else{
		err = s.connection.SetReadDeadline(time.Time{})
	}
	if err != nil {
		return transport.Packet{}, err
	}

	nBytesRead, _, err := s.connection.ReadFromUDP(buffer)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded){
			err = transport.TimeoutErr(timeout)
		}
		return transport.Packet{},err
	}

	// must be a pointer since it's internal structure id modified by Unmarshal()
	var pkt *transport.Packet = &transport.Packet{}
	err = pkt.Unmarshal(buffer[:nBytesRead])
	if err != nil {
		return transport.Packet{}, err
	}

	s.receivedPackets.add(*pkt)
	return *pkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.address
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.receivedPackets.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.sentPackets.getAll()
}


type packets struct {
	sync.RWMutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt)
}

func (p *packets) getAll() []transport.Packet {
	p.RLock()
	defer p.RUnlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}