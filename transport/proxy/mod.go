package proxy

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"go.dedis.ch/cs438/transport"
)

// NewProxy returns a proxy socket. Used by the binary node.
func NewProxy() transport.Transport {
	return Proxy{}
}

// Proxy defines a proxy transport.
type Proxy struct {
}

// CreateSocket implements transport.Transport
func (p Proxy) CreateSocket(address string) (transport.ClosableSocket, error) {
	return &Socket{
		socketAddr: address,
	}, nil
}

// Socket is a "fake" socket that proxies the socket used in an http node. It
// can only be used to set/get the address (if a :0 port is given, then the real
// address will be updated) and use the watchers. The send/recv operation are
// done by the real socket. It is used by the binary node, because it can
// provide an actual socket to the http node.
type Socket struct {
	socketAddr string

	// used to get the watchers
	proxyAddr string
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	panic("not supported")
}

// Recv implements transport.Socket
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	panic("not supported")
}

// GetAddress implements transport.Socket
func (s *Socket) GetAddress() string {
	return s.socketAddr
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	endpoint := "http://" + s.proxyAddr + "/socket/ins"

	resp, err := http.Get(endpoint)
	if err != nil {
		panic("failed to get ins: " + err.Error())
	}

	var result []transport.Packet

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("failed to read ins: " + err.Error())
	}

	err = json.Unmarshal(content, &result)
	if err != nil {
		panic("wrong data from get ins: " + err.Error())
	}

	return result
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	endpoint := "http://" + s.proxyAddr + "/socket/outs"

	resp, err := http.Get(endpoint)
	if err != nil {
		panic("failed to get outs: " + err.Error())
	}

	var result []transport.Packet

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("failed to read outs: " + err.Error())
	}

	err = json.Unmarshal(content, &result)
	if err != nil {
		panic("wrong data from get outs: " + err.Error())
	}

	return result
}

// Close implements transport.Socket
func (s *Socket) Close() error {
	return nil
}

// SetSocketAddress updates the socket address. Used when we know the real
// address in case of providing a :0 port.
func (s *Socket) SetSocketAddress(addr string) {
	s.socketAddr = addr
}

// SetProxyAddress sets the proxy address, to which we can get the watchers.
func (s *Socket) SetProxyAddress(addr string) {
	s.proxyAddr = addr
}
