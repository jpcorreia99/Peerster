package unit

import (
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/peer/impl"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/channel"
	"go.dedis.ch/cs438/transport/udp"
)

var peerFac peer.Factory = impl.NewPeer

var channelFac transport.Factory = channel.NewTransport
var udpFac transport.Factory = udp.NewUDP
