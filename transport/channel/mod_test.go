package channel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.dedis.ch/cs438/transport"
)

func TestScenario(t *testing.T) {

	net := NewTransport()

	sock1, err := net.CreateSocket("A")
	require.NoError(t, err)

	sock2, err := net.CreateSocket("B")
	require.NoError(t, err)

	sock1.Send("B", transport.Packet{
		Header: &transport.Header{},
		Msg: &transport.Message{
			Type: "msgA1",
		},
	}, 0)

	sock2.Send("A", transport.Packet{
		Header: &transport.Header{},
		Msg: &transport.Message{
			Type: "msgB1",
		},
	}, 0)

	sock1.Send("B", transport.Packet{
		Header: &transport.Header{},
		Msg: &transport.Message{
			Type: "msgA2",
		},
	}, 0)

	sock2.Send("A", transport.Packet{
		Header: &transport.Header{},
		Msg: &transport.Message{
			Type: "msgB2",
		},
	}, 0)

	pkt, err := sock2.Recv(time.Second)
	require.NoError(t, err)

	require.Equal(t, "msgA1", pkt.Msg.Type)

	pkt, err = sock2.Recv(time.Second)
	require.NoError(t, err)

	require.Equal(t, "msgA2", pkt.Msg.Type)

	pkt, err = sock1.Recv(time.Second)
	require.NoError(t, err)

	require.Equal(t, "msgB1", pkt.Msg.Type)

	pkt, err = sock1.Recv(time.Second)
	require.NoError(t, err)

	require.Equal(t, "msgB2", pkt.Msg.Type)
}
