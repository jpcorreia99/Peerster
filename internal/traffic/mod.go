// Package traffic records the packets sent and received on a socket and
// generates the corresponding graph. In the test, add the following:
//   defer traffic.SaveGraph("graph.dot", true, false)
package traffic

import (
	"math"
	"os"
	"sync"

	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
)

var (
	globalCounter = atomicCounter{}
	sendCounter   = &atomicCounter{}
	recvCounter   = &atomicCounter{}

	traffics = trafficSlice{}
)

const (
	rcv  = "received"
	sent = "sent"
)

// SaveGraph saves a graphviz representation.
func SaveGraph(path string, withSend, withRcv bool) error {
	f, err := os.Create(path)
	if err != nil {
		return xerrors.Errorf("file: %v", err)
	}

	GenerateItemsGraphviz(f, withSend, withRcv, traffics...)
	return nil
}

// NewTraffic returns a new initialized traffic.
func NewTraffic() *Traffic {
	t := &Traffic{
		items: make([]item, 0),
	}

	traffics = append(traffics, t)

	return t
}

// Traffic defines the struct to store traffic info,
type Traffic struct {
	sync.Mutex

	items []item
}

// LogRecv logs on the traffic that a message has been received.
func (t *Traffic) LogRecv(from, to string, pkt transport.Packet) {
	t.addItem(rcv, from, to, pkt, recvCounter.IncrementAndGet())
}

// LogSent logs on the traffic that a message has been sent.
func (t *Traffic) LogSent(from, to string, pkt transport.Packet) {
	t.addItem(sent, from, to, pkt, sendCounter.IncrementAndGet())
}

func (t *Traffic) addItem(typeStr, from, to string, pkt transport.Packet, counter int) {
	t.Lock()
	defer t.Unlock()

	t.items = append(t.items, item{
		from:          from,
		to:            to,
		typeStr:       typeStr,
		pkt:           pkt,
		typeCounter:   counter,
		globalCounter: globalCounter.IncrementAndGet(),
	})
}

func (t *Traffic) getFirstCounter() int {
	if len(t.items) > 0 {
		return t.items[0].globalCounter
	}

	return math.MaxInt64
}

type item struct {
	from          string
	to            string
	typeStr       string
	pkt           transport.Packet
	globalCounter int
	typeCounter   int
}

// This counter only makes sense when all the nodes are running locally. It is
// useful to analyse the traffic in a developping/test environment, when packets
// order makes sense.
type atomicCounter struct {
	sync.Mutex
	c int
}

func (c *atomicCounter) IncrementAndGet() int {
	c.Lock()
	defer c.Unlock()
	c.c++
	return c.c
}

type trafficSlice []*Traffic

func (tt trafficSlice) Len() int {
	return len(tt)
}

func (tt trafficSlice) Less(i, j int) bool {
	return tt[i].getFirstCounter() < tt[j].getFirstCounter()
}

func (tt trafficSlice) Swap(i, j int) {
	tt[i], tt[j] = tt[j], tt[i]
}
