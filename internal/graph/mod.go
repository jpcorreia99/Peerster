package graph

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	z "go.dedis.ch/cs438/internal/testing"
)

// NewGraph returns a new initialized graph
func NewGraph(p float64) Graph {
	return Graph{
		p: p,
	}
}

// Graph is a utility to generate a random network topology. It uses a naive
// expansion algorithm to generate a pseudo-random graph with no orphan.
type Graph struct {
	p float64
}

// Generate randomly adds peers to nodes. It makes sure the graph is connected
// without orphans.
func (g Graph) Generate(out io.Writer, peers []z.TestNode) {

	fmt.Fprintf(out, "digraph network_topology {\n")

	fmt.Fprintf(out, "labelloc=\"t\";")
	fmt.Fprintf(out, "label = <Network Diagram of %d nodes <font point-size='10'><br/>(generated %s)</font>>;\n\n", len(peers), time.Now().Format("2 Jan 06 - 15:04:05"))
	fmt.Fprintf(out, "graph [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "graph [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "node [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "edge [fontname = \"helvetica\"];\n\n")

	addrToPeer := make(map[string]z.TestNode)
	for _, peer := range peers {
		addrToPeer[peer.GetAddr()] = peer
	}

	peersNeighboors := make(map[string][]string)

	for i := 1; i < len(peers); i++ {

		connected := false
		for !connected {
			for j := 0; j < i; j++ {
				if rand.Float64() >= g.p {
					continue
				}

				connected = true

				inverse := false
				if rand.Float64() > 0.5 {
					inverse = true
				}

				if inverse {
					// +1 is because the node address often start by port 1.
					fmt.Fprintf(out, "\"%d\" -> \"%d\";\n", j+1, i+1)
					peersNeighboors[peers[j].GetAddr()] = append(peersNeighboors[peers[j].GetAddr()], peers[i].GetAddr())
				} else {
					fmt.Fprintf(out, "\"%d\" -> \"%d\";\n", i+1, j+1)
					peersNeighboors[peers[i].GetAddr()] = append(peersNeighboors[peers[i].GetAddr()], peers[j].GetAddr())
				}
			}
		}
	}

	for k, v := range peersNeighboors {
		addrToPeer[k].AddPeer(v...)
	}

	fmt.Fprintf(out, "}\n")
}
