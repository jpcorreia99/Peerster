package traffic

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"go.dedis.ch/cs438/registry"
)

// GenerateItemsGraphviz creates a graphviz representation of the items. One can
// generate a graphical representation with `dot -Tpdf graph.dot -o graph.pdf`
func GenerateItemsGraphviz(out io.Writer, withSend, withRcv bool, traffics ...*Traffic) {

	sort.Sort(trafficSlice(traffics))

	fmt.Fprintf(out, "digraph network_activity {\n")
	fmt.Fprintf(out, "labelloc=\"t\";")
	fmt.Fprintf(out, "label = <Network Diagram of %d nodes <font point-size='10'><br/>(generated %s)</font>>;\n\n", len(traffics), time.Now().Format("2 Jan 06 - 15:04:05"))
	fmt.Fprintf(out, "graph [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "graph [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "node [fontname = \"helvetica\"];\n")
	fmt.Fprintf(out, "edge [fontname = \"helvetica\"];\n\n")

	for _, traffic := range traffics {
		for _, item := range traffic.items {

			if !withSend && item.typeStr == sent {
				continue
			}
			if !withRcv && item.typeStr == rcv {
				continue
			}

			color := "#4AB2FF"

			if item.typeStr == rcv {
				color = "#A8A8A8"
			}

			var msgStr, pktStr string

			msg, err := registry.GlobalRegistry.GetMessage(item.pkt.Msg)
			if err != nil {
				pktStr = "error in getting message:" + err.Error()
			} else {
				pktStr = msg.HTML()
			}

			headerStr := item.pkt.Header.HTML()
			headerStr = strings.ReplaceAll(headerStr, "\n", "<br/>")

			msgStr = fmt.Sprintf("<font point-size='4'><br/></font><font point-size='11' color='#777777'>%s</font><font point-size='4'><br/><br/></font><font point-size='10' color='#aaaaaa'>%s</font>",
				pktStr, headerStr)

			fmt.Fprintf(out, "\"%v\" -> \"%v\" "+
				"[ label = < <font color='#303030'><b>%d</b> <font point-size='10'>(%d)</font> - %s</font><br/>%s> color=\"%s\" ];\n",
				item.from, item.to, item.typeCounter, item.globalCounter, item.pkt.Msg.Type, msgStr, color)
		}
	}
	fmt.Fprintf(out, "}\n")
}
