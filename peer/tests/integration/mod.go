package integration

import (
	"fmt"
	"os"
	"runtime"

	"go.dedis.ch/cs438/internal/binnode"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/peer/impl"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/transport/proxy"
	"go.dedis.ch/cs438/transport/udp"
)

var studentFac peer.Factory = impl.NewPeer
var referenceFac peer.Factory

// a bag of supported OS
var supportedOS = map[string]struct{}{
	"darwin": {},
	"linux":  {},
}

// a bag of supported architecture
var supportedArch = map[string]struct{}{
	"amd64": {},
}

func init() {
	path := getPath()
	referenceFac = binnode.GetBinnodeFac(path)
}

// getPath returns the path in the PEER_BIN_PATH variable if set, otherwise a
// path of form ./node.<OS>.<ARCH>. For example "./node.darwin.amd64". It panics
// in case of an unsupported OS/ARCH.
func getPath() string {
	path := os.Getenv("PEER_BIN_PATH")
	if path != "" {
		return path
	}

	_, found := supportedOS[runtime.GOOS]
	if !found {
		panic("unsupported OS: " + runtime.GOOS)
	}

	_, found = supportedArch[runtime.GOARCH]
	if !found {
		panic("unsupported architecture: " + runtime.GOARCH)
	}

	return fmt.Sprintf("./node.%s.%s", runtime.GOOS, runtime.GOARCH)
}

var udpFac transport.Factory = udp.NewUDP
var proxyFac transport.Factory = proxy.NewProxy
