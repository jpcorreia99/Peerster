package binnode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	rproxy "go.dedis.ch/cs438/registry/proxy"
	tproxy "go.dedis.ch/cs438/transport/proxy"
	"golang.org/x/xerrors"
)

var (
	// defaultLevel can be changed to set the desired level of the logger
	defaultLevel = zerolog.InfoLevel

	// logout is the logger configuration
	logout = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
)

func init() {
	if os.Getenv("BINLOG") == "warn" {
		defaultLevel = zerolog.WarnLevel
	}

	if os.Getenv("BINLOG") == "no" {
		defaultLevel = zerolog.Disabled
	}
}

// GetBinnodeFac returns a binnode factory, which needs the binary path. Due to
// its particular nature, a bin node has some restrictions. This is so because
// we create the gossiper instance via CLI, which restricts the parameters we
// can provide.
// - The socket needs to be a proxy socket. Its address is updated once we know
//   the "real" socket address. GetIns/GetOuts work over the http API.
// - The message registry needs to by a proxy registry. Only GetMessages can be
//   used.
func GetBinnodeFac(binaryPath string) peer.Factory {
	return func(conf peer.Configuration) peer.Peer {
		socket, ok := conf.Socket.(*tproxy.Socket)
		if !ok {
			panic("binnode must have a proxy socket")
		}

		registry, ok := conf.MessageRegistry.(*rproxy.Registry)
		if !ok {
			panic("binnode must have a proxy registry")
		}

		return &binnode{
			conf: conf,
			log: zerolog.New(logout).
				Level(defaultLevel).
				With().Timestamp().Logger().
				With().Caller().Logger().
				With().Str("role", "bin node").Logger(),
			stop:       make(chan struct{}),
			stopped:    make(chan error),
			binaryPath: binaryPath,
			socket:     socket,
			registry:   registry,
		}
	}
}

// binnode defines a peer.Peer that uses the binary and http proxy. This
// node will use its own UDP socket and take care of creating/stopping it.
// Address used is the one found in conf.Socket.GetAddress().
type binnode struct {
	peer.Peer

	conf peer.Configuration
	log  zerolog.Logger

	stop    chan struct{}
	stopped chan error

	// set when Start() is called
	proxyAddr string

	binaryPath string

	// on those we need to set the proxy address once we know it
	socket   *tproxy.Socket
	registry *rproxy.Registry
}

// Start implements peer.Peer. It start the http proxy, which, in turn,
// will start the node.
func (b *binnode) Start() error {
	b.log.Info().Msgf("starting with addr %s", b.conf.Socket.GetAddress())

	args := []string{
		"start",
		"--proxyaddr", "127.0.0.1:0",
		"--nodeaddr", b.conf.Socket.GetAddress(),
		"--antientropy", b.conf.AntiEntropyInterval.String(),
		"--heartbeat", b.conf.HeartbeatInterval.String(),
		"--acktimeout", b.conf.AckTimeout.String(),
		"--totalpeers", strconv.Itoa(int(b.conf.TotalPeers)),
		"--paxosid", strconv.Itoa(int(b.conf.PaxosID)),
		"--paxosproposerretry", b.conf.PaxosProposerRetry.String(),
	}

	// if this is a storage that uses the filesystem then we want to use it
	fileStorage, ok := b.conf.Storage.(fileStorage)
	if ok {
		args = append(args, "--storagefolder", fileStorage.GetFolderPath())
	}

	cmd := exec.Command(b.binaryPath, args...)

	// okOutput, writer := io.Pipe()
	// okWriter := io.MultiWriter(os.Stdout, writer)

	var errOutput bytes.Buffer
	errWriter := io.MultiWriter(os.Stderr, &errOutput)

	cmd.Stderr = errWriter
	// b.cmd.Stdout = okWriter
	cmd.Stdout = os.Stdout

	// start the node proxy, which will also start the node
	err := cmd.Start()
	if err != nil {
		wd, _ := os.Getwd()
		return xerrors.Errorf("failed to run node (from %s): %v", wd, err)
	}

	time.Sleep(time.Second)

	// stop the proxy when the user stops
	go func() {
		defer func() {
			b.log.Info().Msg("binary stopped")
			close(b.stopped)
		}()

		<-b.stop
		err := cmd.Process.Signal(os.Interrupt)
		if err != nil {
			b.log.Err(err).Msg("failed to stop process")
			err = cmd.Process.Kill()
			if err != nil {
				b.log.Err(err).Msg("failed to kill")
				b.stopped <- err
				return
			}
		}

		_, err = cmd.Process.Wait()
		if err != nil {
			b.log.Err(err).Msg("failed to wait for stop process")
			b.stopped <- err
			return
		}
	}()

	proxyPath := filepath.Join(os.TempDir(), fmt.Sprintf("proxyaddress_%d", cmd.Process.Pid))

	maxRetry := 10
	waitRetry := time.Millisecond * 500

	var proxyAddrBuf []byte

	for i := 1; ; i++ {
		proxyAddrBuf, err = ioutil.ReadFile(proxyPath)
		if (err != nil || len(proxyAddrBuf) == 0) && i == maxRetry {
			b.Terminate()
			return xerrors.Errorf("failed to get proxy address: %v", err)
		}

		if err == nil && len(proxyAddrBuf) != 0 {
			break
		}

		b.log.Info().Msgf("waiting %s before retrying", waitRetry.String())
		time.Sleep(waitRetry)
	}

	os.Remove(proxyPath)

	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("socketaddress_%d", cmd.Process.Pid))

	var socketAddrBuf []byte

	for i := 1; ; i++ {
		socketAddrBuf, err = ioutil.ReadFile(socketPath)
		if (err != nil || len(socketAddrBuf) == 0) && i == maxRetry {
			b.Terminate()
			return xerrors.Errorf("failed to get socket address: %v", err)
		}

		if err == nil && len(socketAddrBuf) != 0 {
			break
		}

		b.log.Info().Msgf("waiting %s before retrying", waitRetry.String())
		time.Sleep(waitRetry)
	}

	os.Remove(socketPath)

	// polls the okWriter to have the addresses
	// proxyAddr, socketAddr, err = pollAddrs(okOutput)
	// if err != nil {
	// 	panic("failed to get addresses: " + err.Error())
	// }

	b.proxyAddr = string(proxyAddrBuf)

	b.socket.SetProxyAddress(string(proxyAddrBuf))
	b.registry.SetProxyAddress(string(proxyAddrBuf))

	b.log.Info().Msgf("setting socket address: %s", string(socketAddrBuf))
	b.socket.SetSocketAddress(string(socketAddrBuf))

	// final check if there is anything on stderr

	out, err := ioutil.ReadAll(&errOutput)
	if err != nil {
		b.Terminate()
		return xerrors.Errorf("failed to read output error: %v", err)
	}

	if len(out) != 0 {
		b.Terminate()
		return xerrors.Errorf("failed to start bin: %s", out)
	}

	b.log.Info().Msg("binary successfully started")

	return nil
}

// Stop implement peer.Service. It stops the peer.
func (b binnode) Stop() error {
	endpoint := "http://" + b.proxyAddr + "/service/stop"

	data := []byte{}

	_, err := b.postData(endpoint, data)
	if err != nil {
		return xerrors.Errorf("failed to post data: %v", err)
	}

	return nil
}

// Terminate implements testing.Terminable. It kills the process running the
// http node.
func (b binnode) Terminate() error {
	close(b.stop)

	err := <-b.stopped
	if err != nil {
		return xerrors.Errorf("failed to stop: %v", err)
	}

	return nil
}

// postData returns an error in case the endpoint is supposed to. Otherwise it
// panics if the error doesn't concern the peer, but like http stuff.
func (b binnode) postData(endpoint string, v interface{}) ([]byte, error) {
	buf, err := json.Marshal(&v)
	if err != nil {
		b.log.Fatal().Msgf("failed to encode data: %v", err)
	}

	resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(buf))
	if err != nil {
		b.log.Fatal().Msgf("failed to talk to http node: %v", err)
	}

	defer resp.Body.Close()
	content, err := ioutil.ReadAll(resp.Body)

	// a "normal" error from the peer
	if resp.StatusCode == http.StatusBadRequest {
		if err != nil {
			b.log.Fatal().Msgf("failed to read response: %v", err)
		}

		return nil, xerrors.New(string(content))
	}

	if resp.StatusCode != http.StatusOK {
		b.log.Fatal().Msgf("bad status: %v", resp.Status)
	}

	return content, nil
}

type fileStorage interface {
	GetFolderPath() string
}
