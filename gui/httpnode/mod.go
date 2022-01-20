package httpnode

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/gui/httpnode/controller"
	"go.dedis.ch/cs438/peer"
	"golang.org/x/xerrors"
)

type key int

const (
	requestIDKey key = 0

	// this message is used by the binary node to get the proxy address
	readyMsg = "proxy server is ready to handle requests at '%s'"
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
	if os.Getenv("HTTPLOG") == "warn" {
		defaultLevel = zerolog.WarnLevel
	}

	if os.Getenv("HTTPLOG") == "no" {
		defaultLevel = zerolog.Disabled
	}
}

// Proxy defines the interface for a node proxy
type Proxy interface {
	StartAndListen(proxyAddr string) error
	StopAndClose() error
}

// NewHTTPNode return a proxy http.
func NewHTTPNode(node peer.Peer, conf peer.Configuration) Proxy {
	log := zerolog.New(logout).
		Level(defaultLevel).
		With().Timestamp().Logger().
		With().Caller().Logger().
		With().Str("role", "proxy http").Logger()

	mux := http.NewServeMux()

	messagingctrl := controller.NewMessaging(node, &log)
	socketctrl := controller.NewSocketCtrl(conf.Socket, &log)
	registryctrl := controller.NewRegistryCtrl(conf.MessageRegistry, &log)
	servicectrl := controller.NewServiceCtrl(node, &log)
	datasharingctrl := controller.NewDataSharing(node, &log)
	blockchain := controller.NewBlockchain(conf, &log)

	mux.Handle("/messaging/peers", http.HandlerFunc(messagingctrl.PeerHandler()))
	mux.Handle("/messaging/routing", http.HandlerFunc(messagingctrl.RoutingHandler()))
	mux.Handle("/messaging/unicast", http.HandlerFunc(messagingctrl.UnicastHandler()))
	mux.Handle("/messaging/broadcast", http.HandlerFunc(messagingctrl.BroadcastHandler()))

	mux.Handle("/socket/ins", http.HandlerFunc(socketctrl.InsHandler()))
	mux.Handle("/socket/outs", http.HandlerFunc(socketctrl.OutsHandler()))
	mux.Handle("/socket/address", http.HandlerFunc(socketctrl.AddressHandler()))

	// get all messages processed so far by the message registry.
	mux.Handle("/registry/messages", http.HandlerFunc(registryctrl.MessagesHandler()))
	// shouldn't be used in tests, as SSE can be flaky to use.
	mux.Handle("/registry/pktnotify", http.HandlerFunc(registryctrl.PktNotifyHandler()))

	mux.Handle("/service/stop", http.HandlerFunc(servicectrl.ServiceStopHandler()))

	mux.Handle("/datasharing/upload", http.HandlerFunc(datasharingctrl.UploadHandler()))
	mux.Handle("/datasharing/download", http.HandlerFunc(datasharingctrl.DownloadHandler()))
	// Tag() + Resolve()
	mux.Handle("/datasharing/naming", http.HandlerFunc(datasharingctrl.NamingHandler()))
	mux.Handle("/datasharing/catalog", http.HandlerFunc(datasharingctrl.CatalogHandler()))
	mux.Handle("/datasharing/searchAll", http.HandlerFunc(datasharingctrl.SearchAllHandler()))
	mux.Handle("/datasharing/searchFirst", http.HandlerFunc(datasharingctrl.SearchFirstHandler()))

	mux.Handle("/blockchain", http.HandlerFunc(blockchain.BlockchainHandler()))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not authorized", http.StatusBadGateway)
		log.Error().Msgf("wrong endpoint: %s", r.URL.Path)
	})

	return &httpnode{
		Peer: node,
		conf: conf,
		log:  &log,
		mux:  mux,
		quit: make(chan struct{}),
	}
}

type httpnode struct {
	peer.Peer
	conf   peer.Configuration
	log    *zerolog.Logger
	server *http.Server
	quit   chan struct{}
	ln     net.Listener
	mux    *http.ServeMux
}

// StartAndListen implements Proxy. It will start the node and the http server
// that interfaces with that node.
func (h *httpnode) StartAndListen(proxyAddr string) error {
	err := h.Peer.Start()
	if err != nil {
		return xerrors.Errorf("failed to start node: %v", err)
	}

	peerAddr := h.conf.Socket.GetAddress()
	newLog := h.log.With().Str("peerAddr", peerAddr).Logger()
	h.log = &newLog

	go h.listen(proxyAddr)

	return nil
}

// StopAndClose implements Proxy.
func (h *httpnode) StopAndClose() error {
	h.stop()

	h.log.Info().Msg("proxy stopped")

	err := h.Peer.Stop()
	if err != nil {
		return xerrors.Errorf("failed to close stop node: %v", err)
	}

	h.log.Info().Msg("node stopped")

	return nil
}

// listen start the proxy http server.
func (h *httpnode) listen(proxyAddr string) {
	h.log.Info().Msg("proxy server is starting...")

	done := make(chan struct{})

	ln, err := net.Listen("tcp", proxyAddr)
	if err != nil {
		h.log.Panic().Msgf("failed to create conn '%s': %v", proxyAddr, err)
		return
	}

	addr := ln.Addr().String()

	h.ln = ln

	// this message is used by the binary node to get the proxy address
	h.log.Info().Msgf(readyMsg, addr)

	proxyPath := filepath.Join(os.TempDir(), fmt.Sprintf("proxyaddress_%d", os.Getpid()))

	err = ioutil.WriteFile(proxyPath, []byte(addr), os.ModePerm)
	if err != nil {
		h.log.Panic().Msgf("failed to write proxy address: %v", err)
		return
	}

	newLog := h.log.With().Str("proxyAddr", addr).Logger()
	h.log = &newLog

	nextRequestID := func() string {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}

	h.server = &http.Server{
		Addr:    proxyAddr,
		Handler: tracing(nextRequestID)(logging(h.log)(h.mux)),
	}

	go func() {
		<-h.quit
		h.log.Info().Msg("proxy server is shutting down...")

		os.RemoveAll(proxyPath)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		h.server.SetKeepAlivesEnabled(false)
		err := h.server.Shutdown(ctx)
		if err != nil {
			h.log.Fatal().Msgf("Could not gracefully shutdown the server: %v", err)
		}
		close(done)
	}()

	err = h.server.Serve(ln)
	if err != nil && err != http.ErrServerClosed {
		h.log.Fatal().Msgf("could not listen on %s: %v", proxyAddr, err)
	}

	<-done
	h.log.Info().Msg("server stopped")
}

func (h *httpnode) stop() {
	// we don't close it so it can be called multiple times without harm
	h.quit <- struct{}{}
}

// logging is a utility function that logs the http server events
func logging(logger *zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				requestID, ok := r.Context().Value(requestIDKey).(string)
				if !ok {
					requestID = "unknown"
				}
				logger.Info().Str("requestID", requestID).
					Str("method", r.Method).
					Str("url", r.URL.Path).
					Str("remoteAddr", r.RemoteAddr).
					Str("agent", r.UserAgent()).Msg("")
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// tracing is a utility function that adds header tracing
func tracing(nextRequestID func() string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := r.Header.Get("X-Request-Id")
			if requestID == "" {
				requestID = nextRequestID()
			}
			ctx := context.WithValue(r.Context(), requestIDKey, requestID)
			w.Header().Set("X-Request-Id", requestID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
