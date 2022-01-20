package controller

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/registry"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// NewRegistryCtrl returns a new initialized registry controller.
func NewRegistryCtrl(registry registry.Registry, log *zerolog.Logger) registryctrl {
	return registryctrl{
		registry: registry,
		log:      log,
	}
}

type registryctrl struct {
	registry registry.Registry
	log      *zerolog.Logger
}

func (reg registryctrl) MessagesHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}

		reg.messagesGet(w, r)
	}
}

func (reg registryctrl) PktNotifyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			reg.pktNotifyGet(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			return
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}
	}
}

func (reg registryctrl) messagesGet(w http.ResponseWriter, r *http.Request) {
	messages := reg.registry.GetMessages()

	transpMsgs := make([]transport.Message, len(messages))

	for i, msg := range messages {
		transpMsg, err := reg.registry.MarshalMessage(msg)
		if err != nil {
			http.Error(w, "failed to marshal msg: "+err.Error(), http.StatusInternalServerError)
			return
		}

		transpMsgs[i] = transpMsg
	}

	buf, err := json.Marshal(&transpMsgs)
	if err != nil {
		http.Error(w, "failed to marshal messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buf)
}

// pktNotifyGet creates a SSE connection, where packets are sent as they are
// processed by the registry.
func (reg registryctrl) pktNotifyGet(w http.ResponseWriter, r *http.Request) {
	flusher, _ := w.(http.Flusher)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	pkts := make(chan transport.Packet, 100)

	reg.registry.RegisterNotify(func(msg types.Message, p transport.Packet) error {
		pkts <- p
		return nil
	})

	for {
		select {
		case pkt := <-pkts:
			buf, err := json.Marshal(&pkt)
			if err != nil {
				http.Error(w, "failed to marshal pkt: "+err.Error(), http.StatusInternalServerError)
			}

			fmt.Fprintf(w, "data: %s\n\n", buf)
			flusher.Flush()

		case <-r.Context().Done():
			return
		}
	}
}
