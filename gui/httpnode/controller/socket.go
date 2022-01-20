package controller

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/transport"
)

// NewSocketCtrl returns a new initialized socket controller.
func NewSocketCtrl(socket transport.Socket, log *zerolog.Logger) socketctrl {
	return socketctrl{
		socket: socket,
		log:    log,
	}
}

type socketctrl struct {
	socket transport.Socket
	log    *zerolog.Logger
}

func (s socketctrl) InsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}

		s.insGet(w, r)
	}
}

func (s socketctrl) OutsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}

		s.outsGet(w, r)
	}
}

func (s socketctrl) AddressHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
			return
		}

		s.addressGet(w, r)
	}
}

func (s socketctrl) insGet(w http.ResponseWriter, r *http.Request) {
	ins := s.socket.GetIns()

	buf, err := json.Marshal(&ins)
	if err != nil {
		http.Error(w, "failed to marshal ins: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buf)
}

func (s socketctrl) outsGet(w http.ResponseWriter, r *http.Request) {
	outs := s.socket.GetOuts()

	buf, err := json.Marshal(&outs)
	if err != nil {
		http.Error(w, "failed to marshal outs: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write(buf)
}

func (s socketctrl) addressGet(w http.ResponseWriter, r *http.Request) {
	addr := s.socket.GetAddress()

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	w.Write([]byte(addr))
}
