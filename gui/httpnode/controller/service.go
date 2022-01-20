package controller

import (
	"net/http"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
)

// NewServiceCtrl returns a new initialized service controller.
func NewServiceCtrl(peer peer.Peer, log *zerolog.Logger) servicectrl {
	return servicectrl{
		peer: peer,
		log:  log,
	}
}

type servicectrl struct {
	peer peer.Peer
	log  *zerolog.Logger
}

func (s servicectrl) ServiceStopHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			s.stopPost(w, r)
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

func (s servicectrl) stopPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")

	err := s.peer.Stop()
	if err != nil {
		http.Error(w, "failed to stop: "+err.Error(), http.StatusBadRequest)
		return
	}
}
