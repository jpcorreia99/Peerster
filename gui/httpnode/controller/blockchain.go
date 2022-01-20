package controller

import (
	"encoding/hex"
	"net/http"
	"text/template"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
)

// NewBlockchain returns a new initialized blockchain.
func NewBlockchain(conf peer.Configuration, log *zerolog.Logger) blockchain {
	return blockchain{
		conf: conf,
		log:  log,
	}
}

type blockchain struct {
	conf peer.Configuration
	log  *zerolog.Logger
}

func (b blockchain) BlockchainHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			b.blockchainGet(w, r)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (b blockchain) blockchainGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	store := b.conf.Storage.GetBlockchainStore()

	lastBlockHashHex := hex.EncodeToString(store.Get(storage.LastBlockKey))
	endBlockHasHex := hex.EncodeToString(make([]byte, 32))

	if lastBlockHashHex == "" {
		lastBlockHashHex = endBlockHasHex
	}

	blocks := []viewBlock{}

	for lastBlockHashHex != endBlockHasHex {
		lastBlockBuf := store.Get(string(lastBlockHashHex))

		var lastBlock types.BlockchainBlock

		err := lastBlock.Unmarshal(lastBlockBuf)
		if err != nil {
			http.Error(w, "failed to unmarshal block: "+err.Error(), http.StatusInternalServerError)
			return
		}

		blocks = append(blocks, viewBlock{
			Index:    lastBlock.Index,
			Hash:     hex.EncodeToString(lastBlock.Hash),
			ValueID:  lastBlock.Value.UniqID,
			Name:     lastBlock.Value.Filename,
			Metahash: lastBlock.Value.Metahash,
			PrevHash: hex.EncodeToString(lastBlock.PrevHash),
		})

		lastBlockHashHex = hex.EncodeToString(lastBlock.PrevHash)
	}

	viewData := struct {
		NodeAddr      string
		LastBlockHash string
		Blocks        []viewBlock
	}{
		NodeAddr:      b.conf.Socket.GetAddress(),
		LastBlockHash: hex.EncodeToString(store.Get(storage.LastBlockKey)),
		Blocks:        blocks,
	}

	tmpl, err := template.New("html").ParseFiles(("httpnode/controller/blockchain.gohtml"))
	if err != nil {
		http.Error(w, "failed to parse template: "+err.Error(), http.StatusInternalServerError)
		return
	}

	tmpl.ExecuteTemplate(w, "blockchain.gohtml", viewData)
}

type viewBlock struct {
	Index    uint
	Hash     string
	ValueID  string
	Name     string
	Metahash string
	PrevHash string
}
