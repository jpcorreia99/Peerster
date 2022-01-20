package controller

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/gui/httpnode/types"
	"go.dedis.ch/cs438/peer"
)

// NewDataSharing returns a new initialized datasharing.
func NewDataSharing(node peer.Peer, log *zerolog.Logger) datasharing {
	return datasharing{
		node: node,
		log:  log,
	}
}

type datasharing struct {
	node peer.Peer
	log  *zerolog.Logger
}

func (d datasharing) UploadHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			d.uploadPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (d datasharing) DownloadHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			d.downloadGet(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (d datasharing) NamingHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			d.namingPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		case http.MethodGet:
			d.namingGet(w, r)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (d datasharing) CatalogHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			d.catalogPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		case http.MethodGet:
			d.catalogGet(w, r)
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (d datasharing) SearchAllHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			d.indexPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (d datasharing) SearchFirstHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			d.searchPost(w, r)
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "*")
		default:
			http.Error(w, "forbidden method", http.StatusMethodNotAllowed)
		}
	}
}

func (d datasharing) uploadPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	mh, err := d.node.Upload(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to upload: %v", err), http.StatusBadRequest)
		return
	}

	w.Write([]byte(mh))
}

// Get key=key
func (d datasharing) downloadGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "'key' argument not found or empty", http.StatusBadRequest)
		return
	}

	res, err := d.node.Download(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to download: %v", err),
			http.StatusBadRequest)
		return
	}

	w.Write(res)
}

// Peer.Tag()
// JSON: ["name", "metahash"]
func (d datasharing) namingPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err),
			http.StatusInternalServerError)
		return
	}

	arguments := [2]string{} // [name, metahash]
	err = json.Unmarshal(buf, &arguments)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to unmarshal arguments: %v", err),
			http.StatusInternalServerError)
		return
	}

	err = d.node.Tag(arguments[0], arguments[1])
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to tag: %v", err), http.StatusBadRequest)
		return
	}
}

// Peer.Resolve()
// Get name=name
func (d datasharing) namingGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "'name' argument not found or empty", http.StatusBadRequest)
		return
	}

	mh := d.node.Resolve(name)
	w.Write([]byte(mh))
}

// JSON ["key", "value"]
func (d datasharing) catalogPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err),
			http.StatusInternalServerError)
		return
	}

	arguments := [2]string{} // [key, value]
	err = json.Unmarshal(buf, &arguments)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to unmarshal arguments: %v", err),
			http.StatusInternalServerError)
		return
	}

	d.node.UpdateCatalog(arguments[0], arguments[1])
}

func (d datasharing) catalogGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	catalog := d.node.GetCatalog()

	js, err := json.MarshalIndent(&catalog, "", "\t")
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal catalog: %v", err),
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	w.Write(js)
}

// types.IndexArgument:
// {
//   "Pattern": ".*",
//   "Budget": 2,
//   "Timeout": "2s"
// }
func (d datasharing) indexPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err),
			http.StatusInternalServerError)
		return
	}

	arguments := types.IndexArgument{}
	err = json.Unmarshal(buf, &arguments)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to unmarshal arguments: %v", err),
			http.StatusInternalServerError)
		return
	}

	regex, err := regexp.Compile(arguments.Pattern)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse regex: %v", err),
			http.StatusBadRequest)
		return
	}

	wait, err := time.ParseDuration(arguments.Timeout)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse wait: %v", err),
			http.StatusBadRequest)
		return
	}

	names, err := d.node.SearchAll(*regex, arguments.Budget, wait)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to index: %v", err), http.StatusBadRequest)
		return
	}

	js, err := json.Marshal(&names)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal names: %v", err),
			http.StatusInternalServerError)
		return
	}

	w.Write(js)
}

// types.SearchArgument:
// {
//   "Pattern": ".*",
//   "Initial": 2,
//   "Factor": 2,
//   "Retry": 2,
//   "Timeout": "2s"
// }
func (d datasharing) searchPost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err),
			http.StatusInternalServerError)
		return
	}

	arguments := types.SearchArgument{}
	err = json.Unmarshal(buf, &arguments)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to unmarshal arguments: %v", err),
			http.StatusInternalServerError)
		return
	}

	regex, err := regexp.Compile(arguments.Pattern)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse regex: %v", err),
			http.StatusBadRequest)
		return
	}

	timeout, err := time.ParseDuration(arguments.Timeout)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse wait: %v", err),
			http.StatusBadRequest)
		return
	}

	name, err := d.node.SearchFirst(*regex, peer.ExpandingRing{
		Initial: arguments.Initial,
		Factor:  arguments.Factor,
		Retry:   arguments.Factor,
		Timeout: timeout,
	})

	if err != nil {
		http.Error(w, fmt.Sprintf("failed to search: %v", err), http.StatusBadRequest)
		return
	}

	w.Write([]byte(name))
}
