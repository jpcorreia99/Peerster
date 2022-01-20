package binnode

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"
	"time"

	"go.dedis.ch/cs438/gui/httpnode/types"
	"go.dedis.ch/cs438/peer"
	"golang.org/x/xerrors"
)

// Upload implements peer.DataSharing
func (b binnode) Upload(data io.Reader) (metahash string, err error) {
	endpoint := "http://" + b.proxyAddr + "/datasharing/upload"

	resp, err := http.Post(endpoint, "application/octet-stream", data)
	if err != nil {
		b.log.Fatal().Msgf("failed to post data: %v", err)
	}

	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		b.log.Fatal().Msgf("failed to read response: %v", err)
	}

	// a "normal" error from the peer
	if resp.StatusCode == http.StatusBadRequest {
		return "", xerrors.New(string(content))
	}

	if resp.StatusCode != http.StatusOK {
		b.log.Fatal().Msgf("bad status: %v", resp.Status)
	}

	return string(content), nil
}

// Download implements peer.DataSharing
func (b binnode) Download(metahash string) ([]byte, error) {
	endpoint := "http://" + b.proxyAddr + "/datasharing/download?key=" + metahash

	resp, err := http.Get(endpoint)
	if err != nil {
		b.log.Fatal().Msgf("failed to get: %v", err)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		b.log.Fatal().Msgf("failed to read body: %v", err)
	}

	// a "normal" error from the peer
	if resp.StatusCode == http.StatusBadRequest {
		return nil, xerrors.New(string(content))
	}

	if resp.StatusCode != http.StatusOK {
		b.log.Fatal().Msgf("bad status: %v", resp.Status)
	}

	return content, nil
}

// Tag implements peer.DataSharing
func (b binnode) Tag(name string, mh string) error {
	endpoint := "http://" + b.proxyAddr + "/datasharing/naming"

	data := [2]string{name, mh}

	_, err := b.postData(endpoint, data)
	if err != nil {
		return xerrors.Errorf("failed to post upload: %v", err)
	}

	return nil
}

// Resolve implements peer.DataSharing
func (b binnode) Resolve(name string) (metahash string) {
	endpoint := "http://" + b.proxyAddr + "/datasharing/naming?name=" + name

	resp, err := http.Get(endpoint)
	if err != nil {
		b.log.Fatal().Msgf("failed to get: %v", err)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		b.log.Fatal().Msgf("failed to read body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		b.log.Fatal().Msgf("bad status: %v", resp.Status)
	}

	return string(content)
}

// GetCatalog implements peer.DataSharing
func (b binnode) GetCatalog() peer.Catalog {
	endpoint := "http://" + b.proxyAddr + "/datasharing/catalog"

	resp, err := http.Get(endpoint)
	if err != nil {
		b.log.Fatal().Msgf("failed to get: %v", err)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		b.log.Fatal().Msgf("failed to read body: %v", err)
	}

	data := peer.Catalog{}

	err = json.Unmarshal(content, &data)
	if err != nil {
		b.log.Fatal().Msgf("failed to unmarshal: %v", err)
	}

	return data
}

// UpdateCatalog implements peer.DataSharing
func (b binnode) UpdateCatalog(key string, peer string) {
	endpoint := "http://" + b.proxyAddr + "/datasharing/catalog"

	data := [2]string{key, peer}

	b.postData(endpoint, data)
}

// SearchAll implements peer.DataSharing
func (b binnode) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) (names []string, err error) {
	endpoint := "http://" + b.proxyAddr + "/datasharing/searchAll"

	data := types.IndexArgument{
		Pattern: reg.String(),
		Budget:  budget,
		Timeout: timeout.String(),
	}

	content, err := b.postData(endpoint, data)
	if err != nil {
		return nil, xerrors.Errorf("failed to post search: %v", err)
	}

	result := []string{}

	err = json.Unmarshal(content, &result)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal result: %v", err)
	}

	return result, nil
}

// Search implements peer.DataSharing
func (b binnode) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (name string, err error) {
	endpoint := "http://" + b.proxyAddr + "/datasharing/searchFirst"

	data := types.SearchArgument{
		Pattern: pattern.String(),
		Initial: conf.Initial,
		Factor:  conf.Factor,
		Retry:   conf.Retry,
		Timeout: conf.Timeout.String(),
	}

	content, err := b.postData(endpoint, data)
	if err != nil {
		return "", xerrors.Errorf("failed to post search: %v", err)
	}

	return string(content), nil
}
