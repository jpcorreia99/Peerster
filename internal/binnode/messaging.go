package binnode

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"go.dedis.ch/cs438/gui/httpnode/types"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
)

// Unicast implements peer.Messaging
func (b binnode) Unicast(dest string, msg transport.Message) error {
	endpoint := "http://" + b.proxyAddr + "/messaging/unicast"

	data := types.UnicastArgument{
		Dest: dest,
		Msg:  msg,
	}

	_, err := b.postData(endpoint, data)
	if err != nil {
		return xerrors.Errorf("failed to post data: %v", err)
	}

	return nil
}

// Broadcast implements peer.Messaging
func (b binnode) Broadcast(msg transport.Message) error {
	endpoint := "http://" + b.proxyAddr + "/messaging/broadcast"

	_, err := b.postData(endpoint, msg)
	if err != nil {
		return xerrors.Errorf("failed to post data: %v", err)
	}

	return nil
}

// AddPeer implements peer.Messaging
func (b binnode) AddPeer(addr ...string) {
	endpoint := "http://" + b.proxyAddr + "/messaging/peers"

	data := append(types.AddPeerArgument{}, addr...)
	b.postData(endpoint, data)
}

// GetRoutingTable implements peer.Messaging
func (b binnode) GetRoutingTable() peer.RoutingTable {
	endpoint := "http://" + b.proxyAddr + "/messaging/routing"

	resp, err := http.Get(endpoint)
	if err != nil {
		b.log.Fatal().Msgf("failed to get: %v", err)
	}

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		b.log.Fatal().Msgf("failed to read body: %v", err)
	}

	data := peer.RoutingTable{}

	err = json.Unmarshal(content, &data)
	if err != nil {
		b.log.Fatal().Msgf("failed to unmarshal: %v", err)
	}

	return data
}

// SetRoutingEntry implements peer.Messaging
func (b binnode) SetRoutingEntry(origin, relayAddr string) {
	endpoint := "http://" + b.proxyAddr + "/messaging/routing"

	data := types.SetRoutingEntryArgument{
		Origin:    origin,
		RelayAddr: relayAddr,
	}

	b.postData(endpoint, data)
}
