package peer

// Service defines the functions for the basic operations of a peer.
type Service interface {
	// Start starts the node. It should, among other things, start listening on
	// its address using the socket.
	//
	// - implemented in HW0
	Start() error

	// Stop stops the node. This function must block until all goroutines are
	// done.
	//
	// - implemented in HW0
	Stop() error
}
