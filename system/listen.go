package system

import (
	"net"

	"github.com/cloudflare/tableflip"
)

var upgrader *tableflip.Upgrader

// InitTableflip initializes the global tableflip upgrader.
func InitTableflip(pidFile string) (*tableflip.Upgrader, error) {
	var err error
	upgrader, err = tableflip.New(tableflip.Options{
		PIDFile: pidFile,
	})
	return upgrader, err
}

// Listen returns a net.Listener that is managed by tableflip.
// It will reuse an inherited file descriptor if one exists for the given address.
// If the tableflip upgrader is not initialized (e.g., in tests), it falls back to net.Listen.
func Listen(network, address string) (net.Listener, error) {
	if upgrader == nil {
		return net.Listen(network, address)
	}
	return upgrader.Fds.Listen(network, address)
}
