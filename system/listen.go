package system

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/cloudflare/tableflip"
	"github.com/rs/zerolog/log"
)

// InitTableflip initializes a tableflip upgrader.
func InitTableflip(pidFile string) (*tableflip.Upgrader, error) {
	return tableflip.New(tableflip.Options{
		PIDFile: pidFile,
	})
}

// Listener opens a managed listener.
type Listener interface {
	Listen(network, address string) (net.Listener, error)
}

// Listen returns a net.Listener that is managed by tableflip.
// It will reuse an inherited file descriptor if one exists for the given address.
// If the address starts with "unix:", it will use the "unix" network and the path that follows.
func Listen(upgrader Listener, network, address string) (net.Listener, error) {
	if strings.HasPrefix(address, "unix:") {
		network = "unix"
		address = strings.TrimPrefix(address, "unix:")
	}

	if upgrader == nil {
		return nil, fmt.Errorf("listener manager is not initialized")
	}

	l, err := upgrader.Listen(network, address)
	if err != nil && network == "unix" {
		// If it's a unix socket and it failed, it might be a stale socket file.
		// Since Listen would have returned an inherited FD if it had one,
		// failing here means it's not inherited and net.Listen failed.
		if _, err := os.Stat(address); err == nil {
			log.Warn().Str("address", address).Msg("Removing stale unix socket")
			_ = os.Remove(address)
		}
		return upgrader.Listen(network, address)
	}

	return l, err
}
