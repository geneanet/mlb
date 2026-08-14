package system

import (
	"net"
	"os"
	"strings"

	"github.com/cloudflare/tableflip"
	"github.com/rs/zerolog/log"
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
// If the address starts with "unix:", it will use the "unix" network and the path that follows.
func Listen(network, address string) (net.Listener, error) {
	if strings.HasPrefix(address, "unix:") {
		network = "unix"
		address = strings.TrimPrefix(address, "unix:")
	}

	if upgrader == nil {
		if network == "unix" {
			if _, err := os.Stat(address); err == nil {
				log.Warn().Str("address", address).Msg("Removing stale unix socket")
				_ = os.Remove(address)
			}
		}
		return net.Listen(network, address)
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
