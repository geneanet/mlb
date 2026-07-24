package main

import (
	"strings"
)

// Version is the application version, set at build time via linker flags.
// go build -ldflags "-X main.Version=$(git describe --tags --always --dirty)"
var Version = "dev"

// GetVersion returns the application version.
func GetVersion() string {
	return strings.TrimSpace(Version)
}
