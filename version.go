package main

import (
	"runtime/debug"
	"strings"
)

// Version is the application version, set at build time via linker flags.
// go build -ldflags "-X main.Version=$(git describe --tags --always --dirty)"
var Version = "dev"

// GetVersion returns the application version.
func GetVersion() string {
	return strings.TrimSpace(Version)
}

// GetBuildInfo returns the VCS revision and build time.
func GetBuildInfo() (revision string, buildDate string) {
	revision = "unknown"
	buildDate = "unknown"

	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.time":
				buildDate = setting.Value
			}
		}
	}
	return
}
