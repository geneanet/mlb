package system

import (
	"runtime"
	"syscall"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog/log"
)

// SystemConfig defines the HCL configuration for system-level settings.
type SystemConfig struct {
	RLimit                 *RLimitConfig `hcl:"rlimit,block"`
	GoMaxProcs             int           `hcl:"gomaxprocs,optional"`
	PIDFile                string        `hcl:"pid_file,optional"`
	SystemdNotifyReady     bool          `hcl:"systemd_notify_ready,optional"`
	SystemdNotifyReloading bool          `hcl:"systemd_notify_reloading,optional"`
}

// RLimitConfig defines the HCL configuration for resource limits.
type RLimitConfig struct {
	NOFile uint64 `hcl:"nofile,optional"`
}

// DecodeConfigBlock decodes an HCL block into a SystemConfig.
func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext) (*SystemConfig, hcl.Diagnostics) {
	c := &SystemConfig{
		SystemdNotifyReady:     true,
		SystemdNotifyReloading: true,
	}
	diag := gohcl.DecodeBody(block.Body, ctx, c)
	return c, diag
}

// SetGoMaxProcs sets the GOMAXPROCS value.
func SetGoMaxProcs(gomaxprocs int) {
	log.Debug().Int("value", gomaxprocs).Msg("Setting GOMAXPROCS")
	runtime.GOMAXPROCS(gomaxprocs)
}

// SetRlimitNOFILE sets the maximum number of open file descriptors.
func SetRlimitNOFILE(nofile uint64) {
	var rLimit syscall.Rlimit

	log.Debug().Uint64("value", nofile).Msg("Setting RLIMIT_NOFILE")

	rLimit.Max = nofile
	rLimit.Cur = nofile

	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		panic(err)
	}
}
