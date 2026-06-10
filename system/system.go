package system

import (
	"mlb/misc"
	"runtime"
	"syscall"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog/log"
)

type SystemConfig struct {
	RLimit     *RLimitConfig `hcl:"rlimit,block"`
	GoMaxProcs int           `hcl:"gomaxprocs,optional"`
}

type RLimitConfig struct {
	NOFile uint64 `hcl:"nofile,optional"`
}

func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext) (*SystemConfig, hcl.Diagnostics) {
	c := &SystemConfig{}
	diag := gohcl.DecodeBody(block.Body, ctx, c)
	return c, diag
}

func SetGoMaxProcs(gomaxprocs int) {
	log.Debug().Int("value", gomaxprocs).Msg("Setting GOMAXPROCS")
	runtime.GOMAXPROCS(gomaxprocs)
}

func SetRlimitNOFILE(nofile uint64) {
	var rLimit syscall.Rlimit

	log.Debug().Uint64("value", nofile).Msg("Setting RLIMIT_NOFILE")

	rLimit.Max = nofile
	rLimit.Cur = nofile

	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	misc.PanicIfErr(err)
}
