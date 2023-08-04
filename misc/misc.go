package misc

import (
	"syscall"

	"github.com/rs/zerolog/log"
)

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func SetRlimitNOFILE(nofile uint64) {
	var rLimit syscall.Rlimit

	log.Debug().Uint64("value", nofile).Msg("Setting RLIMIT_NOFILE")

	rLimit.Max = nofile
	rLimit.Cur = nofile

	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	PanicIfErr(err)
}
