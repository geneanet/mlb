package main

import (
	"syscall"

	"github.com/rs/zerolog/log"
)

type Subscribable interface {
	Subscribe() chan BackendMessage
}

type BackendProvider interface {
	GetBackend() *Backend
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func setRlimitNOFILE(nofile uint64) {
	var rLimit syscall.Rlimit

	log.Debug().Uint64("value", nofile).Msg("Setting RLIMIT_NOFILE")

	rLimit.Max = nofile
	rLimit.Cur = nofile

	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	panicIfErr(err)
}
