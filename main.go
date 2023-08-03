package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/hcl/v2/hclsimple"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Main
func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	arg_debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *arg_debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// Parse config
	var config Config
	err := hclsimple.DecodeFile("config.hcl", nil, &config)
	panicIfErr(err)

	if config.RLimitNOFile > 0 {
		setRlimitNOFILE(config.RLimitNOFile)
	}

	// Start serious business
	subscribables := make(map[string]Subscribable, 0)
	backendProviders := make(map[string]BackendProvider, 0)

	for _, c := range config.ConsulInventoryList {
		s := NewInventoryConsul(c, &wg, ctx)
		subscribables[c.ID] = s
	}

	for _, c := range config.MySQLCheckerList {
		s := NewCheckerMySQL(c, subscribables, &wg, ctx)
		subscribables[c.ID] = s
	}

	for _, c := range config.SimpleFilterList {
		s := NewFilter(c, subscribables, &wg, ctx)
		subscribables[c.ID] = s
	}

	for _, c := range config.WRRBalancerList {
		b := NewBalancerWRR(c, subscribables, &wg, ctx)
		backendProviders[c.ID] = b
	}

	for _, c := range config.TCPProxyList {
		NewProxyTCP(c, backendProviders, &wg, ctx)
	}

	NewHTTPServer(config.HTTPAddress, &wg, ctx)

	// Termination signals
	chan_signals := make(chan os.Signal, 1)
	signal.Notify(chan_signals, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-chan_signals
		log.Info().Msg("Termination signal received")
		cancel()
	}()

	wg.Wait()
}
