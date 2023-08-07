package main

import (
	"context"
	"flag"
	"fmt"
	"mlb/backend"
	"mlb/balancer"
	"mlb/checker"
	"mlb/config"
	"mlb/filter"
	"mlb/inventory"
	"mlb/metrics"
	"mlb/proxy"
	"mlb/system"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Main
func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	arg_config := flag.String("config", "config.hcl", "config file")
	arg_debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *arg_debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// Parse conf
	conf, diags := config.LoadConfig(*arg_config)
	if diags.HasErrors() {
		os.Exit(1)
	}

	if conf.System.RLimit.NOFile > 0 {
		system.SetRlimitNOFILE(conf.System.RLimit.NOFile)
	}

	// Start serious business
	subscribables := make(map[string]backend.Subscribable, 0)
	backendProviders := make(map[string]backend.BackendProvider, 0)

	for _, c := range conf.InventoryList {
		s := inventory.New(c, &wg, ctx)
		subscribables[fmt.Sprintf("inventory.%s.%s", c.Type, c.Name)] = s
	}

	for _, c := range conf.CheckerList {
		s := checker.New(c, subscribables, &wg, ctx)
		subscribables[fmt.Sprintf("checker.%s.%s", c.Type, c.Name)] = s
	}

	for _, c := range conf.FilterList {
		s := filter.New(c, subscribables, &wg, ctx)
		subscribables[fmt.Sprintf("filter.%s.%s", c.Type, c.Name)] = s
	}

	for _, c := range conf.BalancerList {
		b := balancer.New(c, subscribables, &wg, ctx)
		backendProviders[fmt.Sprintf("balancer.%s.%s", c.Type, c.Name)] = b
	}

	for _, c := range conf.ProxyList {
		proxy.New(c, backendProviders, &wg, ctx)
	}

	metrics.NewHTTPServer(conf.Metrics.Address, &wg, ctx)

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
