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
	"golang.org/x/exp/slices"
)

// Main
func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	// Parse CLI args
	arg_config := flag.String("config", "config.hcl", "config file")
	arg_kill := flag.Int("kill", 0, "Kill process PID")
	arg_debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	// Setup logger
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Int("pid", os.Getpid()).Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *arg_debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// Parse conf
	conf, diags := config.LoadConfig(*arg_config)
	if diags.HasErrors() {
		os.Exit(1)
	}

	// Adjust max allowed file descriptors
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
	signal.Notify(chan_signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	go func() {
		for {
			switch <-chan_signals {
			case syscall.SIGINT, syscall.SIGTERM:
				log.Info().Msg("Termination signal received")
				cancel()

			case syscall.SIGUSR1:
				log.Info().Msg("Restart signal received")

				procAttr := os.ProcAttr{
					Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
				}

				// Ensure the children has the kill switch with current PID
				var args = make([]string, len(os.Args))
				copy(args, os.Args)
				if i := slices.Index(args, "--kill"); i >= 0 { // Update the PID if the switch was present
					args[i+1] = fmt.Sprintf("%d", os.Getpid())
				} else { // Add the switch if it was not present
					args = append(args, "--kill", fmt.Sprintf("%d", os.Getpid()))
				}

				_, err := os.StartProcess(args[0], args, &procAttr)

				if err != nil {
					log.Error().Err(err).Msg("Error while starting the new process")
				}
			}
		}
	}()

	// If we have the kill switch, kill the given PID after a short delay
	if *arg_kill != 0 {
		go func() {
			time.Sleep(5 * time.Second)
			syscall.Kill(*arg_kill, syscall.SIGTERM)
		}()
	}

	wg.Wait()
}
