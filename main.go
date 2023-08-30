package main

import (
	"context"
	"encoding/json"
	"flag"
	"mlb/backend"
	"mlb/backends_inventory"
	"mlb/backends_processor"
	"mlb/balancer"
	"mlb/config"
	"mlb/metrics"
	"mlb/misc"
	"mlb/proxy"
	"mlb/system"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Main
func main() {
	// Parse CLI args
	arg_config := flag.String("config", "config.hcl", "config file")
	arg_debug := flag.Bool("debug", false, "sets log level to debug")
	arg_process_manager := flag.Bool("process-manager", false, "enable process manager mode")
	arg_notify_parent := flag.Bool("notify-parent", false, "send SIGUSR1 to parent once everything is running")
	flag.Parse()

	// Setup logger
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Int("pid", os.Getpid()).Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *arg_debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// CLI args validation
	if *arg_process_manager && *arg_notify_parent {
		log.Fatal().Msg("Parameters process-manager and notify-parent are mutually exclusives")
	}

	if *arg_process_manager { // Process manager mode
		processManager()

	} else { // Normal mode
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())

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
		backendUpdatesProviders := make(map[string]backend.BackendUpdateProvider, 0)
		backendUpdateSubscribers := make(map[string]backend.BackendUpdateSubscriber, 0)
		backendProviders := make(map[string]backend.BackendProvider, 0)
		backendListProviders := make(map[string]backend.BackendListProvider, 0)

		for _, tc := range conf.BackendsInventoryList {
			i := backends_inventory.New(tc, &wg, ctx)
			id := i.(misc.GetIDInterface).GetID()
			backendUpdatesProviders[id] = i.(backend.BackendUpdateProvider)
			backendListProviders[id] = i.(backend.BackendListProvider)
		}

		for _, tc := range conf.BackendsProcessorList {
			f := backends_processor.New(tc, &wg, ctx)
			id := f.(misc.GetIDInterface).GetID()
			backendUpdatesProviders[id] = f.(backend.BackendUpdateProvider)
			backendListProviders[id] = f.(backend.BackendListProvider)
			backendUpdateSubscribers[id] = f.(backend.BackendUpdateSubscriber)
		}

		for _, tc := range conf.BalancerList {
			b := balancer.New(tc, &wg, ctx)
			id := b.(misc.GetIDInterface).GetID()
			backendProviders[id] = b.(backend.BackendProvider)
			backendListProviders[id] = b.(backend.BackendListProvider)
			backendUpdateSubscribers[id] = b.(backend.BackendUpdateSubscriber)
		}

		for _, c := range conf.ProxyList {
			proxy.New(c, backendProviders, &wg, ctx)
		}

		// Plug update subscribers to providers
		for _, bus := range backendUpdateSubscribers {
			source := bus.GetUpdateSource()
			provider, ok := backendUpdatesProviders[source]
			if !ok {
				log.Panic().Str("subscriber", bus.(misc.GetIDInterface).GetID()).Str("provider", source).Msg("Backend update provider not found !")
			}
			bus.SubscribeTo(provider)
		}

		// HTTP Metrics
		http.HandleFunc("/backends", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			backendsByProvider := make(map[string]backend.BackendsList, len(backendListProviders))
			for id := range backendListProviders {
				backendsByProvider[id] = backendListProviders[id].GetBackendList()
			}
			out, _ := json.Marshal(backendsByProvider)
			w.Write(out)
		})
		http.Handle("/metrics", metrics.HttpLogWrapper(promhttp.Handler()))

		metrics.NewHTTPServer(conf.Metrics.Address, &wg, ctx)

		// Termination signals
		chan_signals := make(chan os.Signal, 1)
		signal.Notify(chan_signals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			for {
				switch <-chan_signals {
				case syscall.SIGINT, syscall.SIGTERM:
					log.Info().Msg("Termination signal received")
					cancel()
				}
			}
		}()

		// If requested, once everything is loaded, notify parent
		if *arg_notify_parent {
			go func() {
				// Add a small delay to ensure modules are all started
				time.Sleep(5 * time.Second)
				syscall.Kill(syscall.Getppid(), syscall.SIGUSR1)
			}()
		}

		wg.Wait()
	}
}
