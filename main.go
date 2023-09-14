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
	"mlb/module"
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

	_ "net/http/pprof"
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

		// Instanciate modules
		ml := module.NewModulesList()

		for _, c := range conf.BackendsInventoryList {
			ml.AddModule(backends_inventory.New(c, &wg, ctx))
		}
		for _, c := range conf.BackendsProcessorList {
			ml.AddModule(backends_processor.New(c, &wg, ctx))
		}
		for _, c := range conf.BalancerList {
			ml.AddModule(balancer.New(c, &wg, ctx))
		}
		for _, c := range conf.ProxyList {
			ml.AddModule(proxy.New(c, &wg, ctx))
		}

		// Bind modules together
		for _, m := range ml {
			m.Bind(ml)
		}

		// HTTP Metrics
		http.HandleFunc("/backends", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			backendListProviders := ml.GetBackendListProviders()
			backendsByProvider := make(map[string]backend.BackendsList, len(backendListProviders))
			for id := range backendListProviders {
				backendsByProvider[id] = backendListProviders.GetBackendListProvider(id).GetBackendList()
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
