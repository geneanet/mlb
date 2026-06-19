// Main entry point for the mlb application.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"mlb/backend"
	_ "mlb/backends_inventory/consul"
	_ "mlb/backends_inventory/static"
	_ "mlb/backends_processor/consul_kv"
	_ "mlb/backends_processor/mysql"
	_ "mlb/backends_processor/redis"
	_ "mlb/backends_processor/simple_filter"
	_ "mlb/balancer/wrr"
	"mlb/config"
	"mlb/metrics"
	"mlb/module"
	_ "mlb/proxy/memcache"
	_ "mlb/proxy/redis"
	_ "mlb/proxy/tcp"
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

// main is the entry point of the application.
func main() {
	// Parse CLI args
	argConfig := flag.String("config", "config.hcl", "config file")
	argDebug := flag.Bool("debug", false, "sets log level to debug")
	argProcessManager := flag.Bool("process-manager", false, "enable process manager mode")
	argNotifyParent := flag.Bool("notify-parent", false, "send SIGUSR1 to parent once everything is running")
	flag.Parse()

	// Setup logger
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Int("pid", os.Getpid()).Caller().Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *argDebug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// CLI args validation
	if *argProcessManager && *argNotifyParent {
		log.Fatal().Msg("Parameters process-manager and notify-parent are mutually exclusive")
	}

	if *argProcessManager { // Process manager mode
		processManager()

	} else { // Normal mode
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.Background())

		// Parse conf
		conf, diags := config.LoadConfig(*argConfig)
		if diags.HasErrors() {
			os.Exit(1)
		}

		// System configuration
		if conf.System != nil {
			// Adjust max allowed file descriptors
			if conf.System.RLimit != nil && conf.System.RLimit.NOFile > 0 {
				system.SetRlimitNOFILE(conf.System.RLimit.NOFile)
			}

			// Adjust GOMAXPROCS
			if conf.System.GoMaxProcs > 0 {
				system.SetGoMaxProcs(conf.System.GoMaxProcs)
			}
		}

		// Instantiate modules
		ml := make(module.ModulesRegistry)

		for _, c := range conf.BackendsInventoryList {
			ml.AddModule(c.FullID(), module.New(c, &wg, ctx, "backends_inventory"))
		}
		for _, c := range conf.BackendsProcessorList {
			ml.AddModule(c.FullID(), module.New(c, &wg, ctx, "backends_processor"))
		}
		for _, c := range conf.BalancerList {
			ml.AddModule(c.FullID(), module.New(c, &wg, ctx, "balancer"))
		}
		for _, c := range conf.ProxyList {
			ml.AddModule(c.FullID(), module.New(c, &wg, ctx, "proxy"))
		}

		// Bind modules together
		for _, m := range ml {
			if b, ok := m.(module.Binder); ok {
				b.Bind(ml)
			}
		}

		// HTTP Metrics
		http.HandleFunc("/backends", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Content-Type", "application/json")
			backendListProviders := module.Filter[backend.BackendListProvider](ml)
			backendsByProvider := make(map[string]backend.BackendsList, len(backendListProviders))
			for id := range backendListProviders {
				backendsByProvider[id] = module.Get[backend.BackendListProvider](backendListProviders, id).GetBackendList()
			}
			out, err := json.Marshal(backendsByProvider)
			if err != nil {
				http.Error(w, "serialization error", http.StatusInternalServerError)
				return
			}
			if _, err := w.Write(out); err != nil {
				log.Warn().Err(err).Msg("Failed to write /backends response")
			}
		})
		http.Handle("/metrics", metrics.HttpLogWrapper(promhttp.Handler()))

		if conf.Metrics != nil {
			if err := metrics.NewHTTPServer(conf.Metrics.Address, &wg, ctx); err != nil {
				log.Fatal().Err(err).Msg("Failed to start metrics HTTP server")
			}
		}

		// Termination signals
		chanSignals := make(chan os.Signal, 1)
		signal.Notify(chanSignals, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			for {
				switch <-chanSignals {
				case syscall.SIGINT, syscall.SIGTERM:
					log.Info().Msg("Termination signal received")
					cancel()
					return
				}
			}
		}()

		// If requested, once everything is loaded, notify parent
		if *argNotifyParent {
			go func() {
				// Add a small delay to ensure modules are all started
				time.Sleep(5 * time.Second)
				syscall.Kill(syscall.Getppid(), syscall.SIGUSR1)
			}()
		}

		wg.Wait()
	}
}
