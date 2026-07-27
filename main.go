// Main entry point for the mlb application.
package main

import (
	"context"
	"flag"
	"fmt"
	_ "mlb/backends_inventory/consul"
	_ "mlb/backends_inventory/static"
	_ "mlb/backends_processor/consul_kv"
	_ "mlb/backends_processor/mysql"
	_ "mlb/backends_processor/redis"
	_ "mlb/backends_processor/simple_filter"
	_ "mlb/balancer/wrr"
	_ "mlb/balancer/wlc"
	"mlb/config"
	"mlb/dashboard"
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
	argConfigTest := flag.Bool("configtest", false, "validate configuration and exit")
	argVersion := flag.Bool("version", false, "display version and exit")
	argDebug := flag.Bool("debug", false, "sets log level to debug")
	argProcessManager := flag.Bool("process-manager", false, "enable process manager mode")
	argNotifyParent := flag.Bool("notify-parent", false, "send SIGUSR1 to parent once everything is running")
	flag.Parse()

	// Handle version
	if *argVersion {
		revision, buildDate := GetBuildInfo()
		fmt.Printf("MLB %s\nBuildDate %s\nRevision %s\n", GetVersion(), buildDate, revision)
		os.Exit(0)
	}

	// Handle config test
	if *argConfigTest {
		_, diags := config.LoadConfig(*argConfig)
		if diags.HasErrors() {
			os.Exit(1)
		}
		fmt.Printf("Configuration %s is valid\n", *argConfig)
		os.Exit(0)
	}

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

		log.Info().Str("version", GetVersion()).Msg("Starting MLB")

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
			m, err := module.New(c, &wg, ctx, "backends_inventory")
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create backends_inventory module")
			}
			ml.AddModule(c.FullID(), m)
		}
		for _, c := range conf.BackendsProcessorList {
			m, err := module.New(c, &wg, ctx, "backends_processor")
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create backends_processor module")
			}
			ml.AddModule(c.FullID(), m)
		}
		for _, c := range conf.BalancerList {
			m, err := module.New(c, &wg, ctx, "balancer")
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create balancer module")
			}
			ml.AddModule(c.FullID(), m)
		}
		for _, c := range conf.ProxyList {
			m, err := module.New(c, &wg, ctx, "proxy")
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to create proxy module")
			}
			ml.AddModule(c.FullID(), m)
		}

		// Bind modules together
		for _, m := range ml {
			if b, ok := m.(module.Binder); ok {
				if err := b.Bind(ml); err != nil {
					log.Fatal().Err(err).Msg("Failed to bind module")
				}
			}
		}

		// Metrics
		http.Handle("/metrics", metrics.HttpLogWrapper(promhttp.Handler()))

		// Dashboard and API handlers
		dashboard.RegisterHandlers(http.DefaultServeMux, ml, conf)

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
