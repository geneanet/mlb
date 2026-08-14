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
	_ "mlb/balancer/wlc"
	_ "mlb/balancer/wrr"
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

	"github.com/coreos/go-systemd/v22/daemon"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"

	_ "net/http/pprof"
)

// main is the entry point of the application.
func main() {
	// Parse CLI args
	argConfig := flag.String("config", "config.hcl", "config file")
	argConfigTest := flag.Bool("configtest", false, "validate configuration and exit")
	argVersion := flag.Bool("version", false, "display version and exit")
	argDebug := flag.Bool("debug", false, "sets log level to debug")
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

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	log.Info().Str("version", GetVersion()).Msg("Starting MLB")

	// Parse conf
	conf, diags := config.LoadConfig(*argConfig)
	if diags.HasErrors() {
		os.Exit(1)
	}

	// System configuration
	pidFile := ""
	if conf.System != nil {
		// Adjust max allowed file descriptors
		if conf.System.RLimit != nil && conf.System.RLimit.NOFile > 0 {
			system.SetRlimitNOFILE(conf.System.RLimit.NOFile)
		}

		// Adjust GOMAXPROCS
		if conf.System.GoMaxProcs > 0 {
			system.SetGoMaxProcs(conf.System.GoMaxProcs)
		}

		pidFile = conf.System.PIDFile
	}

	// Initialize Tableflip
	upg, err := system.InitTableflip(pidFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize tableflip")
	}
	defer upg.Stop()

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

	// Signal that the new process is ready after a small delay to ensure modules are all started
	go func() {
		time.Sleep(5 * time.Second)
		if err := upg.Ready(); err != nil {
			log.Error().Err(err).Msg("Failed to signal readiness to tableflip")
		}

		// Notify systemd that we are ready and communicate the new MAINPID.
		// This is crucial for systemd to track the correct process after a tableflip upgrade
		// and avoid killing the new process.
		_, _ = daemon.SdNotify(false, daemon.SdNotifyReady)
		_, _ = daemon.SdNotify(false, fmt.Sprintf("MAINPID=%d", os.Getpid()))
	}()

	// Termination signals
	chanSignals := make(chan os.Signal, 1)
	signal.Notify(chanSignals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for {
			select {
			case sig := <-chanSignals:
				switch sig {
				case syscall.SIGINT, syscall.SIGTERM:
					log.Info().Msg("Termination signal received")
					cancel()
					return
				case syscall.SIGHUP:
					log.Info().Msg("Upgrade signal (SIGHUP) received")

					// Notify systemd that a reload is initiating.
					// For Type=notify-reload, systemd expects RELOADING=1 and MONOTONIC_USEC.
					var ts unix.Timespec
					_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts)
					_, _ = daemon.SdNotify(false, fmt.Sprintf("%s\nMONOTONIC_USEC=%d", daemon.SdNotifyReloading, ts.Nano()/1000))

					if err := upg.Upgrade(); err != nil {
						log.Error().Err(err).Msg("Upgrade failed")
					}
				}
			case <-upg.Exit():
				log.Info().Msg("Exit signal received from tableflip (new process is ready)")
				cancel()
				return
			}
		}
	}()

	wg.Wait()
}
