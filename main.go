package main

import (
	"context"
	"flag"
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

	arg_consul_url := flag.String("consul-url", "http://localhost:8500", "Consul URL")
	arg_consul_service := flag.String("consul-service", "mysql", "Consul service name")
	arg_consul_period := flag.Float64("consul-period", 1, "Default period of Consul refresh")
	arg_max_consul_period := flag.Float64("max-consul-period", 5, "Max period of Consul refresh")
	arg_mysql_user := flag.String("mysql-user", "", "MySQL user")
	arg_mysql_password := flag.String("mysql-password", "", "MySQL password")
	arg_mysql_period := flag.Float64("mysql-period", .3, "Default period of MySQL refresh")
	arg_max_mysql_period := flag.Float64("max-mysql-period", 2, "Max period of MySQL refresh")
	arg_backoff_factor := flag.Float64("backoff-factor", 1.5, "Backoff factor")
	arg_rlimit_nofile := flag.Uint64("rlimit-nofile", 0, "Set OS limit for nunmber of open files")
	arg_close_timeout := flag.Duration("close-timeout", 1*time.Hour, "Time to wait on exit before closing all connections")
	arg_debug := flag.Bool("debug", false, "sets log level to debug")
	arg_proxies := proxyFlags{}
	flag.Var(&arg_proxies, "proxy", "Add a proxy (ip:port,tag,status)")
	arg_http_address := flag.String("http-address", ":2112", "HTTP binding address")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *arg_debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	if *arg_rlimit_nofile > 0 {
		setRlimitNOFILE(*arg_rlimit_nofile)
	}

	inv := NewInventoryConsul("inv_consul", *arg_consul_url, *arg_consul_service, *arg_consul_period, *arg_max_consul_period, *arg_backoff_factor, &wg, ctx)
	chk := NewCheckerMySQL("chk_mysql", *arg_mysql_user, *arg_mysql_password, *arg_mysql_period, *arg_max_mysql_period, *arg_backoff_factor, inv, &wg, ctx)
	for _, p := range arg_proxies {
		filter := NewFilter("filter_"+p.id, p.tag, p.status, chk, &wg, ctx)
		balancer := NewBalancerWRR("balancer_"+p.id, filter, &wg, ctx)
		NewProxyTCP("proxy_"+p.id, p.address, balancer, *arg_close_timeout, &wg, ctx)
	}

	NewHTTPServer(*arg_http_address, &wg, ctx)

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
