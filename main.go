package main

import (
	"flag"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Main
func main() {
	var wg sync.WaitGroup

	arg_consul_url := flag.String("consul-url", "http://localhost:8500", "Consul URL")
	arg_consul_service := flag.String("consul-service", "mysql", "Consul service name")
	arg_consul_period := flag.Float64("consul-period", 1, "Default period of Consul refresh")
	arg_max_consul_period := flag.Float64("max-consul-period", 5, "Max period of Consul refresh")
	arg_mysql_user := flag.String("mysql-user", "", "MySQL user")
	arg_mysql_password := flag.String("mysql-password", "", "MySQL password")
	arg_mysql_period := flag.Float64("mysql-period", .3, "Default period of MySQL refresh")
	arg_max_mysql_period := flag.Float64("max-mysql-period", 2, "Max period of MySQL refresh")
	arg_backoff_factor := flag.Float64("backoff-factor", 1.5, "Backoff factor")
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

	consul_chan := make(chan consulMessage)

	consul := newConsul(*arg_consul_url, *arg_consul_service, *arg_consul_period, *arg_max_consul_period, *arg_backoff_factor, consul_chan)
	consul.start(&wg)

	directory := newBackendDirectory(*arg_mysql_user, *arg_mysql_password, *arg_mysql_period, *arg_max_mysql_period, *arg_backoff_factor, consul_chan)
	directory.start(&wg)

	for _, p := range arg_proxies {
		proxy := newProxy(p.address, p.tag, p.status, directory)
		proxy.start(&wg)
	}

	log.Info().Str("address", *arg_http_address).Msg("Starting HTTP server")
	http.Handle("/metrics", HTTPLogWrapper(promhttp.Handler()))
	http.ListenAndServe(*arg_http_address, nil)

	wg.Wait()
}
