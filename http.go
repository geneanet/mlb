package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

func HTTPLogWrapper(original_handler http.Handler) http.Handler {
	logFn := func(rw http.ResponseWriter, r *http.Request) {
		uri := r.RequestURI
		method := r.Method
		peer := r.RemoteAddr

		// Serve the request
		original_handler.ServeHTTP(rw, r)

		// Log the details
		log.Info().Str("uri", uri).Str("method", method).Str("peer", peer).Msg("HTTP Request")
	}
	return http.HandlerFunc(logFn)
}

func NewHTTPServer(address string, wg *sync.WaitGroup, ctx context.Context) {
	srv := http.Server{}

	wg.Add(1)

	// Shutdown the server if the context is closed
	go func() {
		<-ctx.Done()
		err := srv.Shutdown(context.Background())
		panicIfErr(err)
	}()

	// Start the server and serve the requests
	go func() {
		log.Info().Str("address", address).Msg("Starting HTTP server")
		defer wg.Done()
		defer log.Info().Str("address", address).Msg("HTTP server stopped")

		// Set SO_REUSEPORT
		lc := net.ListenConfig{
			Control: func(network, address string, conn syscall.RawConn) error {
				var operr error
				if err := conn.Control(func(fd uintptr) {
					operr = os.NewSyscallError("setsockopt", syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1))
				}); err != nil {
					return err
				}
				return operr
			},
		}

		// Bind
		listener, err := lc.Listen(context.Background(), "tcp", address)
		panicIfErr(err)

		http.Handle("/metrics", HTTPLogWrapper(promhttp.Handler()))

		err = srv.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		panicIfErr(err)
	}()
}
