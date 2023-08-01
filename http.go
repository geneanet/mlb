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

type HTTPServer struct {
	address string
	running bool
	srv     *http.Server
}

func newHTTPServer(address string) *HTTPServer {
	return &HTTPServer{
		address: address,
		srv:     &http.Server{},
	}
}

func (s *HTTPServer) start(wg *sync.WaitGroup) {
	if s.running {
		return
	}

	s.running = true
	wg.Add(1)

	go func() {
		log.Info().Str("address", s.address).Msg("Starting HTTP server")
		defer log.Info().Str("address", s.address).Msg("HTTP server stopped")
		defer wg.Done()

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
		listener, err := lc.Listen(context.Background(), "tcp", s.address)
		panicIfErr(err)

		http.Handle("/metrics", HTTPLogWrapper(promhttp.Handler()))

		err = s.srv.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		panicIfErr(err)
	}()
}

func (s *HTTPServer) stop() {
	err := s.srv.Shutdown(context.Background())
	panicIfErr(err)
}
