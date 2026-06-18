package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

// MetricsConfig defines the HCL configuration for the metrics server.
type MetricsConfig struct {
	Address string `hcl:"address"`
}

// DecodeConfigBlock decodes an HCL block into a MetricsConfig.
func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext) (*MetricsConfig, hcl.Diagnostics) {
	c := &MetricsConfig{}
	diag := gohcl.DecodeBody(block.Body, ctx, c)
	return c, diag
}

// HttpLogWrapper wraps an http.Handler to log details of each request.
func HttpLogWrapper(originalHandler http.Handler) http.Handler {
	logFn := func(rw http.ResponseWriter, r *http.Request) {
		uri := r.RequestURI
		method := r.Method
		peer := r.RemoteAddr

		// Serve the request
		originalHandler.ServeHTTP(rw, r)

		// Log the details
		log.Info().Str("uri", uri).Str("method", method).Str("peer", peer).Msg("HTTP Request")
	}
	return http.HandlerFunc(logFn)
}

// NewHTTPServer creates and starts a new HTTP server with SO_REUSEPORT.
func NewHTTPServer(address string, wg *sync.WaitGroup, ctx context.Context) error {
	srv := http.Server{}

	// Shutdown the server if the context is closed
	wg.Add(1)
	context.AfterFunc(ctx, func() {
		defer wg.Done()
		err := srv.Shutdown(context.Background())
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Failed to shutdown HTTP server")
		}
	})

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
	if err != nil {
		return err
	}

	// Start the server and serve the requests
	go func() {
		log.Info().Str("address", address).Msg("Starting HTTP server")
		defer log.Info().Str("address", address).Msg("HTTP server stopped")

		err = srv.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		log.Error().Err(err).Msg("HTTP server failed")
	}()

	return nil
}
