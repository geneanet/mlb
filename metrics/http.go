package metrics

import (
	"context"
	"errors"
	"mlb/misc"
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

type MetricsConfig struct {
	Address string `hcl:"address"`
}

func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext) (*MetricsConfig, hcl.Diagnostics) {
	c := &MetricsConfig{}
	diag := gohcl.DecodeBody(block.Body, ctx, c)
	return c, diag
}

func HttpLogWrapper(original_handler http.Handler) http.Handler {
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
		misc.PanicIfErr(err)
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
		misc.PanicIfErr(err)

		err = srv.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		misc.PanicIfErr(err)
	}()
}
