package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

type Proxy struct {
	address            string
	backend_tag        string
	backend_status     string
	directory          *BackendDirectory
	close_timeout      time.Duration
	listener           net.Listener
	connections_wg     sync.WaitGroup
	connections_ctx    context.Context
	connections_cancel context.CancelFunc
	ctx                context.Context
	cancel             context.CancelFunc
}

func newProxy(address string, backend_tag string, backend_status string, directory *BackendDirectory, close_timeout time.Duration, wg *sync.WaitGroup, ctx context.Context) *Proxy {
	p := &Proxy{
		address:        address,
		backend_tag:    backend_tag,
		backend_status: backend_status,
		directory:      directory,
		close_timeout:  close_timeout,
	}

	wg.Add(1)

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.connections_ctx, p.connections_cancel = context.WithCancel(p.ctx)

	log.Info().Str("address", p.address).Str("backend_tag", p.backend_tag).Str("backend_status", p.backend_status).Msg("Opening Frontend")

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
	var err error
	p.listener, err = lc.Listen(context.Background(), "tcp", p.address)
	panicIfErr(err)

	go func() {
		<-p.ctx.Done()
		err := p.listener.Close()
		panicIfErr(err)
	}()

	go func() {
		defer log.Info().Str("address", p.address).Str("backend_tag", p.backend_tag).Str("backend_status", p.backend_status).Msg("Frontend closed")
		defer p.listener.Close()
		defer wg.Done()

		for {
			conn, err := p.listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				break
			}
			panicIfErr(err)
			p.connections_wg.Add(1)
			log.Debug().Str("peer", conn.RemoteAddr().String()).Msg("Accepting Frontend connection")
			go p._handle_connection(conn)
		}

		p.connections_wg.Wait()
	}()

	return p
}

func (p *Proxy) _pipe(input net.Conn, output net.Conn, done chan bool) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(r.(error)).Msg("Error while processing pipe")
		}
		done <- true
	}()

	buffer := make([]byte, 64*1024)

	for {
		nbytes, err := input.Read(buffer)
		if err == io.EOF || errors.Is(err, net.ErrClosed) {
			return
		}
		panicIfErr(err)
		_, err = output.Write(buffer[:nbytes])
		if errors.Is(err, net.ErrClosed) {
			return
		}
		panicIfErr(err)
	}
}

func (p *Proxy) _handle_connection(conn_front net.Conn) {
	defer p.connections_wg.Done()
	defer conn_front.Close()
	defer log.Debug().Str("peer", conn_front.RemoteAddr().String()).Msg("Closing Frontend connection")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If the proxy context is closed, close the connection after a grace period
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			log.Debug().Str("peer", conn_front.RemoteAddr().String()).Msg("Frontend closed, waiting for connection to end.")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(p.close_timeout):
			log.Warn().Str("peer", conn_front.RemoteAddr().String()).Msg("Timeout reached, force closing connection.")
			cancel()
		}
	}()

	err := conn_front.(*net.TCPConn).SetNoDelay(true)
	panicIfErr(err)

	// Prometheus
	metrics_FeCnxProcessed.WithLabelValues(p.address).Inc()
	metrics_FeActCnx.WithLabelValues(p.address).Inc()
	defer metrics_FeActCnx.WithLabelValues(p.address).Dec()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("peer", conn_front.RemoteAddr().String()).Err(r.(error)).Msg("Error while processing connection")
			// Prometheus
			metrics_FeCnxErrors.WithLabelValues(p.address).Inc()
		}
	}()

	backend_address, err := p.directory.getBackend(p.backend_tag, p.backend_status)
	panicIfErr(err)

	// Prometheus
	metrics_BeCnxProcessed.WithLabelValues(backend_address).Inc()
	metrics_BeActCnx.WithLabelValues(backend_address).Inc()
	defer metrics_BeActCnx.WithLabelValues(backend_address).Dec()

	// Open backend connection
	backend_addrport, err := netip.ParseAddrPort(backend_address)
	panicIfErr(err)

	log.Debug().Str("peer", backend_address).Msg("Opening Backend connection")
	conn_back, err := net.DialTCP("tcp", nil, net.TCPAddrFromAddrPort(backend_addrport))
	panicIfErr(err)
	defer conn_back.Close()
	defer log.Debug().Str("peer", backend_address).Msg("Closing Backend connection")

	err = conn_back.SetNoDelay(true)
	panicIfErr(err)

	// Pipe the connections both ways (need a size of 1 so that the pipes will not block on exit if nobody is listening on the channel)
	done_front_back := make(chan bool, 1)
	done_back_front := make(chan bool, 1)

	go p._pipe(conn_front, conn_back, done_front_back)
	go p._pipe(conn_back, conn_front, done_back_front)

	// Wait for one pipe to end or the context to be cancelled
	select {
	case <-done_front_back:
	case <-done_back_front:
	case <-ctx.Done():
	}

	// Ensure both ends are closed so both pipes will exit
	conn_front.Close()
	conn_back.Close()
}
