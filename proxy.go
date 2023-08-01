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
	running            bool
	listener           net.Listener
	connections_wg     sync.WaitGroup
	connections_ctx    context.Context
	connections_cancel context.CancelFunc
}

func newProxy(address string, backend_tag string, backend_status string, directory *BackendDirectory) *Proxy {
	return &Proxy{
		address:        address,
		backend_tag:    backend_tag,
		backend_status: backend_status,
		directory:      directory,
		running:        false,
	}
}

func (p *Proxy) start(wg *sync.WaitGroup, ctx context.Context) {
	if p.running {
		return
	}

	p.running = true
	wg.Add(1)
	p.connections_ctx, p.connections_cancel = context.WithCancel(ctx)

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
		defer p.listener.Close()
		defer wg.Done()
		defer func() { p.running = false }()

		for {
			conn, err := p.listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				return
			}
			panicIfErr(err)
			p.connections_wg.Add(1)
			log.Debug().Str("address", conn.RemoteAddr().String()).Msg("Accepting Frontend connection")
			go p._handle_connection(conn)
		}
	}()
}

func (p *Proxy) stop() {
	if !p.running {
		return
	}

	p.listener.Close()
	p.running = false
	log.Info().Str("address", p.address).Str("backend_tag", p.backend_tag).Str("backend_status", p.backend_status).Msg("Frontend closed")
}

func (p *Proxy) close_connections() {
	p.connections_cancel()
}

func (p *Proxy) wait_connections(ctx context.Context, timeout time.Duration) {
	var (
		local_ctx context.Context
		cancel    context.CancelFunc
	)

	if timeout > 0 {
		local_ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		local_ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	waited := make(chan bool)

	go func() {
		p.connections_wg.Wait()
		waited <- true
	}()

	select {
	case <-waited:
	case <-local_ctx.Done():
		log.Warn().Str("address", p.address).Str("backend_tag", p.backend_tag).Str("backend_status", p.backend_status).Msg("Timeout reached, force closing connections !")
	}

	// Close remaining connections and cancel associated context
	p.close_connections()
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
	defer log.Debug().Str("address", conn_front.RemoteAddr().String()).Msg("Closing Frontend connection")

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

	log.Debug().Str("address", backend_address).Msg("Opening Backend connection")
	conn_back, err := net.DialTCP("tcp", nil, net.TCPAddrFromAddrPort(backend_addrport))
	panicIfErr(err)
	defer conn_back.Close()
	defer log.Debug().Str("address", backend_address).Msg("Closing Backend connection")

	err = conn_back.SetNoDelay(true)
	panicIfErr(err)

	// Pipe the connections both ways
	done_front_back := make(chan bool)
	done_back_front := make(chan bool)

	go p._pipe(conn_front, conn_back, done_front_back)
	go p._pipe(conn_back, conn_front, done_back_front)

	// Wait for one pipe to end or the proxy is force closed
	select {
	case <-done_front_back:
	case <-done_back_front:
	case <-p.connections_ctx.Done():
	}

	// Close both connections to ensure the other pipe will end
	conn_front.Close()
	conn_back.Close()
}
