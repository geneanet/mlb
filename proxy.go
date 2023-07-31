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

	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

type Proxy struct {
	address        string
	backend_tag    string
	backend_status string
	directory      *BackendDirectory
	running        bool
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

func (p *Proxy) start(wg *sync.WaitGroup) {
	if p.running {
		return
	}

	p.running = true
	wg.Add(1)

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
	listener, err := lc.Listen(context.Background(), "tcp", p.address)
	panicIfErr(err)

	go func() {
		defer listener.Close()
		defer wg.Done()
		defer func() { p.running = false }()

		for {
			conn, err := listener.Accept()
			panicIfErr(err)
			go p._handle_connection(conn)
		}
	}()
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
	log.Debug().Str("address", conn_front.RemoteAddr().String()).Msg("Accepting Frontend connection")
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

	// Wait for one pipe to end
	select {
	case <-done_front_back:
	case <-done_back_front:
	}

	// Close both connections to ensure the other pipe will end
	conn_front.Close()
	conn_back.Close()

}
