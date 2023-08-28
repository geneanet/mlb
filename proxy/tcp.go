package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/metrics"
	"mlb/misc"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

func init() {
	factories["tcp"] = &TCPProxyFactory{}
}

type ProxyTCP struct {
	id              string
	address         string
	backendProvider backend.BackendProvider
	close_timeout   time.Duration
	connect_timeout time.Duration
	client_timeout  time.Duration
	server_timeout  time.Duration
	listener        net.Listener
	connections_wg  sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	log             zerolog.Logger
}

type TCPProxyConfig struct {
	ID             string `hcl:"id,label"`
	Source         string `hcl:"source"`
	Address        string `hcl:"address"`
	ConnectTimeout string `hcl:"connect_timeout,optional"`
	ClientTimeout  string `hcl:"client_timeout,optional"`
	ServerTimeout  string `hcl:"server_timeout,optional"`
	CloseTimeout   string `hcl:"close_timeout,optional"`
}

type TCPProxyFactory struct{}

func (w TCPProxyFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &TCPProxyConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w TCPProxyFactory) parseConfig(tc *Config) *TCPProxyConfig {
	config := &TCPProxyConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("backends_processor.%s.%s", tc.Type, tc.Name)
	if config.ConnectTimeout == "" {
		config.ConnectTimeout = "0s"
	}
	if config.ClientTimeout == "" {
		config.ClientTimeout = "0s"
	}
	if config.ServerTimeout == "" {
		config.ServerTimeout = "0s"
	}
	if config.CloseTimeout == "" {
		config.CloseTimeout = "0s"
	}
	return config
}

func (w TCPProxyFactory) New(tc *Config, backendProviders map[string]backend.BackendProvider, wg *sync.WaitGroup, ctx context.Context) {
	config := w.parseConfig(tc)

	p := &ProxyTCP{
		id:              config.ID,
		address:         config.Address,
		backendProvider: backendProviders[config.Source],
		log:             log.With().Str("id", config.ID).Logger(),
	}

	var err error

	p.connect_timeout, err = time.ParseDuration(config.ConnectTimeout)
	misc.PanicIfErr(err)
	p.client_timeout, err = time.ParseDuration(config.ClientTimeout)
	misc.PanicIfErr(err)
	p.server_timeout, err = time.ParseDuration(config.ServerTimeout)
	misc.PanicIfErr(err)
	p.close_timeout, err = time.ParseDuration(config.CloseTimeout)
	misc.PanicIfErr(err)

	wg.Add(1)

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.log.Info().Str("address", p.address).Msg("Opening Frontend")

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
	p.listener, err = lc.Listen(context.Background(), "tcp", p.address)
	misc.PanicIfErr(err)

	go func() {
		<-p.ctx.Done()
		err := p.listener.Close()
		misc.PanicIfErr(err)
	}()

	go func() {
		defer p.log.Info().Str("address", p.address).Msg("Frontend closed")
		defer p.listener.Close()
		defer wg.Done()
		defer p.cancel()

		for {
			conn, err := p.listener.Accept()
			if errors.Is(err, net.ErrClosed) {
				break
			}
			misc.PanicIfErr(err)
			p.connections_wg.Add(1)
			p.log.Debug().Str("peer", conn.RemoteAddr().String()).Msg("Accepting Frontend connection")
			go p.handle_connection(conn)
		}

		p.connections_wg.Wait()
	}()
}

func (p *ProxyTCP) pipe(input net.Conn, output net.Conn, done chan bool, input_timeout time.Duration, output_timeout time.Duration) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(misc.EnsureError(r)).Msg("Error while processing pipe")
		}
		close(done)
	}()

	buffer := make([]byte, 64*1024)

	for {
		if input_timeout != 0 {
			input.SetReadDeadline(time.Now().Add(input_timeout))
		}
		nbytes, err := input.Read(buffer)
		if err == io.EOF || errors.Is(err, net.ErrClosed) {
			return
		}
		misc.PanicIfErr(err)
		if output_timeout != 0 {
			output.SetReadDeadline(time.Now().Add(output_timeout))
		}
		_, err = output.Write(buffer[:nbytes])
		if errors.Is(err, net.ErrClosed) {
			return
		}
		misc.PanicIfErr(err)
	}
}

func (p *ProxyTCP) handle_connection(conn_front net.Conn) {
	defer p.connections_wg.Done()
	defer conn_front.Close()
	defer p.log.Debug().Str("peer", conn_front.RemoteAddr().String()).Msg("Closing Frontend connection")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If the proxy context is closed, close the connection after a grace period
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			p.log.Debug().Str("peer", conn_front.RemoteAddr().String()).Msg("Frontend closed, waiting for connection to end.")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(p.close_timeout):
			p.log.Warn().Str("peer", conn_front.RemoteAddr().String()).Msg("Timeout reached, force closing connection.")
			cancel()
		}
	}()

	err := conn_front.(*net.TCPConn).SetNoDelay(true)
	misc.PanicIfErr(err)

	// Prometheus
	metrics.FeCnxProcessed.WithLabelValues(p.address).Inc()
	metrics.FeActCnx.WithLabelValues(p.address).Inc()
	defer metrics.FeActCnx.WithLabelValues(p.address).Dec()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("peer", conn_front.RemoteAddr().String()).Err(misc.EnsureError(r)).Msg("Error while processing connection")
			// Prometheus
			metrics.FeCnxErrors.WithLabelValues(p.address).Inc()
		}
	}()

	backend := p.backendProvider.GetBackend()
	var backend_address string
	if backend != nil {
		backend_address = backend.Address
	} else {
		panic(errors.New("no backend found"))
	}

	// Prometheus
	metrics.BeCnxProcessed.WithLabelValues(backend_address).Inc()
	metrics.BeActCnx.WithLabelValues(backend_address).Inc()
	defer metrics.BeActCnx.WithLabelValues(backend_address).Dec()

	// Open backend connection
	p.log.Debug().Str("peer", backend_address).Msg("Opening Backend connection")
	conn_back, err := net.DialTimeout("tcp", backend_address, p.connect_timeout)
	misc.PanicIfErr(err)
	defer conn_back.Close()
	defer p.log.Debug().Str("peer", backend_address).Msg("Closing Backend connection")

	err = conn_back.(*net.TCPConn).SetNoDelay(true)
	misc.PanicIfErr(err)

	// Pipe the connections both ways
	done_front_back := make(chan bool)
	done_back_front := make(chan bool)

	go p.pipe(conn_front, conn_back, done_front_back, p.client_timeout, p.server_timeout)
	go p.pipe(conn_back, conn_front, done_back_front, p.server_timeout, p.client_timeout)

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

func (p *ProxyTCP) GetID() string {
	return p.id
}
