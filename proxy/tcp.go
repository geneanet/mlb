package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/metrics"
	"mlb/misc"
	"mlb/module"
	"net"
	"os"
	"sync"
	"sync/atomic"
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
	id                    string
	addresses             []string
	source                string
	backup_source         string
	backendProvider       backend.BackendProvider
	backupBackendProvider backend.BackendProvider
	close_timeout         time.Duration
	connect_timeout       time.Duration
	client_timeout        time.Duration
	server_timeout        time.Duration
	connections_wg        sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	log                   zerolog.Logger
	wg                    *sync.WaitGroup
	buffer_size           int
	nodelay               bool
	buffer_pool           sync.Pool
}

type TCPProxyConfig struct {
	ID             string   `hcl:"id,label"`
	Source         string   `hcl:"source"`
	BackupSource   string   `hcl:"backup_source,optional"`
	Addresses      []string `hcl:"addresses,optional"`
	ConnectTimeout string   `hcl:"connect_timeout,optional"`
	ClientTimeout  string   `hcl:"client_timeout,optional"`
	ServerTimeout  string   `hcl:"server_timeout,optional"`
	CloseTimeout   string   `hcl:"close_timeout,optional"`
	BufferSize     int      `hcl:"buffer_size,optional"`
	NoDelay        bool     `hcl:"nodelay,optional"`
}

type TCPProxyFactory struct{}

func (w TCPProxyFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &TCPProxyConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w TCPProxyFactory) parseConfig(tc *Config) *TCPProxyConfig {
	config := &TCPProxyConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("proxy.%s.%s", tc.Type, tc.Name)
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
	if config.BufferSize == 0 {
		config.BufferSize = 16384
	}
	return config
}

func (w TCPProxyFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	p := &ProxyTCP{
		id:            config.ID,
		addresses:     config.Addresses,
		log:           log.With().Str("id", config.ID).Logger(),
		buffer_size:   config.BufferSize,
		nodelay:       config.NoDelay,
		source:        config.Source,
		backup_source: config.BackupSource,
		wg:            wg,
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

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.buffer_pool = sync.Pool{
		New: func() any {
			return make([]byte, p.buffer_size)
		},
	}

	return p
}

func (p *ProxyTCP) listen(address string, wg *sync.WaitGroup, ctx context.Context) {
	p.log.Info().Str("address", address).Msg("Opening Frontend")

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

	go func() {
		<-p.ctx.Done()
		err := listener.Close()
		misc.PanicIfErr(err)
	}()

	wg.Add(1)
	go func() {
		defer p.log.Info().Str("address", address).Msg("Frontend closed")
		defer listener.Close()
		defer wg.Done()
		defer p.cancel()

		for {
			conn, err := listener.Accept()
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

func (p *ProxyTCP) pipe(input net.Conn, output net.Conn, done chan struct{}, input_timeout time.Duration, output_timeout time.Duration, counter *atomic.Uint64) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(misc.EnsureError(r)).Msg("Error while processing pipe")
		}
		close(done)
	}()

	buffer := p.buffer_pool.Get().([]byte)
	defer p.buffer_pool.Put(buffer)

	for {
		if input_timeout != 0 {
			input.SetReadDeadline(time.Now().Add(input_timeout))
		}
		nbytes, err := input.Read(buffer)
		counter.Add(uint64(nbytes))
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
	frontend_address := conn_front.LocalAddr().String()
	peer_address := conn_front.RemoteAddr().String()

	defer p.connections_wg.Done()
	defer conn_front.Close()
	defer p.log.Debug().Str("peer", peer_address).Msg("Closing Frontend connection")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If the proxy context is closed, close the connection after a grace period
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			p.log.Debug().Str("peer", peer_address).Msg("Frontend closed, waiting for connection to end.")
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(p.close_timeout):
			p.log.Warn().Str("peer", peer_address).Msg("Timeout reached, force closing connection.")
			cancel()
		}
	}()

	if p.nodelay {
		err := conn_front.(*net.TCPConn).SetNoDelay(true)
		misc.PanicIfErr(err)
	}

	// Prometheus
	metrics.FeCnxProcessed.WithLabelValues(frontend_address, p.id).Inc()
	metrics.FeActCnx.WithLabelValues(frontend_address, p.id).Inc()
	defer metrics.FeActCnx.WithLabelValues(frontend_address, p.id).Dec()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("peer", peer_address).Err(misc.EnsureError(r)).Msg("Error while processing connection")
			// Prometheus
			metrics.FeCnxErrors.WithLabelValues(frontend_address, p.id).Inc()
		}
	}()

	// Try to get a primary backend
	backend := p.backendProvider.GetBackend(false)
	// If no backend try to get a backup backend
	if backend == nil && p.backupBackendProvider != nil {
		backend = p.backupBackendProvider.GetBackend(false)
	}
	// If still no backend try waiting for a primary backend
	if backend == nil {
		backend = p.backendProvider.GetBackend(true)
	}

	var backend_address string
	if backend != nil {
		backend_address = backend.Address
	} else {
		panic(errors.New("no backend found"))
	}

	// Prometheus
	metrics.BeCnxProcessed.WithLabelValues(backend_address, p.id).Inc()
	metrics.BeActCnx.WithLabelValues(backend_address, p.id).Inc()
	defer metrics.BeActCnx.WithLabelValues(backend_address, p.id).Dec()

	// Open backend connection
	p.log.Debug().Str("peer", backend_address).Msg("Opening Backend connection")
	conn_back, err := net.DialTimeout("tcp", backend_address, p.connect_timeout)
	misc.PanicIfErr(err)
	defer conn_back.Close()
	defer p.log.Debug().Str("peer", backend_address).Msg("Closing Backend connection")

	if p.nodelay {
		err = conn_back.(*net.TCPConn).SetNoDelay(true)
		misc.PanicIfErr(err)
	}

	// Pipe the connections both ways
	done_front_back := make(chan struct{})
	done_back_front := make(chan struct{})
	var bytes_in, bytes_out atomic.Uint64

	go p.pipe(conn_front, conn_back, done_front_back, p.client_timeout, p.server_timeout, &bytes_in)
	go p.pipe(conn_back, conn_front, done_back_front, p.server_timeout, p.client_timeout, &bytes_out)

	statsTicker := time.NewTicker(1 * time.Second)
	defer statsTicker.Stop()

	// Wait for one pipe to end or the context to be cancelled + ticker every second for stats
	break_loop := false
	for {
		select {
		case <-done_front_back:
			break_loop = true
		case <-done_back_front:
			break_loop = true
		case <-ctx.Done():
			break_loop = true
		case <-statsTicker.C:
		}

		bytes_in_f := float64(bytes_in.Swap(0))
		bytes_out_f := float64(bytes_out.Swap(0))
		metrics.FeBytesIn.WithLabelValues(frontend_address, p.id).Add(bytes_in_f)
		metrics.FeBytesOut.WithLabelValues(frontend_address, p.id).Add(bytes_out_f)
		metrics.BeBytesIn.WithLabelValues(backend_address, p.id).Add(bytes_in_f)
		metrics.BeBytesOut.WithLabelValues(backend_address, p.id).Add(bytes_out_f)

		if break_loop {
			break
		}
	}

	// Ensure both ends are closed so both pipes will exit
	conn_front.Close()
	conn_back.Close()
}

func (p *ProxyTCP) GetID() string {
	return p.id
}

func (p *ProxyTCP) Bind(modules module.ModulesList) {
	p.backendProvider = modules.GetBackendProvider(p.source)

	if p.backup_source != "" {
		p.backupBackendProvider = modules.GetBackendProvider(p.backup_source)
	}

	// Listening to incoming connections only makes sense after backend providers are available
	for _, v := range p.addresses {
		p.listen(v, p.wg, p.ctx)
	}
}
