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
	"syscall"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/prometheus/client_golang/prometheus"
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
	backupSource          string
	backendProvider       backend.BackendProvider
	backupBackendProvider backend.BackendProvider
	closeTimeout          time.Duration
	connectTimeout        time.Duration
	clientTimeout         time.Duration
	serverTimeout         time.Duration
	timeoutMargin         time.Duration
	connectionsWG         sync.WaitGroup
	ctx                   context.Context
	cancel                context.CancelFunc
	log                   zerolog.Logger
	wg                    *sync.WaitGroup
	bufferSize            int
	nodelay               bool
	bufferPool            sync.Pool
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
	TimeoutMargin  string   `hcl:"timeout_margin,optional"`
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
	if config.TimeoutMargin == "" {
		config.TimeoutMargin = "1s"
	}
	if config.BufferSize == 0 {
		config.BufferSize = 16384
	}
	return config
}

func (w TCPProxyFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	p := &ProxyTCP{
		id:           config.ID,
		addresses:    config.Addresses,
		log:          log.With().Str("id", config.ID).Logger(),
		bufferSize:   config.BufferSize,
		nodelay:      config.NoDelay,
		source:       config.Source,
		backupSource: config.BackupSource,
		wg:           wg,
	}

	var err error

	p.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	misc.PanicIfErr(err)
	p.clientTimeout, err = time.ParseDuration(config.ClientTimeout)
	misc.PanicIfErr(err)
	p.serverTimeout, err = time.ParseDuration(config.ServerTimeout)
	misc.PanicIfErr(err)
	p.closeTimeout, err = time.ParseDuration(config.CloseTimeout)
	misc.PanicIfErr(err)
	p.timeoutMargin, err = time.ParseDuration(config.TimeoutMargin)
	misc.PanicIfErr(err)

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.bufferPool = sync.Pool{
		New: func() any {
			return make([]byte, p.bufferSize)
		},
	}

	return p
}

func (p *ProxyTCP) listen(address string, wg *sync.WaitGroup) {
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
			p.connectionsWG.Add(1)
			p.log.Debug().Str("peer", conn.RemoteAddr().String()).Msg("Accepting Frontend connection")
			go p.handleConnection(conn)
		}

		p.connectionsWG.Wait()
	}()
}

func (p *ProxyTCP) pipe(input net.Conn, output net.Conn, done chan struct{}, inputTimeout time.Duration, outputTimeout time.Duration, inCounter prometheus.Counter, outCounter prometheus.Counter) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(misc.EnsureError(r)).Msg("Error while processing pipe")
		}
		close(done)
	}()

	buffer := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buffer)

	var nextReadDeadline, nextWriteDeadline time.Time

	for {
		if inputTimeout != 0 {
			now := time.Now()
			if nextReadDeadline.IsZero() || now.Add(inputTimeout).After(nextReadDeadline) {
				nextReadDeadline = now.Add(inputTimeout + p.timeoutMargin)
				input.SetReadDeadline(nextReadDeadline)
			}
		}
		nbytes, err := input.Read(buffer)
		if nbytes > 0 {
			inCounter.Add(float64(nbytes))
		}
		if err == io.EOF || errors.Is(err, net.ErrClosed) {
			return
		}
		misc.PanicIfErr(err)
		if outputTimeout != 0 {
			now := time.Now()
			if nextWriteDeadline.IsZero() || now.Add(outputTimeout).After(nextWriteDeadline) {
				nextWriteDeadline = now.Add(outputTimeout + p.timeoutMargin)
				output.SetWriteDeadline(nextWriteDeadline)
			}
		}
		nbytes, err = output.Write(buffer[:nbytes])
		if nbytes > 0 {
			outCounter.Add(float64(nbytes))
		}
		if errors.Is(err, net.ErrClosed) {
			return
		}
		misc.PanicIfErr(err)
	}
}

func (p *ProxyTCP) handleConnection(connFront net.Conn) {
	frontendAddress := connFront.LocalAddr().String()
	peerAddress := connFront.RemoteAddr().String()

	defer p.connectionsWG.Done()
	defer connFront.Close()
	defer p.log.Debug().Str("peer", peerAddress).Msg("Closing Frontend connection")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If the proxy context is closed, close the connection after a grace period
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			p.log.Debug().Str("peer", peerAddress).Msg("Frontend closed, waiting for connection to end.")
		}

		timer := time.NewTimer(p.closeTimeout)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			p.log.Warn().Str("peer", peerAddress).Msg("Timeout reached, force closing connection.")
			cancel()
		}
	}()

	if p.nodelay {
		err := connFront.(*net.TCPConn).SetNoDelay(true)
		misc.PanicIfErr(err)
	}

	// Prometheus
	metrics.FeCnxProcessed.WithLabelValues(frontendAddress, p.id).Inc()
	metrics.FeActCnx.WithLabelValues(frontendAddress, p.id).Inc()
	defer metrics.FeActCnx.WithLabelValues(frontendAddress, p.id).Dec()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("peer", peerAddress).Err(misc.EnsureError(r)).Msg("Error while processing connection")
			// Prometheus
			metrics.FeCnxErrors.WithLabelValues(frontendAddress, p.id).Inc()
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

	var backendAddress string
	if backend != nil {
		backendAddress = backend.Address
	} else {
		panic(errors.New("no backend found"))
	}

	// Prometheus
	metrics.BeCnxProcessed.WithLabelValues(backendAddress, p.id).Inc()
	metrics.BeActCnx.WithLabelValues(backendAddress, p.id).Inc()
	defer metrics.BeActCnx.WithLabelValues(backendAddress, p.id).Dec()

	// Open backend connection
	p.log.Debug().Str("peer", backendAddress).Msg("Opening Backend connection")
	connBack, err := net.DialTimeout("tcp", backendAddress, p.connectTimeout)
	misc.PanicIfErr(err)
	defer connBack.Close()
	defer p.log.Debug().Str("peer", backendAddress).Msg("Closing Backend connection")

	if p.nodelay {
		err = connBack.(*net.TCPConn).SetNoDelay(true)
		misc.PanicIfErr(err)
	}

	feBytesInCounter := metrics.FeBytesIn.WithLabelValues(frontendAddress, p.id)
	feBytesOutCounter := metrics.FeBytesOut.WithLabelValues(frontendAddress, p.id)
	beBytesInCounter := metrics.BeBytesIn.WithLabelValues(backendAddress, p.id)
	beBytesOutCounter := metrics.BeBytesOut.WithLabelValues(backendAddress, p.id)

	// Pipe the connections both ways
	doneFrontBack := make(chan struct{})
	doneBackFront := make(chan struct{})

	go p.pipe(connFront, connBack, doneFrontBack, p.clientTimeout, p.serverTimeout, feBytesInCounter, beBytesOutCounter)
	go p.pipe(connBack, connFront, doneBackFront, p.serverTimeout, p.clientTimeout, beBytesInCounter, feBytesOutCounter)

	// Wait for one pipe to end or the context to be cancelled
	select {
	case <-doneFrontBack:
	case <-doneBackFront:
	case <-ctx.Done():
	}

	// Ensure both ends are closed so both pipes will exit
	connFront.Close()
	connBack.Close()
}

func (p *ProxyTCP) GetID() string {
	return p.id
}

func (p *ProxyTCP) Bind(modules module.ModulesList) {
	p.backendProvider = modules.GetBackendProvider(p.source)

	if p.backupSource != "" {
		p.backupBackendProvider = modules.GetBackendProvider(p.backupSource)
	}

	// Listening to incoming connections only makes sense after backend providers are available
	for _, v := range p.addresses {
		p.listen(v, p.wg)
	}
}
