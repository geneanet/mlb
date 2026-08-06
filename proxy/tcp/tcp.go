package tcp

import (
	"context"
	"errors"
	"io"
	"mlb/backend"
	"mlb/config"
	"mlb/metrics"
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
	module.RegisterFactory("proxy", "tcp", newTCPProxy, validateTCPProxyConfig)
}

// ProxyTCP implements a TCP level proxy.
type ProxyTCP struct {
	id                    string
	addresses             []string
	sources               []string
	backendProviders      []backend.BackendProvider
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
	bufferPool            sync.Pool
	beMetricsCache        map[string]*Metrics
	beMetricsMutex        sync.RWMutex
	closeOnBackendRemoval bool
}

// Metrics holds Prometheus metrics for a specific backend.
type Metrics struct {
	processed prometheus.Counter
	active    prometheus.Gauge
	bytesIn   prometheus.Counter
	bytesOut  prometheus.Counter
	cnxErrors prometheus.Counter
}

// TCPProxyConfig defines the HCL configuration for the TCP proxy.
type TCPProxyConfig struct {
	ID                    string   `hcl:"id,label"`
	Sources               []string `hcl:"sources,optional"`
	Source                string   `hcl:"source,optional"`
	BackupSource          string   `hcl:"backup_source,optional"`
	Addresses             []string `hcl:"addresses,optional"`
	ConnectTimeout        string   `hcl:"connect_timeout,optional"`
	ClientTimeout         string   `hcl:"client_timeout,optional"`
	ServerTimeout         string   `hcl:"server_timeout,optional"`
	CloseTimeout          string   `hcl:"close_timeout,optional"`
	TimeoutMargin         string   `hcl:"timeout_margin,optional"`
	BufferSize            int      `hcl:"buffer_size,optional"`
	CloseOnBackendRemoval bool     `hcl:"close_on_backend_removal,optional"`
}

// validateTCPProxyConfig validates the TCP proxy configuration.
func validateTCPProxyConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &TCPProxyConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	if len(configBody.Sources) == 0 && configBody.Source == "" {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Missing source configuration",
			Detail:   "Either 'sources' or 'source' must be defined.",
		})
	}

	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.ClientTimeout, "client_timeout")
	config.CheckDuration(&diags, configBody.ServerTimeout, "server_timeout")
	config.CheckDuration(&diags, configBody.CloseTimeout, "close_timeout")
	config.CheckDuration(&diags, configBody.TimeoutMargin, "timeout_margin")

	return diags
}

// parseTCPProxyConfig parses the TCP proxy configuration.
func parseTCPProxyConfig(tc *module.Config) *TCPProxyConfig {
	config := &TCPProxyConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode TCP proxy config")
	}
	config.ID = tc.FullID()

	// Handle backward compatibility for source and backup_source
	if len(config.Sources) == 0 && config.Source != "" {
		config.Sources = []string{config.Source}
		if config.BackupSource != "" {
			config.Sources = append(config.Sources, config.BackupSource)
		}
	}

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
		config.BufferSize = 32768
	}
	return config
}

func newTCPProxy(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseTCPProxyConfig(tc)

	p := &ProxyTCP{
		id:                    config.ID,
		addresses:             config.Addresses,
		log:                   log.With().Str("id", config.ID).Logger(),
		bufferSize:            config.BufferSize,
		sources:               config.Sources,
		wg:                    wg,
		beMetricsCache:        make(map[string]*Metrics),
		closeOnBackendRemoval: config.CloseOnBackendRemoval,
	}

	var err error

	p.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	p.clientTimeout, err = time.ParseDuration(config.ClientTimeout)
	if err != nil {
		return nil, err
	}
	p.serverTimeout, err = time.ParseDuration(config.ServerTimeout)
	if err != nil {
		return nil, err
	}
	p.closeTimeout, err = time.ParseDuration(config.CloseTimeout)
	if err != nil {
		return nil, err
	}
	p.timeoutMargin, err = time.ParseDuration(config.TimeoutMargin)
	if err != nil {
		return nil, err
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.bufferPool = sync.Pool{
		New: func() any {
			return &bufferWrapper{buf: make([]byte, p.bufferSize)}
		},
	}

	return p, nil
}

func (p *ProxyTCP) listen(address string, wg *sync.WaitGroup) error {
	p.log.Info().Str("address", address).Msg("Opening Frontend")

	feMetrics := &Metrics{
		processed: metrics.FeCnxProcessed.WithLabelValues(address, p.id),
		active:    metrics.FeActCnx.WithLabelValues(address, p.id),
		bytesIn:   metrics.FeBytesIn.WithLabelValues(address, p.id),
		bytesOut:  metrics.FeBytesOut.WithLabelValues(address, p.id),
		cnxErrors: metrics.FeCnxErrors.WithLabelValues(address, p.id),
	}

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

	context.AfterFunc(p.ctx, func() {
		_ = listener.Close()
	})

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
			if err != nil {
				panic(err)
			}
			p.connectionsWG.Add(1)
			p.log.Debug().Str("peer", conn.RemoteAddr().String()).Msg("Accepting Frontend connection")
			go p.handleConnection(conn, feMetrics)
		}

		p.connectionsWG.Wait()
	}()

	return nil
}

type bufferWrapper struct {
	buf []byte
}

func (p *ProxyTCP) pipe(input net.Conn, output net.Conn, done chan struct{}, inputTimeout time.Duration, outputTimeout time.Duration, inCounter prometheus.Counter, outCounter prometheus.Counter) {
	// Signal completion
	defer close(done)

	// Recover from unexpected panics to prevent proxy crashes
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Interface("error", r).Msg("Error while processing pipe")
		}
	}()

	wrapper := p.bufferPool.Get().(*bufferWrapper)
	buffer := wrapper.buf
	defer p.bufferPool.Put(wrapper)

	var nextReadDeadline, nextWriteDeadline time.Time

	for {
		if inputTimeout != 0 {
			now := time.Now()
			if nextReadDeadline.IsZero() || now.Add(inputTimeout).After(nextReadDeadline) {
				nextReadDeadline = now.Add(inputTimeout + p.timeoutMargin)
				input.SetReadDeadline(nextReadDeadline)
			}
		}
		nbytes, readErr := input.Read(buffer)
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, net.ErrClosed) {
			p.log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(readErr).Msg("Error reading from pipe")
			return
		}

		if nbytes > 0 {
			inCounter.Add(float64(nbytes))
			if outputTimeout != 0 {
				now := time.Now()
				if nextWriteDeadline.IsZero() || now.Add(outputTimeout).After(nextWriteDeadline) {
					nextWriteDeadline = now.Add(outputTimeout + p.timeoutMargin)
					output.SetWriteDeadline(nextWriteDeadline)
				}
			}
			nbytes, writeErr := output.Write(buffer[:nbytes])
			if nbytes > 0 {
				outCounter.Add(float64(nbytes))
			}
			if writeErr != nil {
				if !errors.Is(writeErr, net.ErrClosed) {
					p.log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(writeErr).Msg("Error writing to pipe")
				}
				return
			}
		}

		if readErr != nil {
			return
		}
	}
}

func (p *ProxyTCP) getBackendMetrics(backendAddress string) *Metrics {
	p.beMetricsMutex.Lock()
	defer p.beMetricsMutex.Unlock()

	beM, exists := p.beMetricsCache[backendAddress]
	if !exists {
		beM = &Metrics{
			processed: metrics.BeCnxProcessed.WithLabelValues(backendAddress, p.id),
			active:    metrics.BeActCnx.WithLabelValues(backendAddress, p.id),
			bytesIn:   metrics.BeBytesIn.WithLabelValues(backendAddress, p.id),
			bytesOut:  metrics.BeBytesOut.WithLabelValues(backendAddress, p.id),
		}
		p.beMetricsCache[backendAddress] = beM
	}
	return beM
}

func (p *ProxyTCP) handleConnection(connFront net.Conn, feMetrics *Metrics) {
	peerAddress := connFront.RemoteAddr().String()

	defer p.connectionsWG.Done()

	done := make(chan struct{})
	var closeOnce sync.Once

	closeConn := func() {
		closeOnce.Do(func() {
			p.log.Debug().Str("peer", peerAddress).Msg("Closing Frontend connection")
			close(done)
			err := connFront.Close()
			if err != nil && !errors.Is(err, net.ErrClosed) {
				p.log.Error().Err(err).Str("peer", peerAddress).Msg("Error while closing frontend connection")
			}
		})
	}
	defer closeConn()

	// If the proxy context is closed, close the connection after a grace period
	stopGracefulClosing := context.AfterFunc(p.ctx, func() {
		p.log.Debug().Str("peer", peerAddress).Msg("Frontend closed, waiting for connection to end.")
		timer := time.NewTimer(p.closeTimeout)
		defer timer.Stop()

		select {
		case <-done:
			// Connection closed normally
			return
		case <-timer.C:
			p.log.Warn().Str("peer", peerAddress).Msg("Timeout reached, force closing connection.")
			closeConn()
		}
	})
	defer stopGracefulClosing()

	// Prometheus
	feMetrics.processed.Inc()
	feMetrics.active.Inc()
	defer feMetrics.active.Dec()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("peer", peerAddress).Interface("error", r).Msg("Error while processing connection")
			// Prometheus
			feMetrics.cnxErrors.Inc()
		}
	}()

	// Try to get a backend from any provider without waiting
	var backend *backend.Backend
	var release func()
	for _, provider := range p.backendProviders {
		backend, release = provider.GetBackend(false)
		if backend != nil {
			break
		}
	}

	// If still no backend try waiting for the first provider
	if backend == nil {
		if len(p.backendProviders) > 0 {
			// We only wait on the first provider for simplicity.
			// Upgrade path: wait on multiple providers or implement a more complex fallback strategy.
			backend, release = p.backendProviders[0].GetBackend(true)
		} else {
			panic(errors.New("no backend provider configured"))
		}
	}
	defer release()

	var backendAddress string
	if backend != nil {
		backendAddress = backend.Address
	} else {
		panic(errors.New("no backend found"))
	}

	if p.closeOnBackendRemoval && backend.Ctx != nil {
		stopReset := context.AfterFunc(backend.Ctx, func() {
			select {
			case <-done:
				// Connection already closed
			default:
				p.log.Debug().Str("peer", peerAddress).Msg("Backend removed from balancer, closing connection")
				closeConn()
			}
		})
		defer stopReset()
	}

	beMetrics := p.getBackendMetrics(backendAddress)

	// Prometheus
	beMetrics.processed.Inc()

	// Open backend connection
	p.log.Debug().Str("peer", backendAddress).Msg("Opening Backend connection")
	connBack, err := net.DialTimeout("tcp", backendAddress, p.connectTimeout)
	if err != nil {
		panic(err)
	}

	// Prometheus
	beMetrics.active.Inc()
	defer beMetrics.active.Dec()

	defer connBack.Close()
	defer p.log.Debug().Str("peer", backendAddress).Msg("Closing Backend connection")

	// Pipe the connections both ways
	doneFrontBack := make(chan struct{})
	doneBackFront := make(chan struct{})

	go p.pipe(connFront, connBack, doneFrontBack, p.clientTimeout, p.serverTimeout, feMetrics.bytesIn, beMetrics.bytesOut)
	go p.pipe(connBack, connFront, doneBackFront, p.serverTimeout, p.clientTimeout, beMetrics.bytesIn, feMetrics.bytesOut)

	// Wait for one pipe to end or the context to be cancelled
	select {
	case <-doneFrontBack:
	case <-doneBackFront:
	case <-done:
	}

	// Ensure both ends are closed so both pipes will exit
	connFront.Close()
	connBack.Close()
}

func (p *ProxyTCP) Bind(modules module.ModulesRegistry) error {
	for _, source := range p.sources {
		provider, err := module.Get[backend.BackendProvider](modules, source)
		if err != nil {
			return err
		}
		p.backendProviders = append(p.backendProviders, provider)
	}

	// Listening to incoming connections only makes sense after backend providers are available
	for _, v := range p.addresses {
		if err := p.listen(v, p.wg); err != nil {
			return err
		}
	}
	return nil
}
