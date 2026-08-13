package redis

import (
	"context"
	"errors"
	"io"
	"mlb/backend"
	"mlb/config"
	"mlb/metrics"
	"mlb/module"
	"mlb/system"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	module.RegisterFactory("proxy", "redis", newRedisProxy, validateRedisProxyConfig)
}

// RedisProxy implements a Redis protocol proxy.
type RedisProxy struct {
	id                       string
	addresses                []string
	source                   string
	closeTimeout             time.Duration
	connectTimeout           time.Duration
	backendWaitTimeout       time.Duration
	backendTCPKeepAlive      time.Duration
	connectionsWG            sync.WaitGroup
	ctx                      context.Context
	cancel                   context.CancelFunc
	log                      zerolog.Logger
	wg                       *sync.WaitGroup
	backendUpdatesChan       chan backend.BackendUpdate
	backendUpdatesChanClosed chan struct{}
	backends                 *backend.Registry
	bufferSize               int
	backendConnectionPool    *RedisBackendConnectionPool
	clientQueueSize          int
	backendInputQueueSize    int
	preconnect               int
	idleTimeout              time.Duration
	healthcheck              bool
	beMetricsCache           map[string]*Metrics
	beMetricsMutex           sync.RWMutex
}

// Metrics holds Prometheus metrics for a specific backend or frontend.
type Metrics struct {
	processed prometheus.Counter
	active    prometheus.Gauge
	bytesIn   prometheus.Counter
	bytesOut  prometheus.Counter
	cnxErrors prometheus.Counter
	requests  prometheus.Counter
}

// RedisProxyConfig defines the HCL configuration for the Redis proxy.
type RedisProxyConfig struct {
	ID                    string   `hcl:"id,label"`
	Source                string   `hcl:"source"`
	Addresses             []string `hcl:"addresses,optional"`
	ConnectTimeout        string   `hcl:"connect_timeout,optional"`
	CloseTimeout          string   `hcl:"close_timeout,optional"`
	BackendWaitTimeout    string   `hcl:"backend_wait_timeout,optional"`
	BackendTCPKeepAlive   string   `hcl:"backend_tcp_keepalive,optional"`
	BufferSize            int      `hcl:"buffer_size,optional"`
	ClientQueueSize       int      `hcl:"client_queue_size,optional"`
	BackendInputQueueSize int      `hcl:"backend_input_queue_size,optional"`
	Preconnect            int      `hcl:"preconnect,optional"`
	IdleTimeout           string   `hcl:"idle_timeout,optional"`
	Healthcheck           bool     `hcl:"healthcheck,optional"`
}

// validateRedisProxyConfig validates the Redis proxy configuration.
func validateRedisProxyConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &RedisProxyConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.CloseTimeout, "close_timeout")
	config.CheckDuration(&diags, configBody.BackendWaitTimeout, "backend_wait_timeout")
	config.CheckDuration(&diags, configBody.BackendTCPKeepAlive, "backend_tcp_keepalive")
	config.CheckDuration(&diags, configBody.IdleTimeout, "idle_timeout")

	return diags
}

// parseRedisProxyConfig parses the Redis proxy configuration.
func parseRedisProxyConfig(tc *module.Config) *RedisProxyConfig {
	config := &RedisProxyConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode Redis proxy config")
	}
	config.ID = tc.FullID()
	if config.ConnectTimeout == "" {
		config.ConnectTimeout = "0s"
	}
	if config.CloseTimeout == "" {
		config.CloseTimeout = "0s"
	}
	if config.BackendWaitTimeout == "" {
		config.BackendWaitTimeout = "0s"
	}
	if config.BackendTCPKeepAlive == "" {
		config.BackendTCPKeepAlive = "5s"
	}
	if config.BufferSize == 0 {
		config.BufferSize = 16384
	}
	if config.ClientQueueSize == 0 {
		config.ClientQueueSize = 64
	}
	if config.BackendInputQueueSize == 0 {
		config.BackendInputQueueSize = 1024
	}
	if config.IdleTimeout == "" {
		config.IdleTimeout = "5m"
	}
	return config
}

func newRedisProxy(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseRedisProxyConfig(tc)

	p := &RedisProxy{
		id:                       config.ID,
		addresses:                config.Addresses,
		log:                      log.With().Str("id", config.ID).Logger(),
		bufferSize:               config.BufferSize,
		source:                   config.Source,
		clientQueueSize:          config.ClientQueueSize,
		backendInputQueueSize:    config.BackendInputQueueSize,
		preconnect:               config.Preconnect,
		healthcheck:              config.Healthcheck,
		wg:                       wg,
		backendUpdatesChan:       make(chan backend.BackendUpdate, 100),
		backendUpdatesChanClosed: make(chan struct{}),
		backends:                 backend.NewRegistry(),
		beMetricsCache:           make(map[string]*Metrics),
	}

	var err error

	p.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	p.closeTimeout, err = time.ParseDuration(config.CloseTimeout)
	if err != nil {
		return nil, err
	}
	p.backendWaitTimeout, err = time.ParseDuration(config.BackendWaitTimeout)
	if err != nil {
		return nil, err
	}
	p.backendTCPKeepAlive, err = time.ParseDuration(config.BackendTCPKeepAlive)
	if err != nil {
		return nil, err
	}
	p.idleTimeout, err = time.ParseDuration(config.IdleTimeout)
	if err != nil {
		return nil, err
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.beMetricsCache = make(map[string]*Metrics)
	p.backendConnectionPool = NewRedisBackendConnectionPool(p)

	wg.Add(1)
	p.log.Info().Msg("Redis proxy starting")

	go func() {
		defer wg.Done()
		defer p.log.Info().Msg("Redis proxy stopped")
		defer p.cancel()
		defer close(p.backendUpdatesChanClosed)

	mainloop:
		for {
			select {
			case upd := <-p.backendUpdatesChan: // Backend changed
				switch upd.Kind {
				case backend.UpdBackendAdded:
					p.backends.Add(upd.Backend.Clone())
				case backend.UpdBackendModified:
					p.backends.Update(upd.Backend.Clone())
				case backend.UpdBackendRemoved:
					p.backends.Remove(upd.Address)
				}
				go p.backendConnectionPool.Update()

			case <-p.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return p, nil
}

func (p *RedisProxy) listen(address string, wg *sync.WaitGroup) error {
	p.log.Info().Str("address", address).Msg("Opening Frontend")

	feMetrics := &Metrics{
		processed: metrics.FeCnxProcessed.WithLabelValues(address, p.id),
		active:    metrics.FeActCnx.WithLabelValues(address, p.id),
		bytesIn:   metrics.FeBytesIn.WithLabelValues(address, p.id),
		bytesOut:  metrics.FeBytesOut.WithLabelValues(address, p.id),
		cnxErrors: metrics.FeCnxErrors.WithLabelValues(address, p.id),
		requests:  metrics.FeRequests.WithLabelValues(address, p.id),
	}

	// Bind
	listener, err := system.Listen("tcp", address)
	if err != nil {
		return err
	}

	context.AfterFunc(p.ctx, func() {
		p.log.Debug().Str("address", address).Msg("Closing Frontend listener")
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
				p.log.Error().Err(err).Str("address", address).Msg("Error accepting connection")
				continue
			}
			p.connectionsWG.Add(1)
			go p.handleConnection(conn, feMetrics)
		}

		p.log.Debug().Str("address", address).Msg("Waiting for active connections to close")
		p.connectionsWG.Wait()
	}()

	return nil
}

func (p *RedisProxy) pipe(input net.Conn, output net.Conn, done chan struct{}, stop chan struct{}, bufferSize int, inMetrics *Metrics, outMetrics *Metrics) {
	// Signal completion
	defer close(done)

	// Recover from unexpected panics to prevent proxy crashes
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Stringer("input", input.RemoteAddr()).Stringer("output", output.RemoteAddr()).Interface("error", r).Msg("Error while processing pipe")
		}
	}()

	reader := NewRedisProtocolReader(input, bufferSize)
	defer reader.Release()
	writer := NewRedisProtocolWriter(output, bufferSize)
	defer writer.Release()

	for {
		item, readErr := reader.ReadMessage(true)
		if readErr != nil {
			select {
			case <-stop:
				// Silence read errors when we are stopping
				ReleaseBuffer(item)
				return
			default:
			}

			if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, net.ErrClosed) {
				p.log.Error().Stringer("input", input.RemoteAddr()).Stringer("output", output.RemoteAddr()).Err(readErr).Msg("Error reading Redis message from pipe")
			}
			ReleaseBuffer(item)
			return
		}
		inMetrics.requests.Inc()
		inMetrics.bytesIn.Add(float64(len(item)))

		nbytes, writeErr := writer.Write(item)
		if writeErr == nil {
			writeErr = writer.Flush()
		}
		if writeErr != nil {
			select {
			case <-stop:
				// Silence write errors when we are stopping
				ReleaseBuffer(item)
				return
			default:
			}

			if !errors.Is(writeErr, net.ErrClosed) {
				p.log.Error().Stringer("input", input.RemoteAddr()).Stringer("output", output.RemoteAddr()).Err(writeErr).Msg("Error writing Redis message to pipe")
			}
			ReleaseBuffer(item)
			return
		}
		outMetrics.bytesOut.Add(float64(nbytes))

		ReleaseBuffer(item)
	}
}

func (p *RedisProxy) handleConnection(connFront net.Conn, feMetrics *Metrics) {
	peerAddress := connFront.RemoteAddr()

	defer p.connectionsWG.Done()

	done := make(chan struct{})
	var closeOnce sync.Once

	closeConn := func() {
		closeOnce.Do(func() {
			p.log.Debug().Stringer("peer", peerAddress).Msg("Closing Frontend connection")
			close(done)
			err := connFront.Close()
			if err != nil && !errors.Is(err, net.ErrClosed) {
				p.log.Error().Err(err).Stringer("peer", peerAddress).Msg("Error while closing frontend connection")
			}
		})
	}
	defer closeConn()

	// If the proxy context is closed, close the connection context after a grace period
	stopGracefulClosing := context.AfterFunc(p.ctx, func() {
		p.log.Debug().Stringer("peer", peerAddress).Msg("Frontend closed, waiting for connection to end.")
		timer := time.NewTimer(p.closeTimeout)
		defer timer.Stop()

		select {
		case <-done:
			// Connection closed normally
			return
		case <-timer.C:
			p.log.Warn().Stringer("peer", peerAddress).Msg("Frontend close timeout reached, closing connection")
			closeConn()
		}
	})
	defer stopGracefulClosing()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Stringer("peer", peerAddress).Interface("error", r).Msg("Error while processing connection")
			// Prometheus
			feMetrics.cnxErrors.Inc()
		}
	}()

	// Prometheus
	feMetrics.processed.Inc()
	feMetrics.active.Inc()
	defer feMetrics.active.Dec()

	// Get Backend Connection (pinned for the duration of frontend connection)
	p.log.Debug().Stringer("peer", peerAddress).Msg("Getting backend connection from pool")
	backendConnection, err := p.backendConnectionPool.Get(p.ctx)
	if err != nil {
		p.log.Error().Err(err).Stringer("peer", peerAddress).Msg("No backend found")
		return
	}
	p.log.Debug().Stringer("peer", peerAddress).Str("backend", backendConnection.backend.Address).Msg("Using backend connection")
	defer backendConnection.ResetAndRelease()
	beMetrics := backendConnection.metrics

	// Pipe the connections both ways
	doneFrontBack := make(chan struct{})
	doneBackFront := make(chan struct{})
	stopPipes := make(chan struct{})

	go p.pipe(connFront, backendConnection.conn, doneFrontBack, stopPipes, p.bufferSize, feMetrics, beMetrics)
	go p.pipe(backendConnection.conn, connFront, doneBackFront, stopPipes, p.bufferSize, beMetrics, feMetrics)

	// Wait for one pipe to end or the context to be cancelled
	select {
	case <-doneFrontBack:
	case <-doneBackFront:
	case <-done:
	}

	// Safely stop pipes before returning the backend connection to the pool.
	// We close the stop channel and the frontend connection, and set a deadline
	// on the backend connection to unblock any pending Read calls in the pipe.
	close(stopPipes)
	closeConn()
	backendConnection.conn.SetReadDeadline(time.Now())

	// Wait for pipes to finish
	<-doneFrontBack
	<-doneBackFront

	// Reset deadline for future use
	backendConnection.conn.SetReadDeadline(time.Time{})
}

func (p *RedisProxy) getBackendMetrics(backendAddress string) *Metrics {
	p.beMetricsMutex.Lock()
	defer p.beMetricsMutex.Unlock()

	beM, exists := p.beMetricsCache[backendAddress]
	if !exists {
		beM = &Metrics{
			processed: metrics.BeCnxProcessed.WithLabelValues(backendAddress, p.id),
			active:    metrics.BeActCnx.WithLabelValues(backendAddress, p.id),
			bytesIn:   metrics.BeBytesIn.WithLabelValues(backendAddress, p.id),
			bytesOut:  metrics.BeBytesOut.WithLabelValues(backendAddress, p.id),
			requests:  metrics.BeRequests.WithLabelValues(backendAddress, p.id),
		}
		p.beMetricsCache[backendAddress] = beM
	}
	return beM
}

func (p *RedisProxy) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case p.backendUpdatesChan <- upd:
	case <-p.backendUpdatesChanClosed:
	}
}

func (p *RedisProxy) Bind(modules module.ModulesRegistry) error {
	m, err := module.Get[backend.BackendUpdateProvider](modules, p.source)
	if err != nil {
		return err
	}
	m.ProvideUpdates(p)

	// Listening to incoming connections only makes sense after backend providers are available
	for _, v := range p.addresses {
		if err := p.listen(v, p.wg); err != nil {
			return err
		}
	}
	return nil
}
