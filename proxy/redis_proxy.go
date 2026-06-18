package proxy

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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

func init() {
	module.RegisterFactory("proxy", "redis", newRedisProxy, validateRedisProxyConfig)
}

// RedisProxy implements a Redis protocol proxy.
type RedisProxy struct {
	id                        string
	addresses                 []string
	source                    string
	closeTimeout              time.Duration
	connectTimeout            time.Duration
	backendWaitTimeout        time.Duration
	connectionsWG             sync.WaitGroup
	ctx                       context.Context
	cancel                    context.CancelFunc
	log                       zerolog.Logger
	wg                        *sync.WaitGroup
	backendUpdatesChan        chan backend.BackendUpdate
	backendUpdatesChanClosed  chan struct{}
	backends                  *backend.Registry
	bufferSize                int
	backendConnectionPool     *RedisBackendConnectionPool
	clientQueueSize           int
	backendInflightQueueSize  int
	backendConnectionPoolSize int
	retryPeriod               time.Duration
	retryMaxPeriod            time.Duration
	retryBackoffFactor        float64
}

// RedisProxyConfig defines the HCL configuration for the Redis proxy.
type RedisProxyConfig struct {
	ID                        string   `hcl:"id,label"`
	Source                    string   `hcl:"source"`
	Addresses                 []string `hcl:"addresses,optional"`
	ConnectTimeout            string   `hcl:"connect_timeout,optional"`
	CloseTimeout              string   `hcl:"close_timeout,optional"`
	BackendWaitTimeout        string   `hcl:"backend_wait_timeout,optional"`
	BufferSize                int      `hcl:"buffer_size,optional"`
	ClientQueueSize           int      `hcl:"client_queue_size,optional"`
	BackendInflightQueueSize  int      `hcl:"backend_inflight_queue_size,optional"`
	BackendConnectionPoolSize int      `hcl:"backend_connection_pool_size,optional"`
	RetryPeriod               string   `hcl:"retry_period,optional"`
	RetryMaxPeriod            string   `hcl:"retry_max_period,optional"`
	RetryBackoffFactor        float64  `hcl:"retry_backoff_factor,optional"`
}

// validateRedisProxyConfig validates the Redis proxy configuration.
func validateRedisProxyConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &RedisProxyConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.CloseTimeout, "close_timeout")
	config.CheckDuration(&diags, configBody.BackendWaitTimeout, "backend_wait_timeout")
	config.CheckDuration(&diags, configBody.RetryPeriod, "retry_period")
	config.CheckDuration(&diags, configBody.RetryMaxPeriod, "retry_max_period")

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
	if config.BufferSize == 0 {
		config.BufferSize = 16384
	}
	if config.ClientQueueSize == 0 {
		config.ClientQueueSize = 64
	}
	if config.BackendInflightQueueSize == 0 {
		config.BackendInflightQueueSize = 512
	}
	if config.BackendConnectionPoolSize == 0 {
		config.BackendConnectionPoolSize = 1
	}
	if config.RetryPeriod == "" {
		config.RetryPeriod = "100ms"
	}
	if config.RetryMaxPeriod == "" {
		config.RetryMaxPeriod = "1s"
	}
	if config.RetryBackoffFactor == 0 {
		config.RetryBackoffFactor = 1.5
	}
	return config
}

func newRedisProxy(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) any {
	config := parseRedisProxyConfig(tc)

	p := &RedisProxy{
		id:                        config.ID,
		addresses:                 config.Addresses,
		log:                       log.With().Str("id", config.ID).Logger(),
		bufferSize:                config.BufferSize,
		source:                    config.Source,
		clientQueueSize:           config.ClientQueueSize,
		backendInflightQueueSize:  config.BackendInflightQueueSize,
		backendConnectionPoolSize: config.BackendConnectionPoolSize,
		wg:                        wg,
		backendUpdatesChan:        make(chan backend.BackendUpdate, 100),
		backendUpdatesChanClosed:  make(chan struct{}),
		backends:                  backend.NewRegistry(),
	}

	var err error

	p.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	if err != nil {
		panic(err)
	}
	p.closeTimeout, err = time.ParseDuration(config.CloseTimeout)
	if err != nil {
		panic(err)
	}
	p.backendWaitTimeout, err = time.ParseDuration(config.BackendWaitTimeout)
	if err != nil {
		panic(err)
	}
	p.retryPeriod, err = time.ParseDuration(config.RetryPeriod)
	if err != nil {
		panic(err)
	}
	p.retryMaxPeriod, err = time.ParseDuration(config.RetryMaxPeriod)
	if err != nil {
		panic(err)
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

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

	return p
}

func (p *RedisProxy) listen(address string, wg *sync.WaitGroup) {
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
	if err != nil {
		panic(err)
	}

	context.AfterFunc(p.ctx, func() {
		err := listener.Close()
		if err != nil {
			panic(err)
		}
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
			go p.handleConnection(conn)
		}

		p.connectionsWG.Wait()
	}()
}

func (p *RedisProxy) handleConnection(connFront net.Conn) {
	frontendAddress := connFront.LocalAddr().String()
	peerAddress := connFront.RemoteAddr().String()

	defer p.connectionsWG.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If the connection context is closed, close the connection
	context.AfterFunc(ctx, func() {
		p.log.Debug().Str("peer", peerAddress).Msg("Closing Frontend connection")
		err := connFront.Close()
		if err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	})

	// If the proxy context is closed, close the connection context after a grace period
	stopGracefulClosing := context.AfterFunc(p.ctx, func() {
		p.log.Debug().Str("peer", peerAddress).Msg("Frontend closed, waiting for connection to end.")
		timer := time.AfterFunc(p.closeTimeout, func() {
			p.log.Debug().Str("peer", peerAddress).Msg("Frontend close timeout reached, closing connection")
			cancel()
		})
		// Ensure we stop the timer if the connection finishes before the timeout
		context.AfterFunc(ctx, func() {
			timer.Stop()
		})
	})
	defer stopGracefulClosing()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			p.log.Error().Str("peer", peerAddress).Interface("error", r).Msg("Error while processing connection")
			// Prometheus
			metrics.FeCnxErrors.WithLabelValues(frontendAddress, p.id).Inc()
		}
	}()

	// Set TCPNoDelay
	err := connFront.(*net.TCPConn).SetNoDelay(true)
	if err != nil {
		panic(err)
	}

	// Prometheus
	metrics.FeCnxProcessed.WithLabelValues(frontendAddress, p.id).Inc()
	metrics.FeActCnx.WithLabelValues(frontendAddress, p.id).Inc()
	defer metrics.FeActCnx.WithLabelValues(frontendAddress, p.id).Dec()

	// Get Backend Connection
	backendConnection := p.backendConnectionPool.GetRandom(true)
	if backendConnection == nil {
		panic("No backend found")
	}

	// Read response queue and write responses
	responseChan := make(chan RedisReponse, p.clientQueueSize)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop) // Ensure no backend will block trying to send replies if the client connection is closed
	go func() {
		for {
			select {
			case response := <-responseChan:
				if response.item != nil {
					p.log.Debug().Uint64("queryId", response.query.id).Msg("Received valid response")
					_, err := connFront.Write(response.item)
					if err != nil {
						p.log.Error().Err(err).Str("peer", peerAddress).Msg("Unexpected error while writing to client")
						cancel()
					}
				} else {
					p.log.Debug().Uint64("queryId", response.query.id).Msg("Received failed response")
					cancel()
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Read queries
	frontReader := NewRedisProtocolReader(connFront, p.bufferSize)
	defer frontReader.Release()

	for {
		item, err := frontReader.ReadMessage(true)
		if err == io.EOF || errors.Is(err, net.ErrClosed) {
			return
		} else if err != nil {
			panic("Unexpected error while reading from the client")
		}

		query := NewRedisQuery(item, responseChan, responseChanStop)
		p.log.Debug().Uint64("queryId", query.id).Msg("Received query")

		if !query.IsRestricted() {
			// Add the query to the queue
			err := backendConnection.Query(query)

			if err != nil {
				p.log.Warn().Uint64("queryId", query.id).Msg("Backend has failed, picking a new one")
				backendConnection = p.backendConnectionPool.GetRandom(true)
				if backendConnection == nil {
					panic("No backend found")
				}
				err = backendConnection.Query(query)
				if err != nil {
					panic("Unable to forward the query to the backend")
				}
			}
		} else {
			// Send an error
			query.Reply([]byte("-DENIED Command not supported by MLB Redis proxy\r\n"))
		}
	}
}


func (p *RedisProxy) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case p.backendUpdatesChan <- upd:
	case <-p.backendUpdatesChanClosed:
	}
}

func (p *RedisProxy) Bind(modules module.ModulesRegistry) {
	module.Get[backend.BackendUpdateProvider](modules, p.source).ProvideUpdates(p)

	// Listening to incoming connections only makes sense after backend providers are available
	for _, v := range p.addresses {
		p.listen(v, p.wg)
	}
}
