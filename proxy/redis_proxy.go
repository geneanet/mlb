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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sys/unix"
)

func init() {
	factories["redis"] = &RedisProxyFactory{}
}

type RedisProxy struct {
	id                        string
	addresses                 []string
	source                    string
	close_timeout             time.Duration
	connect_timeout           time.Duration
	backend_wait_timeout      time.Duration
	connections_wg            sync.WaitGroup
	ctx                       context.Context
	cancel                    context.CancelFunc
	log                       zerolog.Logger
	wg                        *sync.WaitGroup
	backendUpdatesChan        chan backend.BackendUpdate
	backendUpdatesChanClosed  chan struct{}
	backends                  *backend.BackendsMap
	buffer_size               int
	backendConnectionPool     *RedisBackendConnectionPool
	clientQueueSize           int
	backendInflightQueueSize  int
	backendConnectionPoolSize int
	retryPeriod               time.Duration
	retryMaxPeriod            time.Duration
	retryBackoffFactor        float64
}

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

type RedisProxyFactory struct{}

func (f RedisProxyFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &RedisProxyConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (f RedisProxyFactory) parseConfig(tc *Config) *RedisProxyConfig {
	config := &RedisProxyConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("proxy.%s.%s", tc.Type, tc.Name)
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

func (f RedisProxyFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := f.parseConfig(tc)

	p := &RedisProxy{
		id:                        config.ID,
		addresses:                 config.Addresses,
		log:                       log.With().Str("id", config.ID).Logger(),
		buffer_size:               config.BufferSize,
		source:                    config.Source,
		clientQueueSize:           config.ClientQueueSize,
		backendInflightQueueSize:  config.BackendInflightQueueSize,
		backendConnectionPoolSize: config.BackendConnectionPoolSize,
		wg:                        wg,
		backendUpdatesChan:        make(chan backend.BackendUpdate),
		backendUpdatesChanClosed:  make(chan struct{}),
		backends:                  backend.NewBackendsMap(),
	}

	var err error

	p.connect_timeout, err = time.ParseDuration(config.ConnectTimeout)
	misc.PanicIfErr(err)
	p.close_timeout, err = time.ParseDuration(config.CloseTimeout)
	misc.PanicIfErr(err)
	p.backend_wait_timeout, err = time.ParseDuration(config.BackendWaitTimeout)
	misc.PanicIfErr(err)
	p.retryPeriod, err = time.ParseDuration(config.RetryPeriod)
	misc.PanicIfErr(err)
	p.retryMaxPeriod, err = time.ParseDuration(config.RetryMaxPeriod)
	misc.PanicIfErr(err)

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
				p.backendConnectionPool.Update()

			case <-p.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return p
}

func (p *RedisProxy) listen(address string, wg *sync.WaitGroup, ctx context.Context) {
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

func (p *RedisProxy) handle_connection(conn_front net.Conn) {
	frontend_address := conn_front.LocalAddr().String()
	peer_address := conn_front.RemoteAddr().String()

	defer p.connections_wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// If the connection context is closed, close the connection
	go func() {
		<-ctx.Done()
		p.log.Debug().Str("peer", peer_address).Msg("Closing Frontend connection")
		err := conn_front.Close()
		misc.PanicIfErr(err)
	}()

	// If the proxy context is closed, close the connection context after a grace period
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

	// Set TCPNoDelay
	err := conn_front.(*net.TCPConn).SetNoDelay(true)
	misc.PanicIfErr(err)

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

	// Get Backend Connection
	backendConnection := p.backendConnectionPool.GetRandom(true)
	if backendConnection == nil {
		panic("No backend found")
	}

	// Read response queue and write responses
	response_chan := make(chan RedisReponse, p.clientQueueSize)
	response_chan_stop := make(chan struct{})
	defer close(response_chan_stop) // Ensure no backend will block trying to send replies if the client connection is closed
	go func() {
		for {
			select {
			case response, ok := <-response_chan:
				if ok {
					if response.item != nil {
						p.log.Debug().Uint64("query_id", response.query.id).Msg("Received valid response")
						conn_front.Write(response.item)
					} else {
						p.log.Debug().Uint64("query_id", response.query.id).Msg("Received failed response")
						cancel()
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Read queries
	front_reader := NewRedisProtocolReader(conn_front, p.buffer_size)

	for {
		item, err := front_reader.ReadMessage(true)
		if err == io.EOF || errors.Is(err, net.ErrClosed) {
			return
		}
		misc.PanicIfErr(err)

		query := NewRedisQuery(item, response_chan, response_chan_stop)
		p.log.Debug().Uint64("query_id", query.id).Msg("Received query")

		if query.IsAllowed() {
			// Add the query to the queue
			err := backendConnection.Query(query)

			if err != nil {
				p.log.Debug().Uint64("query_id", query.id).Msg("Backend has failed, picking a new one")
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

func (p *RedisProxy) GetID() string {
	return p.id
}

func (p *RedisProxy) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case p.backendUpdatesChan <- upd:
	case <-p.backendUpdatesChanClosed:
	}
}

func (p *RedisProxy) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(p)
}

func (p *RedisProxy) GetUpdateSource() string {
	return p.source
}

func (p *RedisProxy) Bind(modules module.ModulesList) {
	p.SubscribeTo(modules.GetBackendUpdateProvider(p.source))

	// Listening to incoming connections only makes sense after backend providers are available
	for _, v := range p.addresses {
		p.listen(v, p.wg, p.ctx)
	}
}
