package memcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"mlb/backend"
	"mlb/config"
	"mlb/metrics"
	"mlb/module"
	"mlb/util"
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
	module.RegisterFactory("proxy", "memcache", newMemcacheProxy, validateMemcacheProxyConfig)
}

// MemcacheProxyConfig defines the HCL configuration for the Memcache proxy.
type MemcacheProxyConfig struct {
	ID                       string   `hcl:"id,label"`
	Source                   string   `hcl:"source"`
	Addresses                []string `hcl:"addresses,optional"`
	ConnectTimeout           string   `hcl:"connect_timeout,optional"`
	CloseTimeout             string   `hcl:"close_timeout,optional"`
	BufferSize               int      `hcl:"buffer_size,optional"`
	ClientQueueSize          int      `hcl:"client_queue_size,optional"`
	BackendInputQueueSize    int      `hcl:"backend_input_queue_size,optional"`
	BackendInflightQueueSize int      `hcl:"backend_inflight_queue_size,optional"`
	BackendMinConnections    int      `hcl:"backend_min_connections,optional"`
	BackendMaxConnections    int      `hcl:"backend_max_connections,optional"`
	BackendTCPKeepAlive      string   `hcl:"backend_tcp_keepalive,optional"`
	MaxFieldsPerCommand      int      `hcl:"max_fields_per_command,optional"`
	FlushBackendWhenAdded    bool     `hcl:"flush_backend_when_added,optional"`
}

// MemcacheProxy implements a Memcache-compatible proxy with consistent hashing support.
// It handles client connections, parses the Memcache protocol, and routes requests
// to backends based on the key using Ketama consistent hashing.
type MemcacheProxy struct {
	id             string
	source         string
	addresses      []string
	closeTimeout   time.Duration
	connectTimeout time.Duration

	connectionsWG sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	log    zerolog.Logger
	wg     *sync.WaitGroup

	backends                 *backend.Registry
	ring                     *memcacheHashRing
	backendUpdatesChan       chan backend.BackendUpdate
	backendUpdatesChanClosed chan struct{}
	backendConnectionPool    *MemcacheBackendConnectionPool

	bufferSize               int
	clientQueueSize          int
	backendInputQueueSize    int
	backendInflightQueueSize int
	backendMinConnections    int
	backendMaxConnections    int
	backendTCPKeepAlive      time.Duration
	flushBackendWhenAdded    bool
	fieldsPool               *sync.Pool
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

// validateMemcacheProxyConfig validates the Memcache proxy configuration.
func validateMemcacheProxyConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &MemcacheProxyConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)
	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.CloseTimeout, "close_timeout")
	config.CheckDuration(&diags, configBody.BackendTCPKeepAlive, "backend_tcp_keepalive")
	if configBody.BackendMinConnections > 0 && configBody.BackendMaxConnections > 0 && configBody.BackendMaxConnections < configBody.BackendMinConnections {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Invalid connection pool configuration",
			Detail:   "backend_max_connections must be greater than or equal to backend_min_connections",
		})
	}
	return diags
}

// parseMemcacheProxyConfig parses the Memcache proxy configuration.
func parseMemcacheProxyConfig(tc *module.Config) *MemcacheProxyConfig {
	config := &MemcacheProxyConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode Memcache proxy config")
	}
	config.ID = tc.FullID()
	if config.ConnectTimeout == "" {
		config.ConnectTimeout = "0s"
	}
	if config.CloseTimeout == "" {
		config.CloseTimeout = "0s"
	}
	if config.BackendTCPKeepAlive == "" {
		config.BackendTCPKeepAlive = "15s"
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
	if config.BackendInflightQueueSize == 0 {
		config.BackendInflightQueueSize = 512
	}
	if config.BackendMinConnections == 0 {
		config.BackendMinConnections = 1
	}
	if config.BackendMaxConnections == 0 {
		config.BackendMaxConnections = config.BackendMinConnections
	}
	if config.BackendMaxConnections < config.BackendMinConnections {
		config.BackendMaxConnections = config.BackendMinConnections
	}
	if config.MaxFieldsPerCommand == 0 {
		config.MaxFieldsPerCommand = 16
	}
	return config
}

func newMemcacheProxy(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseMemcacheProxyConfig(tc)

	p := &MemcacheProxy{
		id:                       config.ID,
		source:                   config.Source,
		addresses:                config.Addresses,
		bufferSize:               config.BufferSize,
		clientQueueSize:          config.ClientQueueSize,
		backendInputQueueSize:    config.BackendInputQueueSize,
		backendInflightQueueSize: config.BackendInflightQueueSize,
		backendMinConnections:    config.BackendMinConnections,
		backendMaxConnections:    config.BackendMaxConnections,
		flushBackendWhenAdded:    config.FlushBackendWhenAdded,
		beMetricsCache:           make(map[string]*Metrics),
		log:                      log.With().Str("id", config.ID).Logger(),
		wg:                       wg,
		backends:                 backend.NewRegistry(),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 100),
		backendUpdatesChanClosed: make(chan struct{}),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, config.MaxFieldsPerCommand)
				return &f
			},
		},
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
	p.backendTCPKeepAlive, err = time.ParseDuration(config.BackendTCPKeepAlive)
	if err != nil {
		return nil, err
	}

	p.ctx, p.cancel = context.WithCancel(ctx)

	p.backendConnectionPool = NewMemcacheBackendConnectionPool(p)

	wg.Add(1)
	p.log.Info().Msg("Memcache proxy starting")

	// Background worker to handle backend updates and update the hash ring
	go func() {
		defer wg.Done()
		defer p.log.Info().Msg("Memcache proxy stopped")
		defer p.cancel()
		defer close(p.backendUpdatesChanClosed)

	mainloop:
		for {
			select {
			case upd := <-p.backendUpdatesChan:
				switch upd.Kind {
				case backend.UpdBackendAdded:
					if p.flushBackendWhenAdded {
						p.flushBackend(upd.Backend)
					}
					p.backends.Add(upd.Backend.Clone())
				case backend.UpdBackendModified:
					p.backends.Add(upd.Backend.Clone())
				case backend.UpdBackendRemoved:
					p.backends.Remove(upd.Address)
				}
				p.ring.update(p.backends.GetList())
				go p.backendConnectionPool.Update()

			case <-p.ctx.Done():
				break mainloop
			}
		}
	}()

	return p, nil
}

// Bind initializes the backend update subscription and starts the listeners.
func (p *MemcacheProxy) Bind(modules module.ModulesRegistry) error {
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

// listen starts a TCP listener on the given address and accepts incoming connections.
func (p *MemcacheProxy) listen(address string, wg *sync.WaitGroup) error {
	p.log.Info().Str("address", address).Msg("Opening Frontend")

	feMetrics := &Metrics{
		processed: metrics.FeCnxProcessed.WithLabelValues(address, p.id),
		active:    metrics.FeActCnx.WithLabelValues(address, p.id),
		bytesIn:   metrics.FeBytesIn.WithLabelValues(address, p.id),
		bytesOut:  metrics.FeBytesOut.WithLabelValues(address, p.id),
		cnxErrors: metrics.FeCnxErrors.WithLabelValues(address, p.id),
		requests:  metrics.FeRequests.WithLabelValues(address, p.id),
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

// flushBackend sends a flush_all command to the backend upon connection.
func (p *MemcacheProxy) flushBackend(b *backend.Backend) {
	conn, err := net.DialTimeout("tcp", b.Address, p.connectTimeout)
	if err != nil {
		p.log.Warn().Err(err).Str("peer", b.Address).Msg("Unable to connect to backend for auto-flush")
		return
	}
	defer conn.Close()

	timeout := p.connectTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		p.log.Warn().Err(err).Str("peer", b.Address).Msg("Failed to set deadline for auto-flush")
		return
	}

	if _, err := conn.Write([]byte("flush_all\r\n")); err != nil {
		p.log.Warn().Err(err).Str("peer", b.Address).Msg("Failed to send flush_all command")
		return
	}

	buf := make([]byte, 8)
	if _, err := conn.Read(buf); err != nil {
		p.log.Warn().Err(err).Str("peer", b.Address).Msg("Failed to read flush_all response")
	} else {
		p.log.Debug().Str("peer", b.Address).Msg("Backend flushed successfully on connect")
	}
}

// ReceiveUpdate receives a backend update and sends it to the background worker.
func (p *MemcacheProxy) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case p.backendUpdatesChan <- upd:
	case <-p.backendUpdatesChanClosed:
	}
}

// handleConnection parses the Memcache protocol for a single client connection.
// It supports both the traditional ASCII protocol and the newer Meta Text protocol.
// Commands are routed to backends using Ketama consistent hashing based on the key.
// Storage commands with payloads (set, ms, etc.) are handled by reading the specified
// number of bytes before forwarding.
func (p *MemcacheProxy) handleConnection(connFront net.Conn, feMetrics *Metrics) {
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
			feMetrics.cnxErrors.Inc()
		}
	}()

	// Prometheus
	feMetrics.processed.Inc()
	feMetrics.active.Inc()
	defer feMetrics.active.Dec()

	// Read response queue and write responses in order
	futureChan := make(chan chan MemcacheResponse, p.clientQueueSize)
	futureChanStop := make(chan struct{})
	defer close(futureChanStop) // Ensure no backend will block trying to send replies if the client connection is closed
	go func() {
		for {
			select {
			case respChan := <-futureChan:
				select {
				case response := <-respChan:
					p.log.Debug().Uint64("queryId", response.query.id).Msg("Received response")
					n, err := connFront.Write(response.item)
					response.Release() // ponytail: return pooled buffer
					if err != nil {
						if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
							p.log.Debug().Err(err).Str("peer", peerAddress).Msg("Error while writing to client (connection closed)")
						} else {
							p.log.Error().Err(err).Str("peer", peerAddress).Msg("Unexpected error while writing to client")
						}
						cancel()
					}
					feMetrics.bytesOut.Add(float64(n))

					// ponytail: return channel to pool
					responseChanPool.Put(respChan)
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Read queries
	reader := NewMemcacheProtocolReader(connFront, p.bufferSize)
	defer reader.Release()

	for {
		line, err := reader.ReadLine()
		if err == io.EOF || errors.Is(err, net.ErrClosed) {
			return
		} else if err != nil {
			panic("Unexpected error while reading from the client")
		}

		fieldsPtr := p.getFields(line)
		fields := *fieldsPtr
		if len(fields) == 0 {
			p.releaseFields(fieldsPtr)
			continue
		}

		feMetrics.requests.Inc()
		feMetrics.bytesIn.Add(float64(len(line)))

		// ponytail: in-place lowercase for the command
		cmd := fields[0]
		for i := 0; i < len(cmd); i++ {
			if cmd[i] >= 'A' && cmd[i] <= 'Z' {
				cmd[i] += 'a' - 'A'
			}
		}

		if bytes.Equal(cmd, []byte("quit")) {
			p.releaseFields(fieldsPtr)
			return
		}

		// Create a channel for this specific query's response (from pool)
		reqRespChan := responseChanPool.Get().(chan MemcacheResponse)

		// Enqueue the future for the response writer
		select {
		case futureChan <- reqRespChan:
		case <-ctx.Done():
			responseChanPool.Put(reqRespChan)
			p.releaseFields(fieldsPtr)
			return
		}

		// Create a query that will eventually reply to our reqRespChan
		query := NewMemcacheQuery(nil, reqRespChan, futureChanStop)

		// Storage commands: <command> <key> <flags> <exptime> <bytes> [noreply]\r\n<data>\r\n
		// Supported: set, add, replace, append, prepend, cas
		if bytes.Equal(cmd, []byte("set")) || bytes.Equal(cmd, []byte("add")) || bytes.Equal(cmd, []byte("replace")) || bytes.Equal(cmd, []byte("append")) || bytes.Equal(cmd, []byte("prepend")) || bytes.Equal(cmd, []byte("cas")) {
			if len(fields) < 5 {
				query.Reply([]byte("CLIENT_ERROR bad command line format\r\n"))
				p.releaseFields(fieldsPtr)
				continue
			}
			// Parse the expected data size (fields[4])
			size, err := util.ParseSize(fields[4])
			if err != nil {
				query.Reply([]byte("CLIENT_ERROR bad command line format\r\n"))
				p.releaseFields(fieldsPtr)
				continue
			}
			// Read the data payload + trailing \r\n
			payload, err := reader.ReadFull(size + 2)
			if err != nil {
				p.releaseFields(fieldsPtr)
				return
			}
			feMetrics.bytesIn.Add(float64(len(payload)))
			// Forward the full command (header + payload) to the appropriate backend
			// ponytail: using pooled buffer instead of bytes.Clone
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			buf.Write(line)
			buf.Write(payload)
			query.item = buf.Bytes()
			query.buffer = buf
			p.forwardSingle(query, fields[1])
			p.releaseFields(fieldsPtr)
			continue
		}

		// Meta Set command: ms <key> <datalen> [flags]... \r\n<data>\r\n
		// The 'ms' command is part of the Memcache Meta Text protocol.
		// ponytail: ms uses field[2] for size, unlike standard storage commands.
		if bytes.Equal(cmd, []byte("ms")) {
			if len(fields) < 3 {
				query.Reply([]byte("CLIENT_ERROR bad command line format\r\n"))
				p.releaseFields(fieldsPtr)
				continue
			}
			// Parse the expected data size (fields[2])
			size, err := util.ParseSize(fields[2])
			if err != nil {
				query.Reply([]byte("CLIENT_ERROR bad command line format\r\n"))
				p.releaseFields(fieldsPtr)
				continue
			}
			// Read the data payload + trailing \r\n
			payload, err := reader.ReadFull(size + 2)
			if err != nil {
				p.releaseFields(fieldsPtr)
				return
			}
			feMetrics.bytesIn.Add(float64(len(payload)))
			// Forward to backend based on key
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			buf.Write(line)
			buf.Write(payload)
			query.item = buf.Bytes()
			query.buffer = buf
			p.forwardSingle(query, fields[1])
			p.releaseFields(fieldsPtr)
			continue
		}

		// Retrieval commands: <command> <key>*\r\n
		if bytes.Equal(cmd, []byte("get")) || bytes.Equal(cmd, []byte("gets")) || bytes.Equal(cmd, []byte("gat")) || bytes.Equal(cmd, []byte("gats")) {
			if len(fields) < 2 {
				query.Reply([]byte("CLIENT_ERROR bad command line format\r\n"))
				p.releaseFields(fieldsPtr)
				continue
			}
			p.handleMultiGet(query, string(cmd), fields[1:])
			p.releaseFields(fieldsPtr)
			continue
		}

		// Other commands with a key (standard and meta)
		// Supported: delete, incr, decr, touch, mg (Meta Get), md (Meta Delete), ma (Meta Arithmetic), me (Meta Debug)
		if len(fields) > 1 && (bytes.Equal(cmd, []byte("delete")) || bytes.Equal(cmd, []byte("incr")) || bytes.Equal(cmd, []byte("decr")) || bytes.Equal(cmd, []byte("touch")) || bytes.Equal(cmd, []byte("mg")) || bytes.Equal(cmd, []byte("md")) || bytes.Equal(cmd, []byte("ma")) || bytes.Equal(cmd, []byte("me"))) {
			// Commands with a key are routed using the hash ring.
			// ponytail: using pooled buffer instead of bytes.Clone
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			buf.Write(line)
			query.item = buf.Bytes()
			query.buffer = buf
			p.forwardSingle(query, fields[1])
		} else {
			// Commands without a key or unknown commands (e.g., stats, version, mn)
			// are forwarded to a random backend.
			// ponytail: using pooled buffer instead of bytes.Clone
			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			buf.Write(line)
			query.item = buf.Bytes()
			query.buffer = buf
			p.forwardSingle(query, nil)
		}
		p.releaseFields(fieldsPtr)
	}
}

// forwardSingle routes a single command to a backend based on the key.
func (p *MemcacheProxy) forwardSingle(q MemcacheQuery, key []byte) {
	var b *backend.Backend
	if key != nil {
		b = p.ring.getBackend(key)
	} else {
		lst := p.backends.GetList()
		if len(lst) > 0 {
			// random selection for commands without a key
			b = lst[rand.Intn(len(lst))]
		}
	}

	if b == nil {
		q.Reply([]byte("SERVER_ERROR no backend available\r\n"))
		return
	}

	conn := p.backendConnectionPool.Get(b.Address)
	if conn == nil {
		q.Reply([]byte("SERVER_ERROR backend failure\r\n"))
		return
	}

	err := conn.Query(q)
	if err != nil {
		q.Reply([]byte("SERVER_ERROR backend failure\r\n"))
		return
	}
}

// handleMultiGet routes a multi-get command by splitting it into multiple queries
// sent to the appropriate backends based on their keys.
func (p *MemcacheProxy) handleMultiGet(q MemcacheQuery, cmd string, keys [][]byte) {
	groups := make(map[*backend.Backend][][]byte)
	for _, k := range keys {
		b := p.ring.getBackend(k)
		if b != nil {
			groups[b] = append(groups[b], k)
		}
	}

	if len(groups) == 0 {
		q.Reply([]byte("END\r\n"))
		return
	}

	type req struct {
		respChan chan MemcacheResponse
		stopChan chan struct{}
	}

	requests := make([]req, 0, len(groups))

	for b, bKeys := range groups {
		conn := p.backendConnectionPool.Get(b.Address)
		if conn == nil {
			continue
		}

		payload := []byte(cmd)
		for _, k := range bKeys {
			payload = append(payload, ' ')
			payload = append(payload, k...)
		}
		payload = append(payload, []byte("\r\n")...)

		respChan := make(chan MemcacheResponse, 1)
		respStopChan := make(chan struct{})
		bq := NewMemcacheQuery(payload, respChan, respStopChan)

		if err := conn.Query(bq); err != nil {
			close(respStopChan)
			continue
		}

		requests = append(requests, req{respChan: respChan, stopChan: respStopChan})
	}

	var combinedResponse bytes.Buffer
	for _, req := range requests {
		resp := <-req.respChan
		close(req.stopChan)
		if resp.item != nil {
			// Strip the END\r\n from intermediate responses to combine them properly
			idx := bytes.LastIndex(resp.item, []byte("END\r\n"))
			if idx != -1 {
				combinedResponse.Write(resp.item[:idx])
			} else {
				combinedResponse.Write(resp.item)
			}
		}
		resp.Release() // ponytail: return pooled buffer
	}

	combinedResponse.Write([]byte("END\r\n"))
	q.Reply(combinedResponse.Bytes())
}

func (p *MemcacheProxy) getBackendMetrics(backendAddress string) *Metrics {
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
