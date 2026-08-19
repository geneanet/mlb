package memcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

import (
	"mlb/backend"
	"mlb/metrics"
	"mlb/module"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/rs/zerolog/log"
)

func TestMemcacheProxyConfigAndInit(t *testing.T) {
	configStr := `
	source = "static"
	addresses = ["127.0.0.1:0"]
	connect_timeout = "2s"
	close_timeout = "2s"
	backend_min_connections = 5
	backend_max_connections = 5
	`

	// Test validation
	f, diags := hclsyntax.ParseConfig([]byte(configStr), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("Parse failed: %v", diags)
	}
	tc := &module.Config{
		Config: f.Body,
		Ctx:    &hcl.EvalContext{},
	}

	diags = validateMemcacheProxyConfig(tc)
	if diags.HasErrors() {
		t.Fatalf("Validation failed: %v", diags)
	}

	// Test initialization
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newMemcacheProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p := mod.(*MemcacheProxy)
	if p.connectTimeout != 2*time.Second {
		t.Errorf("Expected 2s connect timeout, got %v", p.connectTimeout)
	}

	// Test with defaults
	configStrDefault := `
	source = "static"
	`
	f2, diags := hclsyntax.ParseConfig([]byte(configStrDefault), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("Parse failed: %v", diags)
	}
	tc2 := &module.Config{Config: f2.Body, Ctx: &hcl.EvalContext{}}
	mod2, err := newMemcacheProxy(tc2, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p2 := mod2.(*MemcacheProxy)
	if p2.connectTimeout != 0 {
		t.Errorf("Expected default 0s connect timeout, got %v", p2.connectTimeout)
	}
	if p2.backendMinConnections != 1 {
		t.Errorf("Expected default 1 min pool size, got %d", p2.backendMinConnections)
	}
	if p2.backendMaxConnections != 1 {
		t.Errorf("Expected default 1 max pool size, got %d", p2.backendMaxConnections)
	}
	if p2.backendInflightQueueSize != 512 {
		t.Errorf("Expected default 512 inflight queue size, got %d", p2.backendInflightQueueSize)
	}
	if p2.backendInputQueueSize != 1024 {
		t.Errorf("Expected default 1024 input queue size, got %d", p2.backendInputQueueSize)
	}

	// Test with custom queue sizes
	configStrCustom := `
	source = "static"
	backend_input_queue_size = 2048
	backend_inflight_queue_size = 1024
	`
	f3, diags := hclsyntax.ParseConfig([]byte(configStrCustom), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("Parse failed: %v", diags)
	}
	tc3 := &module.Config{Config: f3.Body, Ctx: &hcl.EvalContext{}}
	mod3, err := newMemcacheProxy(tc3, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p3 := mod3.(*MemcacheProxy)
	if p3.backendInputQueueSize != 2048 {
		t.Errorf("Expected 2048 input queue size, got %d", p3.backendInputQueueSize)
	}
	if p3.backendInflightQueueSize != 1024 {
		t.Errorf("Expected 1024 inflight queue size, got %d", p3.backendInflightQueueSize)
	}
}

func TestMemcacheConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "Valid min/max",
			config: `
				source = "s1"
				backend_min_connections = 2
				backend_max_connections = 5
			`,
			wantErr: false,
		},
		{
			name: "Min equals max",
			config: `
				source = "s1"
				backend_min_connections = 2
				backend_max_connections = 2
			`,
			wantErr: false,
		},
		{
			name: "Max less than min",
			config: `
				source = "s1"
				backend_min_connections = 5
				backend_max_connections = 2
			`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, diags := hclsyntax.ParseConfig([]byte(tt.config), "test.hcl", hcl.Pos{Line: 1, Column: 1})
			if diags.HasErrors() {
				t.Fatalf("Parse failed: %v", diags)
			}
			tc := &module.Config{
				Config: f.Body,
				Ctx:    &hcl.EvalContext{},
			}
			vDiags := validateMemcacheProxyConfig(tc)
			if vDiags.HasErrors() != tt.wantErr {
				t.Errorf("validateMemcacheProxyConfig() error = %v, wantErr %v", vDiags.HasErrors(), tt.wantErr)
			}
		})
	}
}

func TestMemcacheConfigParsing(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configStr := `
		source = "s1"
		backend_min_connections = 3
	`
	f, _ := hclsyntax.ParseConfig([]byte(configStr), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	tc := &module.Config{Config: f.Body, Ctx: &hcl.EvalContext{}}

	mod, err := newMemcacheProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p := mod.(*MemcacheProxy)
	if p.backendMinConnections != 3 {
		t.Errorf("Expected min 3, got %d", p.backendMinConnections)
	}
	if p.backendMaxConnections != 3 {
		t.Errorf("Expected max 3 (defaulted from min), got %d", p.backendMaxConnections)
	}
}

func TestMemcacheProxyFactory_InvalidDurations(t *testing.T) {
	configStr := `
	source = "s1"
	connect_timeout = "invalid"
	`
	f, diags := hclsyntax.ParseConfig([]byte(configStr), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Config: f.Body,
		Ctx:    &hcl.EvalContext{},
	}

	vDiags := validateMemcacheProxyConfig(tc)
	if !vDiags.HasErrors() {
		t.Error("expected diagnostics to have errors for invalid duration")
	}
}

func TestMemcacheProxyBindAndListen(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configStr := `
	source = "mock"
	addresses = ["127.0.0.1:0"]
	`
	f, diags := hclsyntax.ParseConfig([]byte(configStr), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("Parse failed: %v", diags)
	}
	tc := &module.Config{Config: f.Body, Ctx: &hcl.EvalContext{}}
	mod, err := newMemcacheProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p := mod.(*MemcacheProxy)

	mockProvider := &mockUpdateProvider{
		readyChan: make(chan struct{}),
	}
	modules := module.ModulesRegistry{
		"mock": mockProvider,
	}

	if err := p.Bind(modules); err != nil {
		t.Fatalf("Bind failed: %v", err)
	}

	if len(mockProvider.subs) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", len(mockProvider.subs))
	}

	// Signal readiness from mock provider
	close(mockProvider.readyChan)

	// Test Ready functionality
	select {
	case <-p.Ready():
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timeout waiting for proxy readiness")
	}

	// Verify listener started
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
}

// Dummy provider to simulate backend updates
type mockUpdateProvider struct {
	subs      []backend.BackendUpdateSubscriber
	readyChan chan struct{}
}

func (m *mockUpdateProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {
	m.subs = append(m.subs, sub)
}

func (m *mockUpdateProvider) Ready() <-chan struct{} {
	if m.readyChan == nil {
		m.readyChan = make(chan struct{})
	}
	return m.readyChan
}

func TestMemcacheHashRing(t *testing.T) {
	ring := newMemcacheHashRing()
	b1 := backend.NewBackend("127.0.0.1:11211", nil)
	b2 := backend.NewBackend("127.0.0.1:11212", nil)

	ring.update([]*backend.Backend{b1, b2})

	// Check distribution
	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		b := ring.getBackend(key)
		counts[b.Address]++
	}

	if len(counts) != 2 || counts[b1.Address] == 0 || counts[b2.Address] == 0 {
		t.Fatalf("Expected distribution between 2 backends, got %v", counts)
	}
}

func TestMemcacheProxyScatterGather(t *testing.T) {
	// Start two dummy memcache backends
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	b2L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()
	defer b2L.Close()

	go dummyMemcacheServer(b1L, "v1")
	go dummyMemcacheServer(b2L, "v2")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)
	b2 := backend.NewBackend(b2L.Addr().String(), nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    2,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	go func() {
		for {
			select {
			case <-proxy.ctx.Done():
				return
			case upd := <-proxy.backendUpdatesChan:
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					proxy.backends.Add(upd.Backend.Clone())
				case backend.UpdBackendRemoved:
					proxy.backends.Remove(upd.Address)
				}
				proxy.ring.update(proxy.backends.GetList())
				go proxy.backendConnectionPool.Update()
			}
		}
	}()

	proxy.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Backend: b1, Address: b1.Address})
	proxy.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Backend: b2, Address: b2.Address})

	// Wait for ring to update
	time.Sleep(100 * time.Millisecond)

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := l.Accept()
		if err == nil {
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Send scatter gather
	_, _ = client.Write([]byte("get key1 key2\r\n"))

	reader := bufio.NewReader(client)
	var resp []byte
	for {
		line, _ := reader.ReadBytes('\n')
		resp = append(resp, line...)
		if bytes.HasSuffix(line, []byte("END\r\n")) {
			break
		}
	}

	if !bytes.Contains(resp, []byte("VALUE key1")) || !bytes.Contains(resp, []byte("VALUE key2")) {
		t.Fatalf("Expected both keys in response, got %s", string(resp))
	}
}

func TestMemcacheProxyEmptyBackends(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy_empty",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    2,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := l.Accept()
		if err == nil {
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_ = client.SetDeadline(time.Now().Add(1 * time.Second))
	_, _ = client.Write([]byte("set key1 0 0 2\r\nv1\r\n"))

	reader := bufio.NewReader(client)
	respBytes := make([]byte, len("SERVER_ERROR no backend available\r\n"))
	_, _ = io.ReadFull(reader, respBytes)
	if string(respBytes) != "SERVER_ERROR no backend available\r\n" {
		t.Fatalf("Expected SERVER_ERROR no backend available, got %s", string(respBytes))
	}
}

func dummyMemcacheServer(l net.Listener, val string) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}

		go func(c net.Conn) {
			defer c.Close()
			reader := bufio.NewReader(c)
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					return
				}

				fields := bytes.Fields(line)
				if len(fields) == 0 {
					continue
				}

				cmd := string(fields[0])
				switch cmd {
				case "set", "add", "replace":
					if len(fields) >= 5 {
						size := 0
						for _, b := range fields[4] {
							if b >= '0' && b <= '9' {
								size = size*10 + int(b-'0')
							}
						}
						buf := make([]byte, size+2)
						_, _ = io.ReadFull(reader, buf)
					}
					_, _ = c.Write([]byte("STORED\r\n"))
				case "ms":
					if len(fields) >= 3 {
						size := 0
						for _, b := range fields[2] {
							if b >= '0' && b <= '9' {
								size = size*10 + int(b-'0')
							}
						}
						buf := make([]byte, size+2)
						_, _ = io.ReadFull(reader, buf)
					}
					_, _ = c.Write([]byte("HD\r\n"))
				case "mg":
					_, _ = c.Write([]byte("VA 2\r\nv1\r\n"))
				case "get":
					for _, k := range fields[1:] {
						_, _ = fmt.Fprintf(c, "VALUE %s 0 %d\r\n%s\r\n", string(k), len(val), val)
					}
					_, _ = c.Write([]byte("END\r\n"))
				case "quit":
					return
				default:
					_, _ = c.Write([]byte("STORED\r\n"))
				}
			}
		}(conn)
	}
}

func TestMemcacheProxyProtocol(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()

	go dummyMemcacheServer(b1L, "v1")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    2,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	go func() {
		for {
			select {
			case <-proxy.ctx.Done():
				return
			case upd := <-proxy.backendUpdatesChan:
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					proxy.backends.Add(upd.Backend.Clone())
				case backend.UpdBackendRemoved:
					proxy.backends.Remove(upd.Address)
				}
				proxy.ring.update(proxy.backends.GetList())
				go proxy.backendConnectionPool.Update()
			}
		}
	}()

	proxy.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Backend: b1, Address: b1.Address})

	time.Sleep(100 * time.Millisecond)

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := l.Accept()
		if err == nil {
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tests := []struct {
		req  string
		resp string
	}{
		// Single GET
		{"get key1\r\n", "VALUE key1 0 2\r\nv1\r\nEND\r\n"},
		// SET with payload
		{"set key2 0 0 2\r\nv2\r\n", "STORED\r\n"},
		// Other command
		{"delete key3\r\n", "STORED\r\n"}, // Our dummy server returns STORED for everything else
		// Bad format (set without payload size)
		{"set key4 0 0\r\n", "CLIENT_ERROR bad command line format\r\n"},
		// Retrieval with missing key
		{"get\r\n", "CLIENT_ERROR bad command line format\r\n"},
		// Command without key (should go to random backend)
		{"stats\r\n", "STORED\r\n"},
		// Unknown command
		{"unknown_cmd\r\n", "STORED\r\n"},
		// Quit
		{"quit\r\n", ""},
	}

	reader := bufio.NewReader(client)
	for i, tt := range tests {
		_ = client.SetDeadline(time.Now().Add(1 * time.Second))
		_, _ = client.Write([]byte(tt.req))
		if tt.resp != "" {
			respBytes := make([]byte, len(tt.resp))
			_, err := io.ReadFull(reader, respBytes)
			if err != nil {
				t.Fatalf("Test %d (%q): expected %q, got error %v", i, tt.req, tt.resp, err)
			}
			if string(respBytes) != tt.resp {
				t.Fatalf("Test %d (%q): expected %q, got %q", i, tt.req, tt.resp, string(respBytes))
			}
		}
	}
}

func TestMemcacheProxy_HandleConnection_GracefulShutdown(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	proxy := &MemcacheProxy{
		id:                       "test_graceful",
		log:                      log.Logger,
		closeTimeout:             100 * time.Millisecond,
		connectTimeout:           time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		wg:                       wg,
		ctx:                      ctx,
		cancel:                   cancel,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := l.Accept()
		if err == nil {
			proxy.connectionsWG.Add(1)
			proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Cancel proxy context
	cancel()

	// Wait for grace period
	time.Sleep(200 * time.Millisecond)

	// Try to read from client, should be closed
	_ = client.SetDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = client.Read(make([]byte, 1))
	if err == nil {
		t.Error("Expected client connection to be closed after grace period")
	}
}

func TestMemcacheProxy_ForwardSingle_Errors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_errors",
		log:                      log.Logger,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		ctx:                      ctx,
		cancel:                   cancel,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	responseChan := make(chan MemcacheResponse, 1)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	// Test No Backend
	q := NewMemcacheQuery([]byte("get key\r\n"), responseChan, responseChanStop, false)
	proxy.forwardSingle(q, []byte("key"))
	resp := <-responseChan
	if string(resp.item) != "SERVER_ERROR no backend available\r\n" {
		t.Errorf("Expected SERVER_ERROR no backend available, got %s", string(resp.item))
	}

	// Test Backend Failure (Connection Pool returns nil)
	b1 := backend.NewBackend("127.0.0.1:1234", nil)
	proxy.backends.Add(b1)
	proxy.ring.update(proxy.backends.GetList())

	q2 := NewMemcacheQuery([]byte("get key\r\n"), responseChan, responseChanStop, false)
	proxy.forwardSingle(q2, []byte("key"))
	resp2 := <-responseChan
	if string(resp2.item) != "SERVER_ERROR backend failure\r\n" {
		t.Errorf("Expected SERVER_ERROR backend failure, got %s", string(resp2.item))
	}
}

func TestMemcachePipelining(t *testing.T) {
	// Setup a dummy backend
	backendAddr := "127.0.0.1:11215"
	lBack, err := net.Listen("tcp", backendAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer lBack.Close()

	go func() {
		for {
			conn, err := lBack.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				reader := bufio.NewReader(c)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					// Artificial delay
					time.Sleep(10 * time.Millisecond)
					if line == "get k1\r\n" {
						_, _ = c.Write([]byte("VALUE k1 0 2\r\nv1\r\nEND\r\n"))
					} else if line == "get k2\r\n" {
						_, _ = c.Write([]byte("VALUE k2 0 2\r\nv2\r\nEND\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("set ")) {
						// Read payload: v2\r\n (6 bytes for "v2\r\n")
						// ponytail: simplistic for test
						p := make([]byte, 4)
						_, _ = io.ReadFull(reader, p)
						_, _ = c.Write([]byte("STORED\r\n"))
					} else {
						_, _ = c.Write([]byte("STORED\r\n"))
					}
				}
			}(conn)
		}
	}()

	// Setup proxy
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &MemcacheProxy{
		id:                       "test-pipeline",
		log:                      log.Logger,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		wg:                       wg,
		ctx:                      ctx,
		cancel:                   cancel,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		bufferSize:               16384,
		clientQueueSize:          64,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	p.backends.Add(backend.NewBackend(backendAddr, nil))
	p.ring.update(p.backends.GetList())
	p.backendConnectionPool = NewMemcacheBackendConnectionPool(p)
	p.backendConnectionPool.Update()

	// Frontend listener
	lFront, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lFront.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := lFront.Accept()
		if err == nil {
			p.connectionsWG.Add(1)
			p.handleConnection(conn, dummyMetrics())
		}
	}()

	// Client
	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Pipeline multiple requests
	requests := []string{
		"set k1 0 0 2\r\nv1\r\n",
		"set k2 0 0 2\r\nv2\r\n",
		"set k3 0 0 2\r\nv3\r\n",
		"delete k1\r\n",
	}

	for _, req := range requests {
		_, _ = client.Write([]byte(req))
	}

	// Read responses
	expectedResponses := []string{
		"STORED\r\n",
		"STORED\r\n",
		"STORED\r\n",
		"STORED\r\n",
	}

	reader := bufio.NewReader(client)
	for i, expected := range expectedResponses {
		_ = client.SetDeadline(time.Now().Add(2 * time.Second))
		resp := make([]byte, len(expected))
		_, err := io.ReadFull(reader, resp)
		if err != nil {
			t.Fatalf("Failed to read response %d: %v", i, err)
		}
		if string(resp) != expected {
			t.Errorf("Response %d: expected %q, got %q", i, expected, string(resp))
		}
	}
}

func TestMemcacheProxyMetaProtocol(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()
	go dummyMemcacheServer(b1L, "v1")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_meta_proxy",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)
	proxy.backends.Add(b1)
	proxy.ring.update(proxy.backends.GetList())
	proxy.backendConnectionPool.Update()

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := l.Accept()
		if conn != nil {
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := bufio.NewReader(client)

	// Test mg
	_, _ = client.Write([]byte("mg key1 v\r\n"))
	resp, _ := reader.ReadBytes('\n')
	if string(resp) != "VA 2\r\n" {
		t.Errorf("Expected VA 2\\r\\n, got %q", string(resp))
	}
	payload := make([]byte, 4) // v1\r\n
	_, _ = io.ReadFull(reader, payload)

	// Test ms
	_, _ = client.Write([]byte("ms key1 2\r\nhi\r\n"))
	resp, _ = reader.ReadBytes('\n')
	if string(resp) != "HD\r\n" {
		t.Errorf("Expected HD\\r\\n, got %q", string(resp))
	}
}

func TestMemcacheProxyMetaProtocolExpanded(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()

	// Dummy server for meta protocol
	go func() {
		for {
			conn, err := b1L.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					fields := bytes.Fields(line)
					if len(fields) == 0 {
						continue
					}
					cmd := string(fields[0])
					switch cmd {
					case "md":
						_, _ = c.Write([]byte("HD\r\n"))
					case "ma":
						_, _ = c.Write([]byte("HD\r\n"))
					case "me":
						_, _ = c.Write([]byte("EN\r\n"))
					case "mn":
						_, _ = c.Write([]byte("HD\r\n"))
					case "ms":
						size := 0
						_, _ = fmt.Sscanf(string(fields[2]), "%d", &size)
						payload := make([]byte, size+2)
						_, _ = io.ReadFull(r, payload)
						_, _ = c.Write([]byte("HD\r\n"))
					case "mg":
						_, _ = c.Write([]byte("VA 2\r\nok\r\n"))
					}
				}
			}(conn)
		}
	}()

	b1 := backend.NewBackend(b1L.Addr().String(), nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_meta_expanded",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)
	proxy.backends.Add(b1)
	proxy.ring.update(proxy.backends.GetList())
	proxy.backendConnectionPool.Update()

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, _ := l.Accept()
		if conn != nil {
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := bufio.NewReader(client)

	tests := []struct {
		req  string
		resp string
	}{
		{"md key1\r\n", "HD\r\n"},
		{"ma key1\r\n", "HD\r\n"},
		{"me key1\r\n", "EN\r\n"},
		{"mn\r\n", "HD\r\n"},
		{"ms key1 2\r\nhi\r\n", "HD\r\n"},
		{"mg key1\r\n", "VA 2\r\nok\r\n"},
		{"ms key1\r\n", "CLIENT_ERROR bad command line format\r\n"},
		{"ms key1 bad\r\n", "CLIENT_ERROR bad command line format\r\n"},
	}

	for _, tt := range tests {
		_, _ = client.Write([]byte(tt.req))
		if tt.resp != "" {
			resp := make([]byte, len(tt.resp))
			_, _ = io.ReadFull(reader, resp)
			if string(resp) != tt.resp {
				t.Errorf("For %q, expected %q, got %q", tt.req, tt.resp, string(resp))
			}
		}
	}
}

func TestMemcacheProxyFlushOnConnectFunctional(t *testing.T) {
	b1L, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer b1L.Close()

	flushReceived := make(chan bool, 1)
	go func() {
		conn, err := b1L.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		line, _ := reader.ReadString('\n')
		if line == "flush_all\r\n" {
			_, _ = conn.Write([]byte("OK\r\n"))
			flushReceived <- true
		}
	}()

	b1 := backend.NewBackend(b1L.Addr().String(), nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		source:                   "mock",
		log:                      zerolog.Nop(),
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		flushBackendWhenAdded:    true,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		backendUpdatesChanClosed: make(chan struct{}),
		beMetricsCache:           make(map[string]*Metrics),
		readyChan:                make(chan struct{}),
	}

	go func() {
		for {
			select {
			case <-proxy.ctx.Done():
				return
			case upd := <-proxy.backendUpdatesChan:
				switch upd.Kind {
				case backend.UpdBackendAdded:
					if proxy.flushBackendWhenAdded {
						proxy.flushBackend(upd.Backend)
					}
					proxy.backends.Add(upd.Backend.Clone())
				}
			}
		}
	}()

	proxy.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Backend: b1, Address: b1.Address})

	select {
	case <-flushReceived:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for flush_all")
	}
}

func TestMemcacheProxyRandomization(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()
	b2L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b2L.Close()

	counts := make(map[string]int)
	var mu sync.Mutex

	handler := func(l net.Listener, name string) {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				reader := bufio.NewReader(c)
				for {
					line, _, err := reader.ReadLine()
					if err != nil {
						return
					}
					if bytes.HasPrefix(line, []byte("stats")) {
						mu.Lock()
						counts[name]++
						mu.Unlock()
						_, _ = c.Write([]byte("END\r\n"))
					} else if string(line) == "quit" {
						return
					}
				}
			}(conn)
		}
	}

	go handler(b1L, "b1")
	go handler(b2L, "b2")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)
	b2 := backend.NewBackend(b2L.Addr().String(), nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy_random",
		source:                   "mock",
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		backendMinConnections:    2,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	go func() {
		for {
			select {
			case <-proxy.ctx.Done():
				return
			case upd := <-proxy.backendUpdatesChan:
				switch upd.Kind {
				case backend.UpdBackendAdded:
					proxy.backends.Add(upd.Backend.Clone())
				}
				proxy.ring.update(proxy.backends.GetList())
				go proxy.backendConnectionPool.Update()
			}
		}
	}()

	proxy.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Backend: b1, Address: b1.Address})
	proxy.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Backend: b2, Address: b2.Address})

	time.Sleep(200 * time.Millisecond)

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, dummyMetrics())
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		_, err := client.Write([]byte("stats\r\n"))
		if err != nil {
			t.Fatalf("Iteration %d: write failed: %v", i, err)
		}
		resp := make([]byte, 5)
		_, err = io.ReadFull(client, resp)
		if err != nil {
			t.Fatalf("Iteration %d: read failed: %v", i, err)
		}
		if string(resp) != "END\r\n" {
			t.Fatalf("Iteration %d: expected END\r\n, got %q", i, string(resp))
		}
	}
	_, _ = client.Write([]byte("quit\r\n"))
	_ = client.Close()

	mu.Lock()
	defer mu.Unlock()
	if counts["b1"] == 0 || counts["b2"] == 0 {
		t.Errorf("Randomization failed: b1=%d, b2=%d", counts["b1"], counts["b2"])
	}
}

func dummyMetrics() *Metrics {
	return &Metrics{
		processed: metrics.FeCnxProcessed.WithLabelValues("test", "test"),
		active:    metrics.FeActCnx.WithLabelValues("test", "test"),
		bytesIn:   metrics.FeBytesIn.WithLabelValues("test", "test"),
		bytesOut:  metrics.FeBytesOut.WithLabelValues("test", "test"),
		cnxErrors: metrics.FeCnxErrors.WithLabelValues("test", "test"),
		requests:  metrics.FeRequests.WithLabelValues("test", "test"),
	}
}

func TestMemcachePhantomResponse(t *testing.T) {
	// Setup a controllable backend
	lBack, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lBack.Close()
	backendAddr := lBack.Addr().String()

	phantomRespReady := make(chan struct{})
	phantomRespSend := make(chan struct{})

	go func() {
		conn, err := lBack.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			switch line {
			case "get phantom\r\n":
				close(phantomRespReady)
				<-phantomRespSend
				_, _ = conn.Write([]byte("VALUE phantom 0 7\r\nphantom\r\nEND\r\n"))
			case "get real\r\n":
				_, _ = conn.Write([]byte("VALUE real 0 4\r\nreal\r\nEND\r\n"))
			}
		}
	}()

	// Setup proxy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &MemcacheProxy{
		id:                       "phantom-test",
		log:                      log.Logger,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		ctx:                      ctx,
		cancel:                   cancel,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		bufferSize:               4096,
		clientQueueSize:          128,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	p.backends.Add(backend.NewBackend(backendAddr, nil))
	p.ring.update(p.backends.GetList())
	p.backendConnectionPool = NewMemcacheBackendConnectionPool(p)
	p.backendConnectionPool.Update()

	// Frontend listener
	lFront, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lFront.Close()

	go func() {
		for {
			conn, err := lFront.Accept()
			if err != nil {
				return
			}
			p.connectionsWG.Add(1)
			go p.handleConnection(conn, dummyMetrics())
		}
	}()

	// 1. Manually get a channel, put a "phantom" response in it, and put it back in the pool.
	ch := getResponseChan()
	phantomQuery := NewMemcacheQuery([]byte("get phantom\r\n"), ch, make(chan struct{}), false)
	ch <- MemcacheResponse{query: phantomQuery, item: []byte("VALUE phantom 0 7\r\nphantom\r\nEND\r\n")}
	putResponseChan(ch)

	// 2. Client 2 connects and sends "get real"
	c2, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	_, _ = c2.Write([]byte("get real\r\n"))

	// Release the real response from backend
	close(phantomRespSend)

	reader := bufio.NewReader(c2)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read from c2: %v", err)
	}

	if line == "VALUE phantom 0 7\r\n" {
		t.Fatal("Client 2 received phantom response!")
	}

	if line != "VALUE real 0 4\r\n" {
		t.Fatalf("Expected VALUE real 0 4, got %q", line)
	}
}
