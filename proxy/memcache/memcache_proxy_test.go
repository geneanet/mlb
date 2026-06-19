package memcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/module"
	"net"
	"sync"
	"testing"
	"time"

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

	p := newMemcacheProxy(tc, wg, ctx).(*MemcacheProxy)
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
	p2 := newMemcacheProxy(tc2, wg, ctx).(*MemcacheProxy)
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
	p3 := newMemcacheProxy(tc3, wg, ctx).(*MemcacheProxy)
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
			if (vDiags.HasErrors()) != tt.wantErr {
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

	p := newMemcacheProxy(tc, wg, ctx).(*MemcacheProxy)
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
	p := newMemcacheProxy(tc, wg, ctx).(*MemcacheProxy)

	mockProvider := &mockUpdateProvider{}
	modules := module.ModulesRegistry{
		"mock": mockProvider,
	}

	p.Bind(modules)

	if len(mockProvider.subs) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", len(mockProvider.subs))
	}

	// Verify listener started
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
}

// Dummy provider to simulate backend updates
type mockUpdateProvider struct {
	subs []backend.BackendUpdateSubscriber
}

func (m *mockUpdateProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {
	m.subs = append(m.subs, sub)
}

func (m *mockUpdateProvider) push(upd backend.BackendUpdate) {
	for _, sub := range m.subs {
		sub.ReceiveUpdate(upd)
	}
}

func TestMemcacheHashRing(t *testing.T) {
	ring := newMemcacheHashRing()
	b1 := &backend.Backend{Address: "127.0.0.1:11211"}
	b2 := &backend.Backend{Address: "127.0.0.1:11212"}
	
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

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}
	b2 := &backend.Backend{Address: b2L.Addr().String(), Meta: backend.NewMetaMap(nil)}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                 "test_proxy",
		source:             "mock",
		addresses:          []string{"127.0.0.1:0"},
		ctx:                ctx,
		cancel:             cancel,
		wg:                 wg,
		connectTimeout:     time.Second,
		closeTimeout:       time.Second,
		backendMinConnections:     2,
		backendMaxConnections:     2,
		backends:           backend.NewRegistry(),
		ring:               newMemcacheHashRing(),
		backendUpdatesChan: make(chan backend.BackendUpdate, 10),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
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
			go proxy.handleConnection(conn)
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Send scatter gather
	client.Write([]byte("get key1 key2\r\n"))
	
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
		id:                        "test_proxy_empty",
		source:                    "mock",
		addresses:                 []string{"127.0.0.1:0"},
		ctx:                       ctx,
		cancel:                    cancel,
		wg:                        wg,
		connectTimeout:            time.Second,
		closeTimeout:              time.Second,
		backendMinConnections:     2,
		backendMaxConnections:     2,
		backends:                  backend.NewRegistry(),
		ring:                      newMemcacheHashRing(),
		backendUpdatesChan:        make(chan backend.BackendUpdate, 10),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
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
			go proxy.handleConnection(conn)
		}
	}()

	client, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	client.SetDeadline(time.Now().Add(1 * time.Second))
	client.Write([]byte("set key1 0 0 2\r\nv1\r\n"))
	
	reader := bufio.NewReader(client)
	respBytes := make([]byte, len("SERVER_ERROR no backend available\r\n"))
	io.ReadFull(reader, respBytes)
	if string(respBytes) != "SERVER_ERROR no backend available\r\n" {
		t.Fatalf("Expected SERVER_ERROR no backend available, got %s", string(respBytes))
	}
}

func dummyMemcacheServer(l net.Listener, val string) {
	for {
		conn, err := l.Accept()
		if err != nil { return }
		
		go func(c net.Conn) {
			defer c.Close()
			reader := bufio.NewReader(c)
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil { return }
				
				fields := bytes.Fields(line)
				if len(fields) == 0 { continue }
				
				cmd := string(fields[0])
				
				if cmd == "set" || cmd == "add" || cmd == "replace" {
					if len(fields) >= 5 {
						size := 0
						for _, b := range fields[4] {
							if b >= '0' && b <= '9' {
								size = size*10 + int(b-'0')
							}
						}
						buf := make([]byte, size+2)
						io.ReadFull(reader, buf)
					}
					c.Write([]byte("STORED\r\n"))
				} else if cmd == "get" {
					for _, k := range fields[1:] {
						c.Write([]byte(fmt.Sprintf("VALUE %s 0 %d\r\n%s\r\n", string(k), len(val), val)))
					}
					c.Write([]byte("END\r\n"))
				} else if cmd == "quit" {
					return
				} else {
					c.Write([]byte("STORED\r\n"))
				}
			}
		}(conn)
	}
}

func TestMemcacheProxyProtocol(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()

	go dummyMemcacheServer(b1L, "v1")

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                        "test_proxy",
		source:                    "mock",
		addresses:                 []string{"127.0.0.1:0"},
		ctx:                       ctx,
		cancel:                    cancel,
		wg:                        wg,
		connectTimeout:            time.Second,
		closeTimeout:              time.Second,
		backendMinConnections:     2,
		backendMaxConnections:     2,
		backends:                  backend.NewRegistry(),
		ring:                      newMemcacheHashRing(),
		backendUpdatesChan:        make(chan backend.BackendUpdate, 10),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
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
			go proxy.handleConnection(conn)
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
		client.SetDeadline(time.Now().Add(1 * time.Second))
		client.Write([]byte(tt.req))
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
		id:                    "test_graceful",
		log:                   log.Logger,
		closeTimeout:          100 * time.Millisecond,
		connectTimeout:        time.Second,
		backendMinConnections: 1,
		backendMaxConnections: 1,
		wg:                    wg,
		ctx:                   ctx,
		cancel:                cancel,
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
	}

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := l.Accept()
		if err == nil {
			proxy.connectionsWG.Add(1)
			proxy.handleConnection(conn)
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
	client.SetDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = client.Read(make([]byte, 1))
	if err == nil {
		t.Error("Expected client connection to be closed after grace period")
	}
}

func TestMemcacheProxy_ForwardSingle_Errors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                    "test_errors",
		log:                   log.Logger,
		backends:              backend.NewRegistry(),
		ring:                  newMemcacheHashRing(),
		backendUpdatesChan:    make(chan backend.BackendUpdate, 10),
		ctx:                   ctx,
		cancel:                cancel,
		backendMinConnections: 1,
		backendMaxConnections: 1,
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	responseChan := make(chan MemcacheResponse, 1)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	// Test No Backend
	q := NewMemcacheQuery([]byte("get key\r\n"), responseChan, responseChanStop)
	proxy.forwardSingle(q, []byte("key"))
	resp := <-responseChan
	if string(resp.item) != "SERVER_ERROR no backend available\r\n" {
		t.Errorf("Expected SERVER_ERROR no backend available, got %s", string(resp.item))
	}

	// Test Backend Failure (Connection Pool returns nil)
	b1 := &backend.Backend{Address: "127.0.0.1:1234", Meta: backend.NewMetaMap(nil)}
	proxy.backends.Add(b1)
	proxy.ring.update(proxy.backends.GetList())

	q2 := NewMemcacheQuery([]byte("get key\r\n"), responseChan, responseChanStop)
	proxy.forwardSingle(q2, []byte("key") )
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
						c.Write([]byte("VALUE k1 0 2\r\nv1\r\nEND\r\n"))
					} else if line == "get k2\r\n" {
						c.Write([]byte("VALUE k2 0 2\r\nv2\r\nEND\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("set ")) {
						// Read payload: v2\r\n (6 bytes for "v2\r\n")
						// ponytail: simplistic for test
						p := make([]byte, 4)
						io.ReadFull(reader, p)
						c.Write([]byte("STORED\r\n"))
					} else {
						c.Write([]byte("STORED\r\n"))
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
		id:                        "test-pipeline",
		log:                       log.Logger,
		connectTimeout:            time.Second,
		closeTimeout:              time.Second,
		wg:                        wg,
		ctx:                       ctx,
		cancel:                    cancel,
		backends:                  backend.NewRegistry(),
		ring:                      newMemcacheHashRing(),
		backendMinConnections:     1,
		backendMaxConnections:     1,
		bufferSize:                16384,
		clientQueueSize:           64,
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
	}
	p.backends.Add(&backend.Backend{Address: backendAddr})
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
			p.handleConnection(conn)
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
		client.Write([]byte(req))
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
		client.SetDeadline(time.Now().Add(2 * time.Second))
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


