package redis

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/zclconf/go-cty/cty"
)

func TestRedisProxy_MaxMessageSize(t *testing.T) {
	for _, tt := range []struct {
		value    string
		wantSize int
		wantErr  bool
	}{
		{"", DefaultMaxMessageSize, false},
		{`"64mb"`, 64 * 1024 * 1024, false},
		{"10", 10, false},
		{"11", 11, false},
		{"0", 0, true},
		{`"0b"`, 0, true},
		{"-1", 0, true},
		{`"-1mb"`, 0, true},
		{`"invalid"`, 0, true},
		{"true", 0, true},
		{"1.5", 0, true},
		{"9999999999999999999999999999999", 0, true},
	} {
		t.Run(tt.value, func(t *testing.T) {
			attr := ""
			if tt.value != "" {
				attr = "max_message_size = " + tt.value
			}
			block := testutil.ParseHCL(t, fmt.Sprintf(`
proxy "redis" "test" {
	source = "test"
	%s
}`, attr))
			cfg := &module.Config{Config: block.Body}
			diags := validateRedisProxyConfig(cfg)
			if diags.HasErrors() != tt.wantErr {
				t.Fatalf("unexpected validation result: %v", diags)
			}
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			defer func() {
				cancel()
				wg.Wait()
			}()
			mod, err := newRedisProxy(cfg, &wg, ctx)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected constructor error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			p := mod.(*RedisProxy)
			if p.maxMessageSize != tt.wantSize {
				t.Fatalf("got limit %d, want %d", p.maxMessageSize, tt.wantSize)
			}
			// Exercise both fresh and pooled readers with the configured limit.
			for range 2 {
				reader := p.newProtocolReader(strings.NewReader("$5\r\nhello\r\n"))
				msg, err := reader.ReadMessage(false)
				if tt.wantSize < 11 {
					if err == nil || !strings.Contains(err.Error(), "max_message_size") {
						t.Errorf("expected size limit error, got %v", err)
					}
				} else if err != nil || string(msg) != "$5\r\nhello\r\n" {
					t.Errorf("got message %q, error %v", msg, err)
				}
				p.releaseBuffer(msg)
				reader.Release()
			}
		})
	}
}

func TestRedisProxy_BackendReuseAfterReadError(t *testing.T) {
	for _, tt := range []struct {
		name        string
		input       string
		fromBackend bool
	}{
		{"client size error", "$1000\r\n", false},
		{"backend size error", "$1000\r\n", true},
		{"client protocol error", "$invalid\r\n", false},
		{"backend protocol error", "$invalid\r\n", true},
		{"normal client disconnect", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			p := &RedisProxy{
				ctx:            ctx,
				maxMessageSize: 64,
				resetTimeout:   time.Second,
				closeTimeout:   time.Second,
				backends:       backend.NewRegistry(zerolog.Nop(), false),
				beMetricsCache: make(map[string]*Metrics),
			}
			be := backend.NewBackend("test-backend:6379", nil)
			p.backends.Add(be)
			front, client := net.Pipe()
			back, server := net.Pipe()
			backendCtx, backendCancel := context.WithCancel(ctx)
			pool := &RedisBackendConnectionPool{proxy: p, ctx: ctx}
			rbc := &RedisBackendConnection{
				pool:    pool,
				backend: be,
				conn:    back,
				ctx:     backendCtx,
				cancel:  backendCancel,
				metrics: p.getBackendMetrics(be.Address),
			}
			pool.pool = []*RedisBackendConnection{rbc}
			p.backendConnectionPool = pool
			stopClosing := context.AfterFunc(backendCtx, func() { _ = back.Close() })
			handlerDone := make(chan struct{})
			serverDone := make(chan struct{})
			var serverErr error
			t.Cleanup(func() {
				cancel()
				_ = client.Close()
				_ = front.Close()
				_ = back.Close()
				_ = server.Close()
				stopClosing()
				select {
				case <-handlerDone:
				case <-time.After(2 * time.Second):
					t.Error("connection handler did not stop")
				}
				select {
				case <-serverDone:
					if serverErr != nil {
						t.Errorf("backend server: %v", serverErr)
					}
				case <-time.After(2 * time.Second):
					t.Error("backend server did not stop")
				}
			})

			// A backend that would successfully complete ECHO/RESET if asked.
			go func() {
				defer close(serverDone)
				serverErr = func() error {
					if tt.fromBackend {
						if _, err := server.Write([]byte(tt.input)); err != nil {
							return err
						}
					}
					reader := NewRedisProtocolReader(server, 128)
					defer reader.Release()
					for {
						msg, err := reader.ReadMessage(true)
						if err != nil {
							return nil
						}
						var reply []byte
						const echoPrefix = "*2\r\n$4\r\nECHO\r\n"
						switch {
						case bytes.HasPrefix(msg, []byte(echoPrefix)):
							reply = msg[len(echoPrefix):]
						case string(msg) == "*1\r\n$5\r\nRESET\r\n":
							reply = []byte("+RESET\r\n")
						default:
							err = fmt.Errorf("unexpected backend command %q", msg)
							ReleaseBuffer(msg)
							return err
						}
						_, err = server.Write(reply)
						ReleaseBuffer(msg)
						if err != nil {
							return err
						}
					}
				}()
			}()
			feMetrics := &Metrics{
				processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_processed"}),
				active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_active"}),
				cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "test_errors"}),
			}
			p.connectionsWG.Add(1)
			go func() {
				defer close(handlerDone)
				p.handleConnection(front, feMetrics)
			}()
			if tt.input == "" {
				_ = client.Close()
			} else if !tt.fromBackend {
				if _, err := client.Write([]byte(tt.input)); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-handlerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("connection handler did not finish")
			}

			discarded := tt.input != ""
			if got := backendCtx.Err() != nil; got != discarded {
				t.Errorf("backend discarded = %v, want %v", got, discarded)
			}
			wantPoolSize, wantResets := 1, uint64(1)
			if discarded {
				wantPoolSize, wantResets = 0, 0
				select {
				case <-serverDone:
				case <-time.After(2 * time.Second):
					t.Fatal("discarded backend socket was not closed")
				}
			}
			pool.mutex.Lock()
			poolSize := len(pool.pool)
			pool.mutex.Unlock()
			if poolSize != wantPoolSize || rbc.resetCount != wantResets {
				t.Errorf("pool size = %d, resets = %d; want %d, %d", poolSize, rbc.resetCount, wantPoolSize, wantResets)
			}
		})
	}
}

func TestRedisProxy_PipeProtocolErrorDuringStop(t *testing.T) {
	p := &RedisProxy{}
	input, sender := net.Pipe()
	defer input.Close()
	defer sender.Close()
	output, receiver := net.Pipe()
	defer output.Close()
	defer receiver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rbc := &RedisBackendConnection{
		pool:    &RedisBackendConnectionPool{proxy: p},
		backend: backend.NewBackend("test-backend:6379", nil),
		ctx:     ctx,
		cancel:  cancel,
	}
	stop := make(chan struct{})
	close(stop)
	done := make(chan struct{})
	go p.pipe(input, output, done, stop, rbc, nil, nil)
	if _, err := sender.Write([]byte("$invalid\r\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pipe did not stop")
	}
	if ctx.Err() == nil {
		t.Fatal("protocol error during shutdown did not discard the backend")
	}
}

func TestRedisProxyConfig(t *testing.T) {
	t.Run("Parse valid config", func(t *testing.T) {
		hcl := `
			source = "backends_inventory.static.test"
			addresses = [":6379"]
			preconnect = 5
			idle_timeout = "10m"
			healthcheck = true
		`
		parser := hclparse.NewParser()
		f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
		tc := &module.Config{
			Config: f.Body,
		}

		config := parseRedisProxyConfig(tc)
		if config.Preconnect != 5 {
			t.Errorf("expected 5, got %d", config.Preconnect)
		}
		if config.IdleTimeout != "10m" {
			t.Errorf("expected 10m, got %s", config.IdleTimeout)
		}
		if !config.Healthcheck {
			t.Error("expected true")
		}
	})

	t.Run("Validate valid config", func(t *testing.T) {
		hcl := `
			source = "backends_inventory.static.test"
			addresses = [":6379"]
		`
		parser := hclparse.NewParser()
		f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
		tc := &module.Config{
			Config: f.Body,
		}

		diags := validateRedisProxyConfig(tc)
		if diags.HasErrors() {
			t.Error("expected no errors")
		}
	})

	t.Run("Validate invalid duration", func(t *testing.T) {
		hcl := `
			source = "backends_inventory.static.test"
			idle_timeout = "invalid"
		`
		parser := hclparse.NewParser()
		f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
		tc := &module.Config{
			Config: f.Body,
		}

		diags := validateRedisProxyConfig(tc)
		if !diags.HasErrors() {
			t.Error("expected errors")
		}
	})
}

func TestNewRedisProxy(t *testing.T) {
	hcl := `
		source = "backends_inventory.static.test"
		addresses = [":6379"]
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Config: f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Error("expected p to be not nil")
	}

	rp := p.(*RedisProxy)
	if rp.bufferSize != 16384 {
		t.Errorf("expected 16384, got %d", rp.bufferSize)
	}
	if rp.maxReusedBufferSize != 65536 {
		t.Errorf("expected 65536, got %d", rp.maxReusedBufferSize)
	}
	if rp.idleTimeout != 5*time.Minute {
		t.Errorf("expected 5m, got %v", rp.idleTimeout)
	}
}

type mockBackendProvider struct {
	provided  bool
	readyChan chan struct{}
}

func (m *mockBackendProvider) ProvideUpdates(receiver backend.BackendUpdateSubscriber) {
	m.provided = true
}

func (m *mockBackendProvider) Ready() <-chan struct{} {
	if m.readyChan == nil {
		m.readyChan = make(chan struct{})
	}
	return m.readyChan
}

func TestRedisProxy_BindAndReceiveUpdate(t *testing.T) {
	hcl := `
		source = "backends_inventory.static.test"
		addresses = ["127.0.0.1:0"] // 0 to pick a random port
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Category: "proxy",
		Type:     "redis",
		Name:     "test",
		Config:   f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rp := p.(*RedisProxy)

	mockProvider := &mockBackendProvider{readyChan: make(chan struct{})}
	registry := make(module.ModulesRegistry)
	registry.AddModule("backends_inventory.static.test", mockProvider)

	rp.listener = testutil.Listener{}
	err = rp.Bind(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockProvider.provided {
		t.Error("expected mockProvider.provided to be true")
	}

	// Signal readiness
	close(mockProvider.readyChan)
	select {
	case <-rp.Ready():
		// OK
	case <-time.After(1 * time.Second):
		t.Errorf("timeout waiting for redis proxy readiness")
	}

	// Test ReceiveUpdate
	be := backend.NewBackend("127.0.0.1:6379", nil)
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Backend: be,
	})

	// Wait for the async update to process
	testutil.Eventually(t, func() bool {
		return rp.backends.Has(be.Address)
	}, 1*time.Second, 10*time.Millisecond)

	if !rp.backends.Has(be.Address) {
		t.Errorf("expected backend %s to be present", be.Address)
	}

	// Test Modified
	be.Meta.Set("default", "foo", cty.StringVal("bar"))
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Backend: be,
	})
	testutil.Eventually(t, func() bool {
		list := rp.backends.GetList()
		if len(list) == 0 {
			return false
		}
		val, ok := list[0].Meta.Get("default", "foo")
		return ok && val.AsString() == "bar"
	}, 1*time.Second, 10*time.Millisecond)

	val, ok := rp.backends.GetList()[0].Meta.Get("default", "foo")
	if !ok {
		t.Error("expected meta value to be present")
	}
	if val.AsString() != "bar" {
		t.Errorf("expected bar, got %s", val.AsString())
	}

	// Test Removed
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: be.Address,
	})
	testutil.Eventually(t, func() bool {
		return !rp.backends.Has(be.Address)
	}, 1*time.Second, 10*time.Millisecond)

	if rp.backends.Has(be.Address) {
		t.Errorf("expected backend %s to be removed", be.Address)
	}
}

// TestRedisProxy_CloseOnBackendRemoval verifies that active connections are closed when
// the backend is removed from the balancer, if close_on_backend_removal is enabled.
func TestRedisProxy_CloseOnBackendRemoval(t *testing.T) {
	// 1. Setup Backend (Echo server is enough as we just need a TCP connection)
	lnBackend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnBackend.Close()
	go func() {
		for {
			conn, err := lnBackend.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_, _ = io.Copy(c, c)
			}(conn) // simple echo
		}
	}()

	// 2. Setup Redis Proxy
	hcl := `
		source = "backends_inventory.static.test"
		addresses = ["127.0.0.1:0"]
		close_on_backend_removal = true
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Category: "proxy",
		Type:     "redis",
		Name:     "test",
		Config:   f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rp := p.(*RedisProxy)

	mockProvider := &mockBackendProvider{readyChan: make(chan struct{})}
	registry := make(module.ModulesRegistry)
	registry.AddModule("backends_inventory.static.test", mockProvider)

	rp.listener = testutil.Listener{}
	err = rp.Bind(registry)
	if err != nil {
		t.Fatal(err)
	}
	close(mockProvider.readyChan)
	<-rp.Ready()

	// Manual Listener for proxy to easily get the address and control the loop
	lnProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnProxy.Close()
	proxyAddr := lnProxy.Addr().String()

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_processed"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_active"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_in"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_out"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_requests"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_cnx_errors"}),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lnProxy.Accept()
			if err != nil {
				return
			}
			rp.connectionsWG.Add(1)
			go rp.handleConnection(conn, feMetrics)
		}
	}()

	// 3. Add backend
	beCtx, beCancel := context.WithCancel(context.Background())
	be := backend.NewBackend(lnBackend.Addr().String(), nil)
	be.Ctx = beCtx
	be.Cancel = beCancel
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Backend: be,
	})

	// Wait for backend to be available in pool
	testutil.Eventually(t, func() bool {
		return rp.backends.Has(be.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// 4. Connect client
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Wait for the proxy to accept the connection and get a backend
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}

	// 5. Trigger removal (which cancels beCtx)
	beCancel()
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: be.Address,
	})

	// 6. Verify client connection is closed
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err != nil {
			close(done)
		}
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("expected client connection to be closed after backend removal")
	}
}

func TestRedisProxy_NotCloseOnBackendRemoval(t *testing.T) {
	// 1. Setup Backend
	lnBackend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnBackend.Close()
	go func() {
		for {
			conn, err := lnBackend.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	// 2. Setup Redis Proxy (close_on_backend_removal = false)
	hcl := `
		source = "backends_inventory.static.test"
		addresses = ["127.0.0.1:0"]
		close_on_backend_removal = false
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Category: "proxy",
		Type:     "redis",
		Name:     "test",
		Config:   f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rp := p.(*RedisProxy)

	mockProvider := &mockBackendProvider{readyChan: make(chan struct{})}
	registry := make(module.ModulesRegistry)
	registry.AddModule("backends_inventory.static.test", mockProvider)

	rp.listener = testutil.Listener{}
	err = rp.Bind(registry)
	if err != nil {
		t.Fatal(err)
	}
	close(mockProvider.readyChan)
	<-rp.Ready()

	lnProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnProxy.Close()
	proxyAddr := lnProxy.Addr().String()

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_processed2"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_active2"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_in2"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_out2"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_requests2"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_cnx_errors2"}),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lnProxy.Accept()
			if err != nil {
				return
			}
			rp.connectionsWG.Add(1)
			go rp.handleConnection(conn, feMetrics)
		}
	}()

	// 3. Add backend
	beCtx, beCancel := context.WithCancel(context.Background())
	be := backend.NewBackend(lnBackend.Addr().String(), nil)
	be.Ctx = beCtx
	be.Cancel = beCancel
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Backend: be,
	})
	testutil.Eventually(t, func() bool {
		return rp.backends.Has(be.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// 4. Connect client
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}

	// 5. Trigger removal
	beCancel()
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: be.Address,
	})

	// 6. Verify client connection is NOT closed
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err != nil {
			close(done)
		}
	}()

	select {
	case <-done:
		t.Error("expected client connection to stay open after backend removal")
	case <-time.After(500 * time.Millisecond):
		// Success
	}
}

func TestRedisProxy_CloseOnBackendRemoval_NoBalancer(t *testing.T) {
	// 1. Setup Backend
	lnBackend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnBackend.Close()
	go func() {
		for {
			conn, err := lnBackend.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()

	// 2. Setup Redis Proxy
	hcl := `
		source = "backends_inventory.static.test"
		addresses = ["127.0.0.1:0"]
		close_on_backend_removal = true
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Category: "proxy",
		Type:     "redis",
		Name:     "test",
		Config:   f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rp := p.(*RedisProxy)

	mockProvider := &mockBackendProvider{readyChan: make(chan struct{})}
	registry := make(module.ModulesRegistry)
	registry.AddModule("backends_inventory.static.test", mockProvider)

	rp.listener = testutil.Listener{}
	err = rp.Bind(registry)
	if err != nil {
		t.Fatal(err)
	}
	close(mockProvider.readyChan)
	<-rp.Ready()

	lnProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnProxy.Close()
	proxyAddr := lnProxy.Addr().String()

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_processed3"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_active3"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_in3"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_out3"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_requests3"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_cnx_errors3"}),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lnProxy.Accept()
			if err != nil {
				return
			}
			rp.connectionsWG.Add(1)
			go rp.handleConnection(conn, feMetrics)
		}
	}()

	// 3. Add backend WITHOUT context
	be := backend.NewBackend(lnBackend.Addr().String(), nil)
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Backend: be,
	})
	testutil.Eventually(t, func() bool {
		return rp.backends.Has(be.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Verify that the proxy created a context
	storedBe := rp.backends.GetList()[0]
	if storedBe.Ctx == nil {
		t.Fatal("expected proxy to create a context for the backend")
	}

	// 4. Connect client
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}

	// 5. Trigger removal
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: be.Address,
	})

	// 6. Verify client connection is closed
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		_, err := conn.Read(buf)
		if err != nil {
			close(done)
		}
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("expected client connection to be closed after backend removal (direct inventory case)")
	}
}

func TestRedisProxy_AcceptLoop_Backoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mockLn := testutil.NewMockFailingListener(ln, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nopLogger := zerolog.Nop()
	p := &RedisProxy{
		id:                    "test-accept-backoff",
		log:                   nopLogger,
		ctx:                   ctx,
		cancel:                cancel,
		backends:              backend.NewRegistry(nopLogger, false),
		beMetricsCache:        make(map[string]*Metrics),
		closeOnBackendRemoval: false,
	}

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_processed_backoff"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_active_backoff"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_in_backoff"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_out_backoff"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_requests_backoff"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_cnx_errors_backoff"}),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go p.acceptLoop(mockLn, ln.Addr().String(), &wg, feMetrics)

	// Connect client after backoff retries succeed
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	testutil.Eventually(t, func() bool {
		return mockLn.FailCount() == 2
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	_ = ln.Close()
	wg.Wait()
}

func TestRedisProxy_AcceptLoop_CancelDuringBackoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mockLn := testutil.NewMockFailingListener(ln, 1000)

	ctx, cancel := context.WithCancel(context.Background())

	nopLogger := zerolog.Nop()
	p := &RedisProxy{
		id:                    "test-accept-cancel",
		log:                   nopLogger,
		ctx:                   ctx,
		cancel:                cancel,
		backends:              backend.NewRegistry(nopLogger, false),
		beMetricsCache:        make(map[string]*Metrics),
		closeOnBackendRemoval: false,
	}

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_proc_cancel"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_act_cancel"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bin_cancel"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bout_cancel"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_req_cancel"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_err_cancel"}),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go p.acceptLoop(mockLn, ln.Addr().String(), &wg, feMetrics)

	// Wait for at least 1 failed accept
	testutil.Eventually(t, func() bool {
		return mockLn.FailCount() >= 1
	}, 1*time.Second, 10*time.Millisecond)

	// Cancel context while it is backing off
	cancel()

	// WaitGroup MUST unblock quickly
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("acceptLoop failed to exit promptly upon context cancellation")
	}
}

func TestRedisProxy_MultipleListeners_Independence(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nopLogger := zerolog.Nop()
	p := &RedisProxy{
		id:     "test-redis-multi-ln",
		log:    nopLogger,
		ctx:    ctx,
		cancel: cancel,
	}

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_proc_redis_mln"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_act_redis_mln"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bin_redis_mln"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bout_redis_mln"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_err_redis_mln"}),
	}

	var wg1, wg2 sync.WaitGroup
	wg1.Add(1)
	wg2.Add(1)
	go p.acceptLoop(ln1, ln1.Addr().String(), &wg1, feMetrics)
	go p.acceptLoop(ln2, ln2.Addr().String(), &wg2, feMetrics)

	// Close ln1: acceptLoop for ln1 should exit cleanly without cancelling the proxy context
	// or blocking/waiting prematurely on connectionsWG.
	_ = ln1.Close()

	// Wait deterministically for ln1's acceptLoop to exit
	done1 := make(chan struct{})
	go func() {
		wg1.Wait()
		close(done1)
	}()
	select {
	case <-done1:
	case <-time.After(1 * time.Second):
		t.Fatal("ln1 acceptLoop did not exit after listener closed")
	}

	if p.ctx.Err() != nil {
		t.Fatalf("closing one listener cancelled the entire proxy context: %v", p.ctx.Err())
	}

	// Dial ln2: should connect and accept without WaitGroup misuse panic
	conn, err := net.Dial("tcp", ln2.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial active listener ln2: %v", err)
	}
	_ = conn.Close()

	cancel()
	_ = ln2.Close()
	wg2.Wait()
}

func TestRedisProxy_DedicatedReaderPool_SizeIsolation(t *testing.T) {
	p1 := &RedisProxy{
		bufferSize:          1024,
		maxReusedBufferSize: 4096,
	}
	p2 := &RedisProxy{
		bufferSize:          32768,
		maxReusedBufferSize: 65536,
	}

	// Reader from proxy 1
	r1 := p1.newProtocolReader(bytes.NewBuffer(nil))
	if r1.br.Size() != 1024 {
		t.Fatalf("expected r1 buffer size 1024, got %d", r1.br.Size())
	}
	r1.Release()

	// Reader from proxy 2 must NOT receive proxy 1's 1024-byte reader
	r2 := p2.newProtocolReader(bytes.NewBuffer(nil))
	if r2.br.Size() != 32768 {
		t.Fatalf("expected r2 buffer size 32768, got %d (polluted by proxy 1 pool)", r2.br.Size())
	}
	r2.Release()

	// Reusing from proxy 1 again must yield a 1024-byte reader
	r1Again := p1.newProtocolReader(bytes.NewBuffer(nil))
	if r1Again.br.Size() != 1024 {
		t.Fatalf("expected r1Again buffer size 1024, got %d", r1Again.br.Size())
	}
	r1Again.Release()
}

func TestRedisProxy_GetBackendList(t *testing.T) {
	// Compile-time check that RedisProxy implements backend.BackendListProvider
	var _ backend.BackendListProvider = (*RedisProxy)(nil)

	// Test with nil backends registry
	var nilProxy *RedisProxy
	if got := nilProxy.GetBackendList(); got != nil {
		t.Errorf("expected nil for nil RedisProxy, got %v", got)
	}

	p := &RedisProxy{}
	if got := p.GetBackendList(); got != nil {
		t.Errorf("expected nil for nil backends registry, got %v", got)
	}

	// Test with backends in registry
	p.backends = backend.NewRegistry(zerolog.Nop(), false)
	b1 := backend.NewBackend("127.0.0.1:6379", nil)
	b2 := backend.NewBackend("127.0.0.1:6380", nil)
	p.backends.Add(b1)
	p.backends.Add(b2)

	list := p.GetBackendList()
	if len(list) != 2 {
		t.Fatalf("expected 2 backends, got %d", len(list))
	}

	addresses := make(map[string]bool)
	for _, b := range list {
		addresses[b.Address] = true
	}
	if !addresses["127.0.0.1:6379"] || !addresses["127.0.0.1:6380"] {
		t.Errorf("unexpected backend list: %v", list)
	}
}
