package redis

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/zclconf/go-cty/cty"
)

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
	time.Sleep(100 * time.Millisecond)

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
	time.Sleep(100 * time.Millisecond)

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
	time.Sleep(100 * time.Millisecond)

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

type mockFailingListener struct {
	net.Listener
	failCount int
	maxFails  int
	mu        sync.Mutex
}

func (m *mockFailingListener) Accept() (net.Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failCount < m.maxFails {
		m.failCount++
		return nil, errors.New("simulated temporary accept error")
	}
	return m.Listener.Accept()
}

func TestRedisProxy_AcceptLoop_Backoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mockLn := &mockFailingListener{
		Listener: ln,
		maxFails: 2,
	}

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
		mockLn.mu.Lock()
		defer mockLn.mu.Unlock()
		return mockLn.failCount == 2
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

	mockLn := &mockFailingListener{
		Listener: ln,
		maxFails: 1000,
	}

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
		mockLn.mu.Lock()
		defer mockLn.mu.Unlock()
		return mockLn.failCount >= 1
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

