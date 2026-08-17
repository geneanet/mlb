package redis

import (
	"context"
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
	be := &backend.Backend{Address: "127.0.0.1:6379", Meta: backend.NewEmptyMetaMap(0)}
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
			go io.Copy(conn, conn) // simple echo
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
	be := &backend.Backend{
		Address: lnBackend.Addr().String(),
		Meta:    backend.NewEmptyMetaMap(0),
		Ctx:     beCtx,
		Cancel:  beCancel,
	}
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
			go io.Copy(conn, conn)
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
	be := &backend.Backend{
		Address: lnBackend.Addr().String(),
		Meta:    backend.NewEmptyMetaMap(0),
		Ctx:     beCtx,
		Cancel:  beCancel,
	}
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
			go io.Copy(conn, conn)
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
	be := &backend.Backend{
		Address: lnBackend.Addr().String(),
		Meta:    backend.NewEmptyMetaMap(0),
		// Ctx and Cancel are nil
	}
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

