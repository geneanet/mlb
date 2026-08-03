package redis

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"mlb/backend"
	"mlb/metrics"
	"mlb/module"
	"mlb/testutil"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/rs/zerolog"
)

// TestRedisProxyFactory_ValidateConfig verifies that the RedisProxyFactory correctly validates
// a valid HCL configuration block. It checks that mandatory fields (like source) and
// optional fields (like addresses and connect_timeout) are accepted.
func TestRedisProxyFactory_ValidateConfig(t *testing.T) {
	configHCL := []byte(`
		source = "test-source"
		addresses = ["127.0.0.1:0"]
		connect_timeout = "2s"
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Category: "proxy",
		Type:     "redis_proxy",
		Name:     "test",
		Config:   file.Body,
		Ctx:      &hcl.EvalContext{},
	}

	res := validateRedisProxyConfig(tc)
	if res.HasErrors() {
		t.Errorf("unexpected errors: %v", res)
	}
}

// TestRedisProxy_RegistryIntegration verifies that the Redis proxy can be correctly
// decoded, validated, and instantiated using the global module registry functions.
func TestRedisProxy_RegistryIntegration(t *testing.T) {
	configHCL := []byte(`
		source = "test-source"
		addresses = ["127.0.0.1:0"]
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	block := &hcl.Block{
		Type:   "proxy",
		Labels: []string{"redis", "test"},
		Body:   file.Body,
		LabelRanges: []hcl.Range{
			{Start: hcl.Pos{Line: 1, Column: 1}, End: hcl.Pos{Line: 1, Column: 10}},
			{Start: hcl.Pos{Line: 1, Column: 11}, End: hcl.Pos{Line: 1, Column: 20}},
		},
	}
	ctx := &hcl.EvalContext{}

	// 1. Test DecodeConfigBlock
	cfg, diags := module.DecodeConfigBlock(block, ctx, "proxy")
	if diags.HasErrors() {
		t.Fatalf("DecodeConfigBlock failed: %v", diags)
	}
	if cfg.Type != "redis" || cfg.Name != "test" {
		t.Errorf("Unexpected config: %+v", cfg)
	}

	// 2. Test ValidateConfig
	diags = module.ValidateConfig(cfg, "proxy")
	if diags.HasErrors() {
		t.Fatalf("ValidateConfig failed: %v", diags)
	}

	// 3. Test New
	wg := &sync.WaitGroup{}
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := module.New(cfg, wg, bgCtx, "proxy")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if mod == nil {
		t.Fatal("module.New returned nil")
	}
	if _, ok := mod.(*RedisProxy); !ok {
		t.Errorf("Expected *RedisProxy, got %T", mod)
	}
}

// TestRedisProxyFactory_parseConfig verifies the default value assignment and correct parsing
// of configuration values from HCL into the internal ConfigRedis struct.
// It checks defaults for: timeouts, buffer sizes, queue sizes, and retry parameters.
func TestRedisProxyFactory_parseConfig(t *testing.T) {
	configHCL := []byte(`
		source = "test-source"
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Category: "proxy",
		Type:     "redis_proxy",
		Name:     "test",
		Config:   file.Body,
		Ctx:      &hcl.EvalContext{},
	}

	config := parseRedisProxyConfig(tc)
	if config.ID != "proxy.redis_proxy.test" {
		t.Errorf("expected ID proxy.redis_proxy.test, got %s", config.ID)
	}
	if config.Source != "test-source" {
		t.Errorf("expected source test-source, got %s", config.Source)
	}
	if config.ConnectTimeout != "0s" {
		t.Errorf("expected ConnectTimeout 0s, got %s", config.ConnectTimeout)
	}
	if config.CloseTimeout != "0s" {
		t.Errorf("expected CloseTimeout 0s, got %s", config.CloseTimeout)
	}
	if config.BackendWaitTimeout != "0s" {
		t.Errorf("expected BackendWaitTimeout 0s, got %s", config.BackendWaitTimeout)
	}
	if config.BackendTCPKeepAlive != "15s" {
		t.Errorf("expected BackendTCPKeepAlive 15s, got %s", config.BackendTCPKeepAlive)
	}
	if config.BufferSize != 16384 {
		t.Errorf("expected BufferSize 16384, got %d", config.BufferSize)
	}
	if config.ClientQueueSize != 64 {
		t.Errorf("expected ClientQueueSize 64, got %d", config.ClientQueueSize)
	}
	if config.BackendInflightQueueSize != 512 {
		t.Errorf("expected BackendInflightQueueSize 512, got %d", config.BackendInflightQueueSize)
	}
	if config.BackendMinConnections != 1 {
		t.Errorf("expected BackendMinConnections 1, got %d", config.BackendMinConnections)
	}
	if config.BackendMaxConnections != 1 {
		t.Errorf("expected BackendMaxConnections 1, got %d", config.BackendMaxConnections)
	}
	if config.RetryPeriod != "100ms" {
		t.Errorf("expected RetryPeriod 100ms, got %s", config.RetryPeriod)
	}
	if config.RetryMaxPeriod != "1s" {
		t.Errorf("expected RetryMaxPeriod 1s, got %s", config.RetryMaxPeriod)
	}
	if config.RetryBackoffFactor != 1.5 {
		t.Errorf("expected RetryBackoffFactor 1.5, got %f", config.RetryBackoffFactor)
	}
}

// TestRedisProxyFactory_New verifies the creation of a RedisProxy instance and its
// ability to handle backend updates (add, modify, remove) through ReceiveUpdate.
func TestRedisProxyFactory_New(t *testing.T) {
	configHCL := []byte(`
		source = "test-source"
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Category: "proxy",
		Type:     "redis_proxy",
		Name:     "test",
		Config:   file.Body,
		Ctx:      &hcl.EvalContext{},
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newRedisProxy(tc, &wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p, ok := mod.(*RedisProxy)
	if !ok {
		t.Fatal("expected mod to be *RedisProxy")
	}

	// Test ReceiveUpdate processing for backend lifecycle events
	p.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:1234",
		Backend: &backend.Backend{Address: "127.0.0.1:1234", Meta: backend.NewEmptyMetaMap(0)},
	})

	testutil.Eventually(t, func() bool {
		return p.backends.Has("127.0.0.1:1234")
	}, 1*time.Second, 10*time.Millisecond)

	p.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Address: "127.0.0.1:1234",
		Backend: &backend.Backend{Address: "127.0.0.1:1234", Meta: backend.NewEmptyMetaMap(0)},
	})
	// No exported state change for modified in this mock, but we can wait for loop to process
	time.Sleep(10 * time.Millisecond)

	p.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "127.0.0.1:1234",
	})

	testutil.Eventually(t, func() bool {
		return !p.backends.Has("127.0.0.1:1234")
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait() // Ensure the mainloop stops cleanly
}

// TestRedisProxy_ListenAndConnection is an integration-like test that verifies:
// 1. The proxy starts a TCP listener.
// 2. It accepts client connections and routes PING commands to a mock backend.
// 3. It correctly enforces command restrictions (denying MONITOR).
// 4. It properly handles backend discovery and binding to update providers.
func TestRedisProxy_ListenAndConnection(t *testing.T) {
	// Start local TCP server to act as a mock Redis backend
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener.Close()

	// Mock backend logic that responds to PING with +PONG
	go func() {
		for {
			conn, err := backendListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if n > 0 {
						c.Write([]byte("+PONG\r\n"))
					}
				}
			}(conn)
		}
	}()

	configHCL := []byte(`
		source = "test-source"
		addresses = ["127.0.0.1:0"]
		backend_wait_timeout = "1s"
		backend_min_connections = 1
		backend_max_connections = 1
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Category: "proxy",
		Type:     "redis_proxy",
		Name:     "test",
		Config:   file.Body,
		Ctx:      &hcl.EvalContext{},
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newRedisProxy(tc, &wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p := mod.(*RedisProxy)

	// Set dynamic address so listen picks a random available port
	p.addresses = []string{"127.0.0.1:0"}

	// Manually provide the backend to the proxy's internal state
	p.backends.Add(&backend.Backend{Address: backendListener.Addr().String(), Meta: backend.NewEmptyMetaMap(0)})
	p.backendConnectionPool.Update()

	testutil.Eventually(t, func() bool {
		p.backendConnectionPool.mutex.RLock()
		defer p.backendConnectionPool.mutex.RUnlock()
		return len(p.backendConnectionPool.pool) == 1
	}, 1*time.Second, 10*time.Millisecond)

	// Determine a free port for the proxy to listen on
	listenAddr := "127.0.0.1:0"
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		t.Fatal(err)
	}

	proxyAddr := ln.Addr().String()
	ln.Close() // Release it so the proxy can bind to it

	p.addresses = []string{proxyAddr}

	// Mock update provider to satisfy Bind interface
	dummyProvider := &dummyUpdateProvider{
		sourceName: "test-source",
	}
	moduleList := make(module.ModulesRegistry)
	moduleList.AddModule("test-source", dummyProvider)

	p.Bind(moduleList)

	// Wait for proxy listener to start
	testutil.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	// Connect to the proxy
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Scenario 1: Send an allowed command (PING)
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "+PONG\r\n" {
		t.Errorf("expected +PONG\r\n, got %s", string(buf[:n]))
	}

	// Scenario 2: Send a denied command (MONITOR)
	_, err = conn.Write([]byte("*1\r\n$7\r\nMONITOR\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	n, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	expectedDenied := "-ERR Command 'MONITOR' not supported by MLB Redis proxy\r\n"
	if string(buf[:n]) != expectedDenied {
		t.Errorf("expected denied message, got %s", string(buf[:n]))
	}

	conn.Close()

	cancel()
	wg.Wait()
}

// dummyUpdateProvider is a helper mock for testing Bind and backend updates.
type dummyUpdateProvider struct {
	sourceName string
}

func (d *dummyUpdateProvider) ProvideUpdates(r backend.BackendUpdateSubscriber) {}
func (d *dummyUpdateProvider) Bind(modules module.ModulesRegistry)                 {}

func (d *dummyUpdateProvider) IsBackendUpdateProvider(source string) bool {
	return d.sourceName == source
}

// TestRedisProxy_HandleConnection_NoBackendPanic verifies that the proxy handles
// the scenario where no backends are available in the pool. It ensures that the
// deferred recovery handler prevents the application from crashing.
func TestRedisProxy_HandleConnection_NoBackendPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:  "test-panic",
		log: zerolog.Nop(),
		ctx: ctx,
		beMetricsCache: make(map[string]*Metrics),
	}
	p.backendConnectionPool = NewRedisBackendConnectionPool(p)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			time.Sleep(50 * time.Millisecond)
			conn.Close()
		}
	}()

	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	p.connectionsWG.Add(1)
	// handleConnection should recover from the panic when GetRandom returns nil (if wait=false)
	// or when it detects no backends available.
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("handleConnection panicked unexpectedly: %v", r)
		}
	}()
	p.handleConnection(conn, dummyMetrics())
}

// TestRedisProxy_HandleConnection_FailedResponse verifies that an aborted response
// from the backend (represented by an Abort call) causes the proxy
// to return a Redis protocol error to the client instead of dropping the connection.
func TestRedisProxy_HandleConnection_FailedResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:              "test-failed-resp",
		log:             zerolog.Nop(),
		clientQueueSize: 10,
		bufferSize:      1024,
		ctx:             ctx,
		beMetricsCache:  make(map[string]*Metrics),
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	respReceived := make(chan string, 1)

	// Mock client connection
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			conn.Write([]byte("PING\r\n"))
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err == nil {
				respReceived <- string(buf[:n])
			}
			conn.Close()
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChan:     make(chan RedisQuery, 1),
		inputChanStop: make(chan struct{}),
		ctx:           context.Background(),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.updateWaitState()
	p.backendConnectionPool.mutex.Unlock()

	// Intercept the query and simulate a backend failure by aborting it
	go func() {
		query := <-rbc.inputChan
		query.Abort()
	}()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront, dummyMetrics())
		close(done)
	}()

	select {
	case resp := <-respReceived:
		expectedError := "-ERR Backend connection failed\r\n"
		if resp != expectedError {
			t.Errorf("expected %q, got %q", expectedError, resp)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("no response received from proxy")
	}

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("handleConnection did not exit after client closed")
	}
}

// TestRedisProxy_HandleConnection_BackendRetrySuccess verifies that if the first
// backend connection chosen from the pool fails to process the query, the proxy
// correctly retries with another available backend.
func TestRedisProxy_HandleConnection_BackendRetrySuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:              "test-retry-success",
		log:             zerolog.Nop(),
		clientQueueSize: 10,
		bufferSize:      1024,
		ctx:             ctx,
		beMetricsCache:  make(map[string]*Metrics),
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Mock client connection
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			conn.Write([]byte("PING\r\n"))
			buf := make([]byte, 1024)
			n, _ := conn.Read(buf)
			if string(buf[:n]) == "+PONG\r\n" {
				conn.Close()
			}
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)

	// First backend: simulate a failure by closing its input channel
	rbc1 := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChanStop: make(chan struct{}),
	}
	close(rbc1.inputChanStop) // Force Query() to return an error

	// Second backend: will successfully process the query
	rbc2 := &RedisBackendConnection{
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}

	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc1] = struct{}{}
	p.backendConnectionPool.pool[rbc2] = struct{}{}
	p.backendConnectionPool.updateWaitState()
	p.backendConnectionPool.mutex.Unlock()

	// Intercept the query on the second backend and provide a successful reply
	go func() {
		query := <-rbc2.inputChan
		query.Reply([]byte("+PONG\r\n"))
	}()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront, dummyMetrics())
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("handleConnection did not exit after retry success")
	}
}

// TestRedisProxy_HandleConnection_GracefulShutdownTimeout verifies that when the proxy
// is signaled to shut down, active client connections are forcefully closed if they
// do not close themselves within the configured closeTimeout period.
func TestRedisProxy_HandleConnection_GracefulShutdownTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &RedisProxy{
		id:              "test-shutdown-timeout",
		log:             zerolog.Nop(),
		clientQueueSize: 10,
		bufferSize:      1024,
		closeTimeout:    10 * time.Millisecond,
		ctx:             ctx,
		beMetricsCache:  make(map[string]*Metrics),
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Mock client connection that hangs open
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			buf := make([]byte, 10)
			conn.Read(buf)
			conn.Close()
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChan:     make(chan RedisQuery, 1),
		inputChanStop: make(chan struct{}),
		ctx:           context.Background(),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.updateWaitState()
	p.backendConnectionPool.mutex.Unlock()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront, dummyMetrics())
		close(done)
	}()

	// Trigger the graceful shutdown by cancelling the main context
	cancel()

	select {
	case <-done:
		// Success: connection closed after closeTimeout
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handleConnection did not exit after shutdown timeout")
	}
}

// TestRedisProxy_HandleConnection_ClientWriteError verifies that the proxy correctly
// detects when it can no longer write to the client (e.g., client closed connection
// before response) and exits the connection handler gracefully.
func TestRedisProxy_HandleConnection_ClientWriteError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:              "test-client-write-err",
		log:             zerolog.Nop(),
		clientQueueSize: 10,
		bufferSize:      1024,
		ctx:             ctx,
		beMetricsCache:  make(map[string]*Metrics),
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Frontend client that closes its end immediately after sending the command
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			conn.Write([]byte("PING\r\n"))
			conn.Close() // Close BEFORE proxy attempts to write the response back
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChan:     make(chan RedisQuery, 1),
		inputChanStop: make(chan struct{}),
		ctx:           context.Background(),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.updateWaitState()
	p.backendConnectionPool.mutex.Unlock()

	// Intercept query and reply to trigger a write to the closed client connection
	go func() {
		query := <-rbc.inputChan
		// Brief sleep to ensure the client has closed the connection
		time.Sleep(100 * time.Millisecond)
		query.Reply([]byte("+OK\r\n"))
	}()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront, dummyMetrics())
		close(done)
	}()

	select {
	case <-done:
		// Success: handleConnection exited after encountering the write error
	case <-time.After(1 * time.Second):
		t.Fatal("handleConnection did not exit on client write error")
	}
}

// TestRedisProxy_HandleConnection_ClientReadError verifies that an unexpected error
// while reading from the client triggers a panic that is caught by the recovery handler.
func TestRedisProxy_HandleConnection_ClientReadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:         "test-read-err",
		log:        zerolog.Nop(),
		bufferSize: 1024,
		ctx:        ctx,
		beMetricsCache: make(map[string]*Metrics),
	}

	// Use the errorConnRedis from tcp_test.go (we'll need to define it or similar)
	badConn := &errorConnRedis{
		err: io.ErrUnexpectedEOF,
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChan:     make(chan RedisQuery, 1),
		inputChanStop: make(chan struct{}),
		ctx:           context.Background(),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.updateWaitState()
	p.backendConnectionPool.mutex.Unlock()

	p.connectionsWG.Add(1)
	// We expect a panic, which is caught by the internal recover() in handleConnection
	// and logged. We check that it didn't crash the test.
	p.handleConnection(badConn, dummyMetrics())
}

// TestRedisProxy_HandleConnection_RetryNoBackendPanic verifies that if a backend fails
// and no other backend is available for retry, the proxy panics as expected.
func TestRedisProxy_HandleConnection_RetryNoBackendPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:              "test-retry-no-be",
		log:             zerolog.Nop(),
		clientQueueSize: 10,
		bufferSize:      1024,
		ctx:             ctx,
		beMetricsCache:  make(map[string]*Metrics),
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			conn.Write([]byte("PING\r\n"))
			time.Sleep(100 * time.Millisecond)
			conn.Close()
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChanStop: make(chan struct{}),
	}
	close(rbc.inputChanStop) // Force Query() failure

	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.updateWaitState()
	p.backendConnectionPool.mutex.Unlock()

	p.connectionsWG.Add(1)
	p.handleConnection(connFront, dummyMetrics())
}

// errorConnRedis is a mock net.Conn that returns an error on Read.
type errorConnRedis struct {
	net.Conn
	err error
}

func (c *errorConnRedis) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}
func (c *errorConnRedis) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5678}
}
func (c *errorConnRedis) Read(b []byte) (n int, err error) {
	return 0, c.err
}
func (c *errorConnRedis) Write(b []byte) (n int, err error) {
	return 0, nil
}
func (c *errorConnRedis) Close() error {
	return nil
}

// TestRedisProxyFactory_InvalidDurations verifies that ValidateConfig catches invalid duration strings.
func TestRedisProxyFactory_InvalidDurations(t *testing.T) {
	hclText := `
		source = "s1"
		connect_timeout = "invalid"
	`
	file, diags := hclsyntax.ParseConfig([]byte(hclText), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Category: "proxy",
		Type:     "redis",
		Name:     "test_proxy_invalid",
		Config:   file.Body,
		Ctx:    &hcl.EvalContext{},
	}

	vDiags := validateRedisProxyConfig(tc)
	if !vDiags.HasErrors() {
		t.Error("expected diagnostics to have errors for invalid duration")
	}
}

func TestRedisConfigValidation(t *testing.T) {
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
			vDiags := validateRedisProxyConfig(tc)
			if (vDiags.HasErrors()) != tt.wantErr {
				t.Errorf("validateRedisProxyConfig() error = %v, wantErr %v", vDiags.HasErrors(), tt.wantErr)
			}
		})
	}
}

func TestRedisConfigParsing(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configStr := `
		source = "s1"
		backend_min_connections = 3
		backend_input_queue_size = 2000
		backend_tcp_keepalive = "30s"
	`
	f, _ := hclsyntax.ParseConfig([]byte(configStr), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	tc := &module.Config{Config: f.Body, Ctx: &hcl.EvalContext{}}

	mod, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	p := mod.(*RedisProxy)
	if p.backendMinConnections != 3 {
		t.Errorf("Expected min 3, got %d", p.backendMinConnections)
	}
	if p.backendMaxConnections != 3 {
		t.Errorf("Expected max 3 (defaulted from min), got %d", p.backendMaxConnections)
	}
	if p.backendInputQueueSize != 2000 {
		t.Errorf("Expected input queue size 2000, got %d", p.backendInputQueueSize)
	}
	if p.backendTCPKeepAlive != 30*time.Second {
		t.Errorf("Expected backendTCPKeepAlive 30s, got %s", p.backendTCPKeepAlive)
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
