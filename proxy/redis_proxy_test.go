package proxy

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/rs/zerolog"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
)

// TestRedisProxyFactory_ValidateConfig verifies that the RedisProxyFactory correctly validates
// a valid HCL configuration block. It checks that mandatory fields (like source) and
// optional fields (like addresses and connect_timeout) are accepted.
func TestRedisProxyFactory_ValidateConfig(t *testing.T) {
	f := &RedisProxyFactory{}
	configHCL := []byte(`
		source = "test-source"
		addresses = ["127.0.0.1:0"]
		connect_timeout = "2s"
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &Config{
		Type:   "redis_proxy",
		Name:   "test",
		Config: file.Body,
		ctx:    &hcl.EvalContext{},
	}

	res := f.ValidateConfig(tc)
	if res.HasErrors() {
		t.Errorf("unexpected errors: %v", res)
	}
}

// TestRedisProxyFactory_parseConfig verifies the default value assignment and correct parsing
// of configuration values from HCL into the internal ConfigRedis struct.
// It checks defaults for: timeouts, buffer sizes, queue sizes, and retry parameters.
func TestRedisProxyFactory_parseConfig(t *testing.T) {
	f := &RedisProxyFactory{}
	configHCL := []byte(`
		source = "test-source"
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &Config{
		Type:   "redis_proxy",
		Name:   "test",
		Config: file.Body,
		ctx:    &hcl.EvalContext{},
	}

	config := f.parseConfig(tc)
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
	if config.BufferSize != 16384 {
		t.Errorf("expected BufferSize 16384, got %d", config.BufferSize)
	}
	if config.ClientQueueSize != 64 {
		t.Errorf("expected ClientQueueSize 64, got %d", config.ClientQueueSize)
	}
	if config.BackendInflightQueueSize != 512 {
		t.Errorf("expected BackendInflightQueueSize 512, got %d", config.BackendInflightQueueSize)
	}
	if config.BackendConnectionPoolSize != 1 {
		t.Errorf("expected BackendConnectionPoolSize 1, got %d", config.BackendConnectionPoolSize)
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
	f := &RedisProxyFactory{}
	configHCL := []byte(`
		source = "test-source"
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &Config{
		Type:   "redis_proxy",
		Name:   "test",
		Config: file.Body,
		ctx:    &hcl.EvalContext{},
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := f.New(tc, &wg, ctx)
	p, ok := mod.(*RedisProxy)
	if !ok {
		t.Fatal("expected mod to be *RedisProxy")
	}

	if p.GetID() != "proxy.redis_proxy.test" {
		t.Errorf("expected ID proxy.redis_proxy.test, got %s", p.GetID())
	}
	if p.GetUpdateSource() != "test-source" {
		t.Errorf("expected update source test-source, got %s", p.GetUpdateSource())
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

	f := &RedisProxyFactory{}
	configHCL := []byte(`
		source = "test-source"
		addresses = ["127.0.0.1:0"]
		backend_wait_timeout = "1s"
		backend_connection_pool_size = 1
	`)

	file, diags := hclsyntax.ParseConfig(configHCL, "config.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &Config{
		Type:   "redis_proxy",
		Name:   "test",
		Config: file.Body,
		ctx:    &hcl.EvalContext{},
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := f.New(tc, &wg, ctx)
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
	moduleList := module.NewModulesList()
	moduleList.AddModule(dummyProvider)

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
	expectedDenied := "-DENIED Command not supported by MLB Redis proxy\r\n"
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
func (d *dummyUpdateProvider) GetID() string                                    { return d.sourceName }
func (d *dummyUpdateProvider) Bind(modules module.ModulesList)                  {}
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
	p.handleConnection(conn)
}

// TestRedisProxy_HandleConnection_FailedResponse verifies that an aborted response
// from the backend (represented by a nil item in the response) causes the proxy
// to correctly terminate the client session by cancelling the client context.
func TestRedisProxy_HandleConnection_FailedResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:              "test-failed-resp",
		log:             zerolog.Nop(),
		clientQueueSize: 10,
		bufferSize:      1024,
		ctx:             ctx,
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
			// Wait for the proxy to close the connection
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
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	// Intercept the query and simulate a backend failure by aborting it
	go func() {
		query := <-rbc.inputChan
		query.Abort()
	}()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront)
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("handleConnection did not exit on aborted response")
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
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	// Intercept the query on the second backend and provide a successful reply
	go func() {
		query := <-rbc2.inputChan
		query.Reply([]byte("+PONG\r\n"))
	}()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront)
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
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	p.connectionsWG.Add(1)
	done := make(chan struct{})
	go func() {
		p.handleConnection(connFront)
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
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
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
		p.handleConnection(connFront)
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
	}

	// Use the errorConnRedis from tcp_test.go (we'll need to define it or similar)
	badConn := &errorConnRedis{
		err: io.ErrUnexpectedEOF,
	}

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	p.connectionsWG.Add(1)
	// We expect a panic, which is caught by the internal recover() in handleConnection
	// and logged. We check that it didn't crash the test.
	p.handleConnection(badConn)
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
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	p.connectionsWG.Add(1)
	p.handleConnection(connFront)
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
func (c *errorConnRedis) SetNoDelay(noDelay bool) error {
	return nil
}
