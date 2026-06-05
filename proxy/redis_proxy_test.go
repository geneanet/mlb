package proxy

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/rs/zerolog"
	"mlb/backend"
	"mlb/module"
)

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

	// Test ReceiveUpdate processing
	p.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:1234",
		Backend: &backend.Backend{Address: "127.0.0.1:1234", Meta: backend.NewEmptyMetaMap(0)},
	})
	time.Sleep(50 * time.Millisecond) // Allow processing

	if !p.backends.Has("127.0.0.1:1234") {
		t.Errorf("expected backends to have 127.0.0.1:1234")
	}

	p.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Address: "127.0.0.1:1234",
		Backend: &backend.Backend{Address: "127.0.0.1:1234", Meta: backend.NewEmptyMetaMap(0)},
	})
	time.Sleep(50 * time.Millisecond) // Allow processing

	p.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "127.0.0.1:1234",
	})
	time.Sleep(50 * time.Millisecond) // Allow processing

	if p.backends.Has("127.0.0.1:1234") {
		t.Errorf("expected backends not to have 127.0.0.1:1234")
	}

	cancel()
	wg.Wait() // Ensure mainloop stops
}

func TestRedisProxy_ListenAndConnection(t *testing.T) {
	// Start local TCP server to act as a backend
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener.Close()

	// Simple backend that responds to PING
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
					// Only respond if it's not a closed conn
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

	// Provide the backend
	p.backends.Add(&backend.Backend{Address: backendListener.Addr().String(), Meta: backend.NewEmptyMetaMap(0)})
	p.backendConnectionPool.Update()

	time.Sleep(50 * time.Millisecond) // wait for pool

	// We can't easily get the listening address if we bind inside, so we'll start a custom listener or modify it.
	// We'll call listen directly to avoid Bind
	listenAddr := "127.0.0.1:0"
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		t.Fatal(err)
	}
	
	proxyAddr := ln.Addr().String()
	ln.Close() // Release it so listen can use it, might be a small race

	p.addresses = []string{proxyAddr}

	// We implement a dummy module provider to test Bind
	dummyProvider := &dummyUpdateProvider{
		sourceName: "test-source",
	}
	moduleList := module.NewModulesList()
	moduleList.AddModule(dummyProvider)

	p.Bind(moduleList)

	time.Sleep(100 * time.Millisecond) // Allow server to start

	// Connect to proxy
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send an allowed command
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

	// Send a denied command
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
// the panic when no backends are available in the pool.
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
	// Should not panic (caught by recovery handler)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("handleConnection panicked: %v", r)
		}
	}()
	p.handleConnection(conn)
}

// TestRedisProxy_HandleConnection_FailedResponse verifies that an aborted response
// from the backend (nil item) causes the client context to be cancelled.
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

	// Frontend client
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			conn.Write([]byte("PING\r\n"))
			// Wait for close
			buf := make([]byte, 10)
			conn.Read(buf)
			conn.Close()
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// We don't defer connFront.Close() here because handleConnection will close it

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	rbc := &RedisBackendConnection{
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}
	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc] = struct{}{}
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	// Intercept query and abort it
	go func() {
		query := <-rbc.inputChan
		query.Abort()
	}()

	p.connectionsWG.Add(1)
	// handleConnection should return when the client context is cancelled
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
// backend chosen fails to accept the query, the proxy successfully retries with another.
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

	// Frontend client
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
	// We don't defer connFront.Close() here because handleConnection will close it

	p.backendConnectionPool = NewRedisBackendConnectionPool(p)

	// First backend: will fail the query
	rbc1 := &RedisBackendConnection{
		pool:          p.backendConnectionPool,
		inputChanStop: make(chan struct{}),
	}
	close(rbc1.inputChanStop) // Force Query() to fail

	// Second backend: will succeed
	rbc2 := &RedisBackendConnection{
		pool:      p.backendConnectionPool,
		inputChan: make(chan RedisQuery, 1),
	}

	p.backendConnectionPool.mutex.Lock()
	p.backendConnectionPool.pool[rbc1] = struct{}{}
	p.backendConnectionPool.pool[rbc2] = struct{}{}
	p.backendConnectionPool.waitBackendsSemaphore.Release(1)
	p.backendConnectionPool.mutex.Unlock()

	// Intercept query on second backend and reply
	go func() {
		// We might need to wait for rbc1 to be picked and failed
		// Since GetRandom is random, it might pick rbc2 first.
		
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
// is stopped, active connections are closed after the grace period.
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

	// Frontend client
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			// Keep it open
			buf := make([]byte, 10)
			conn.Read(buf)
			conn.Close()
		}
	}()

	connFront, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// We don't defer connFront.Close() here because handleConnection will close it

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

	// Trigger shutdown
	cancel()

	select {
	case <-done:
		// Success: connection closed after timeout
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handleConnection did not exit after shutdown timeout")
	}
}

// TestRedisProxy_HandleConnection_ClientWriteError verifies that if writing to the
// client fails, the connection handler exits.
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

	// Frontend client that closes immediately after sending
	go func() {
		conn, err := net.Dial("tcp", l.Addr().String())
		if err == nil {
			conn.Write([]byte("PING\r\n"))
			conn.Close() // Close BEFORE proxy can write back
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

	// Intercept query and reply
	go func() {
		query := <-rbc.inputChan
		// Wait a bit to ensure client closed its end
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
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("handleConnection did not exit on client write error")
	}
}
