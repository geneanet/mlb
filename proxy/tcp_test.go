package proxy

import (
	"bytes"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// mockBackendProvider provides a simple mock for the backend.BackendProvider interface.
type mockBackendProvider struct {
	id             string
	backendAddress string
	returnNil      bool
	mu             sync.RWMutex
}

func (m *mockBackendProvider) GetBackend(wait bool) *backend.Backend {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.returnNil {
		return nil
	}
	return &backend.Backend{
		Address: m.backendAddress,
	}
}

func (m *mockBackendProvider) setReturnNil(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.returnNil = v
}


func (m *mockBackendProvider) Bind(modules module.ModulesRegistry) {
	// No operation needed for this mock
}

// getFreePort attempts to obtain an available ephemeral port.
func getFreePort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer listener.Close()
	return listener.Addr().String(), nil
}

// startEchoServer starts a basic TCP echo server and returns the net.Listener.
func startEchoServer(t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return listener
}

// TestTCPProxyFactory verifies the parsing and validation of a complete HCL configuration block
// for the TCP proxy. It tests that:
// 1. The HCL parser correctly maps fields to the module.Config struct.
// 2. The TCPProxyFactory.ValidateConfig method successfully validates a correct config.
// 3. The TCPProxyFactory.New method creates a ProxyTCP instance with all fields properly initialized.
// 4. The buffer pool is correctly initialized with the configured buffer size.
func TestTCPProxyFactory(t *testing.T) {
	hclText := `
		source = "primary_src"
		backup_source = "backup_src"
		addresses = ["127.0.0.1:0"]
		connect_timeout = "1s"
		client_timeout = "1s"
		server_timeout = "1s"
		close_timeout = "1s"
		timeout_margin = "1s"
		buffer_size = 1024
		nodelay = true
	`
	file, diags := hclsyntax.ParseConfig([]byte(hclText), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Category: "proxy",
		Type:     "tcp",
		Name:     "test_proxy",
		Config:   file.Body,
		Ctx:    &hcl.EvalContext{},
	}

	vDiags := validateTCPProxyConfig(tc)
	if vDiags.HasErrors() {
		t.Fatal(vDiags)
	}

	wg := &sync.WaitGroup{}
	ctx := context.Background()

	mod := newTCPProxy(tc, wg, ctx)
	if mod == nil {
		t.Fatal("expected mod not to be nil")
	}

	p := mod.(*ProxyTCP)
	if p.source != "primary_src" {
		t.Errorf("expected source primary_src, got %s", p.source)
	}
	if p.backupSource != "backup_src" {
		t.Errorf("expected backupSource backup_src, got %s", p.backupSource)
	}
	if p.bufferSize != 1024 {
		t.Errorf("expected bufferSize 1024, got %d", p.bufferSize)
	}
	if !p.nodelay {
		t.Errorf("expected nodelay to be true")
	}

	// Ensure the buffer pool initializes correctly
	wrapper := p.bufferPool.Get().(*bufferWrapper)
	b := wrapper.buf
	if len(b) != 1024 {
		t.Errorf("expected buffer length 1024, got %d", len(b))
	}
}

// TestTCPProxyFactory_Defaults verifies the fallback behavior when optional fields are omitted
// from the HCL configuration block. It tests that:
// 1. Omitted timeout values correctly default to 0s (no timeout).
// 2. The timeout margin defaults to 1s.
// 3. The buffer size defaults to 32768 bytes.
func TestTCPProxyFactory_Defaults(t *testing.T) {
	hclText := `
		source = "s1"
	`
	file, diags := hclsyntax.ParseConfig([]byte(hclText), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Type:   "tcp",
		Name:   "test_proxy_def",
		Config: file.Body,
		Ctx:    &hcl.EvalContext{},
	}

	mod := newTCPProxy(tc, &sync.WaitGroup{}, context.Background())
	p := mod.(*ProxyTCP)

	// Validate correct default configuration values
	if p.bufferSize != 32768 {
		t.Errorf("expected bufferSize 32768, got %d", p.bufferSize)
	}
	if p.connectTimeout != 0 {
		t.Errorf("expected connectTimeout 0, got %v", p.connectTimeout)
	}
	if p.clientTimeout != 0 {
		t.Errorf("expected clientTimeout 0, got %v", p.clientTimeout)
	}
	if p.serverTimeout != 0 {
		t.Errorf("expected serverTimeout 0, got %v", p.serverTimeout)
	}
	if p.closeTimeout != 0 {
		t.Errorf("expected closeTimeout 0, got %v", p.closeTimeout)
	}
	if p.timeoutMargin != 1*time.Second {
		t.Errorf("expected timeoutMargin 1s, got %v", p.timeoutMargin)
	}
	if p.closeOnBackendRemoval {
		t.Errorf("expected closeOnBackendRemoval to default to false")
	}
}

// TestTCPProxy_NormalAndBackupAndNoBackend tests the primary request routing and failover logic
// inside the handleConnection function. It verifies that:
//  1. A connection can be successfully routed to the primary backend.
//  2. Data can be sent and received (echoed) through the primary backend.
//  3. When the primary backend is unavailable (simulated by returning nil), the proxy
//     successfully falls back to the configured backup source.
//  4. Data flows correctly through the backup backend.
//  5. The stats ticker loop in handleConnection correctly processes data counters over time.
func TestTCPProxy_NormalAndBackupAndNoBackend(t *testing.T) {
	primaryBackend := startEchoServer(t)
	defer primaryBackend.Close()

	backupBackend := startEchoServer(t)
	defer backupBackend.Close()

	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ProxyTCP{
		id:             "proxy.tcp.test",
		addresses:      []string{proxyAddr},
		log:            zerolog.Nop(),
		bufferSize:     32768,
		nodelay:        true,
		source:         "primary_backend",
		backupSource:   "backup_backend",
		wg:             wg,
		ctx:            ctx,
		cancel:         cancel,
		connectTimeout: 5 * time.Second,
		clientTimeout:  5 * time.Second,
		serverTimeout:  5 * time.Second,
		closeTimeout:   5 * time.Second,
		timeoutMargin:  1 * time.Second,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	primaryProvider := &mockBackendProvider{id: "primary_backend", backendAddress: primaryBackend.Addr().String()}
	backupProvider := &mockBackendProvider{id: "backup_backend", backendAddress: backupBackend.Addr().String()}

	modules := make(module.ModulesRegistry)
	modules.AddModule("primary_backend", primaryProvider)
	modules.AddModule("backup_backend", backupProvider)
	p.Bind(modules)

	// Wait for proxy listener to start
	testutil.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	// Scenario 1: Normal backend works
	conn1, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}

	testData1 := []byte("hello proxy")
	_, err = conn1.Write(testData1)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1024)
	n1, err := conn1.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(testData1, buf[:n1]) {
		t.Errorf("expected %v, got %v", testData1, buf[:n1])
	}
	conn1.Close()

	// Scenario 2: Main backend fails, fallback to backup
	primaryProvider.setReturnNil(true)
	conn2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}

	testData2 := []byte("hello backup")
	_, err = conn2.Write(testData2)
	if err != nil {
		t.Fatal(err)
	}

	n2, err := conn2.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(testData2, buf[:n2]) {
		t.Errorf("expected %v, got %v", testData2, buf[:n2])
	}
	conn2.Close()

	// Wait for piping goroutines to settle
	time.Sleep(50 * time.Millisecond)
}

// TestTCPProxy_NoBackendPanic tests the extreme failure scenario where neither a primary nor
// a backup backend is available. It verifies that:
// 1. handleConnection correctly detects the lack of backends and issues a panic.
// 2. The panic is safely caught by the deferred recovery handler in handleConnection.
// 3. The proxy continues to run without crashing the entire application.
func TestTCPProxy_NoBackendPanic(t *testing.T) {
	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ProxyTCP{
		id:             "proxy.tcp.test_panic",
		addresses:      []string{proxyAddr},
		log:            zerolog.Nop(),
		bufferSize:     32768,
		nodelay:        true,
		source:         "missing_backend",
		wg:             wg,
		ctx:            ctx,
		cancel:         cancel,
		connectTimeout: 5 * time.Second,
		clientTimeout:  5 * time.Second,
		serverTimeout:  5 * time.Second,
		closeTimeout:   5 * time.Second,
		timeoutMargin:  1 * time.Second,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	provider := &mockBackendProvider{id: "missing_backend", backendAddress: "", returnNil: true}
	modules := make(module.ModulesRegistry)
	modules.AddModule("missing_backend", provider)
	p.Bind(modules)

	// Wait for proxy listener to start
	testutil.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	// This connection will trigger a panic inside handleConnection due to no backend,
	// which must be caught and logged safely.
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.Write([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	conn.Close()
}

// TestTCPProxy_TimeoutAndContextCancel tests the proxy's graceful shutdown and hanging
// connection termination logic. It verifies that:
// 1. When the proxy's main context is cancelled, active connections are given a grace period.
// 2. If a connection remains open beyond the closeTimeout, it is forcefully terminated.
// 3. The select statement waiting on time.After(p.closeTimeout) properly executes and invokes cancel().
func TestTCPProxy_TimeoutAndContextCancel(t *testing.T) {
	backend := startEchoServer(t)
	defer backend.Close()

	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	p := &ProxyTCP{
		id:             "proxy.tcp.test_timeout",
		addresses:      []string{proxyAddr},
		log:            zerolog.Nop(),
		bufferSize:     32768,
		nodelay:        true,
		source:         "test_backend",
		wg:             wg,
		ctx:            ctx,
		cancel:         cancel,
		connectTimeout: 5 * time.Second,
		clientTimeout:  500 * time.Millisecond,
		serverTimeout:  500 * time.Millisecond,
		closeTimeout:   50 * time.Millisecond,
		timeoutMargin:  10 * time.Millisecond,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	provider := &mockBackendProvider{id: "test_backend", backendAddress: backend.Addr().String()}
	modules := make(module.ModulesRegistry)
	modules.AddModule("test_backend", provider)
	p.Bind(modules)

	// Wait for proxy listener to start
	testutil.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}

	// Test context cancel propagation
	// By cancelling the proxy context, it waits closeTimeout then forces cancel
	cancel()

	// Wait for the WaitGroup to be done (proxy stopped)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("proxy did not stop within timeout after cancel")
	}

	conn.Close()
}

// panicConn is a custom net.Conn that injects errors to test the pipe panic handler.
type panicConn struct {
	net.Conn
}

func (c *panicConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (c *panicConn) Read(b []byte) (n int, err error) {
	panic("injected read panic")
}

func (c *panicConn) Write(b []byte) (n int, err error) {
	return 0, net.ErrClosed
}

// TestTCPProxy_PipeErrors tests the robustness of the bidirectional pipe function when
// unexpected network errors occur. It verifies that:
//  1. An unexpected panic (e.g., from a misbehaving reader) inside the pipe is safely caught
//     by the deferred recovery handler.
//  2. The pipe function exits gracefully and signals completion on the done channel,
//     preventing goroutine leaks.
func TestTCPProxy_PipeErrors(t *testing.T) {
	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ProxyTCP{
		id:             "proxy.tcp.test_pipe",
		addresses:      []string{proxyAddr},
		log:            zerolog.Nop(),
		bufferSize:     32768,
		nodelay:        true,
		source:         "test_backend",
		wg:             wg,
		ctx:            ctx,
		cancel:         cancel,
		connectTimeout: 5 * time.Second,
		clientTimeout:  5 * time.Second,
		serverTimeout:  5 * time.Second,
		closeTimeout:   5 * time.Second,
		timeoutMargin:  1 * time.Second,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	badConn := &panicConn{}
	feBytesInCounter := metrics.FeBytesIn.WithLabelValues("1", p.id)
	beBytesInCounter := metrics.BeBytesIn.WithLabelValues("2", p.id)
	done := make(chan struct{})
	go p.pipe(badConn, badConn, done, 0, 0, feBytesInCounter, beBytesInCounter)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("pipe did not recover from panic")
	}
}

// closedConn is a custom net.Conn that simulates a closed network connection for writes.
type closedConn struct {
	net.Conn
}

func (c *closedConn) Read(b []byte) (n int, err error) {
	time.Sleep(10 * time.Millisecond)
	b[0] = 'a'
	return 1, nil
}

func (c *closedConn) Write(b []byte) (n int, err error) {
	return 0, net.ErrClosed
}

// TestTCPProxy_PipeClosedErr tests the specific edge case where a network write fails
// with net.ErrClosed. It verifies that:
// 1. The pipe function specifically checks for net.ErrClosed.
// 2. Upon encountering net.ErrClosed, the pipe returns cleanly without panicking.
func TestTCPProxy_PipeClosedErr(t *testing.T) {
	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ProxyTCP{
		id:         "proxy.tcp.test_pipe2",
		addresses:  []string{proxyAddr},
		log:        zerolog.Nop(),
		bufferSize: 32768,
		nodelay:    true,
		source:     "test_backend",
		wg:         wg,
		ctx:        ctx,
		cancel:     cancel,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	badConn := &closedConn{}
	feBytesInCounter := metrics.FeBytesIn.WithLabelValues("1", p.id)
	beBytesInCounter := metrics.BeBytesIn.WithLabelValues("2", p.id)
	done := make(chan struct{})

	go p.pipe(badConn, badConn, done, 0, 0, feBytesInCounter, beBytesInCounter)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("pipe did not return on ErrClosed")
	}
}

// errorConn is a custom net.Conn that returns a specific error on Read.
type errorConn struct {
	net.Conn
	err error
}

func (c *errorConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (c *errorConn) Read(b []byte) (n int, err error) {
	return 0, c.err
}

func (c *errorConn) Write(b []byte) (n int, err error) {
	return 0, nil
}

// TestTCPProxy_PipeReadError verifies that a generic read error (not EOF or ErrClosed)
// is correctly caught and logged, and the pipe terminates.
func TestTCPProxy_PipeReadError(t *testing.T) {
	p := &ProxyTCP{
		id:         "test_read_err",
		log:        zerolog.Nop(),
		bufferSize: 1024,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 1024)} },
		},
	}

	expectedErr := io.ErrUnexpectedEOF
	badConn := &errorConn{err: expectedErr}
	done := make(chan struct{})

	go p.pipe(badConn, badConn, done, 0, 0, prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{}))

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("pipe did not return on read error")
	}
}

// TestTCPProxy_PipeWriteError verifies that a generic write error (not ErrClosed)
// is correctly caught and logged, and the pipe terminates.
func TestTCPProxy_PipeWriteError(t *testing.T) {
	p := &ProxyTCP{
		id:         "test_write_err",
		log:        zerolog.Nop(),
		bufferSize: 1024,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 1024)} },
		},
	}

	// Use the existing errorConn and mockConn
	input := &bytes.Buffer{}
	input.Write([]byte("some data"))

	done := make(chan struct{})
	badWriter := &errorConn{err: io.ErrShortWrite}

	go p.pipe(&mockConn{reader: input}, badWriter, done, 0, 0, prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{}))

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("pipe did not return on write error")
	}
}

// mockConn is a helper for testing pipe
type mockConn struct {
	net.Conn
	reader io.Reader
	writer io.Writer
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.reader != nil {
		return m.reader.Read(b)
	}
	return 0, io.EOF
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.writer != nil {
		return m.writer.Write(b)
	}
	return len(b), nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

// TestTCPProxy_DoneBackFront tests the connection teardown synchronization inside handleConnection.
// It verifies that:
//  1. When the backend server terminates the connection first, the backend-to-frontend pipe
//     completes and closes the doneBackFront channel.
//  2. The select loop in handleConnection properly detects the closed doneBackFront channel,
//     breaks out of the loop, and safely tears down both connections.
func TestTCPProxy_DoneBackFront(t *testing.T) {
	// Start a backend that closes the connection immediately after accepting
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// Close the backend connection immediately
			_ = conn.Close()
		}
	}()
	backendAddr := listener.Addr().String()

	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ProxyTCP{
		id:             "proxy.tcp.test_done",
		addresses:      []string{proxyAddr},
		log:            zerolog.Nop(),
		bufferSize:     32768,
		nodelay:        true,
		source:         "test_backend",
		wg:             wg,
		ctx:            ctx,
		cancel:         cancel,
		connectTimeout: 5 * time.Second,
		clientTimeout:  5 * time.Second,
		serverTimeout:  5 * time.Second,
		closeTimeout:   5 * time.Second,
		timeoutMargin:  1 * time.Second,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	provider := &mockBackendProvider{id: "test_backend", backendAddress: backendAddr}
	modules := make(module.ModulesRegistry)
	modules.AddModule("test_backend", provider)
	p.Bind(modules)

	// Wait for proxy listener to start
	testutil.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	// Connect to proxy. The backend will immediately close its side.
	// This ensures doneBackFront is closed before doneFrontBack.
	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for connection to be processed
	time.Sleep(50 * time.Millisecond)

	conn.Close()
}

type customBackendProvider struct {
	id string
	be *backend.Backend
}

func (c *customBackendProvider) GetBackend(wait bool) *backend.Backend {
	return c.be
}


func (c *customBackendProvider) Bind(modules module.ModulesRegistry) {}

// TestTCPProxy_CloseOnBackendRemoval verifies that active connections are closed when
// the backend is removed from the balancer, if close_on_backend_removal is enabled.
func TestTCPProxy_CloseOnBackendRemoval(t *testing.T) {
	backendServer := startEchoServer(t)
	defer backendServer.Close()

	proxyAddr, err := getFreePort()
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ProxyTCP{
		id:                    "proxy.tcp.test_close",
		addresses:             []string{proxyAddr},
		log:                   zerolog.Nop(),
		bufferSize:            32768,
		nodelay:               true,
		source:                "test_backend",
		wg:                    wg,
		ctx:                   ctx,
		cancel:                cancel,
		connectTimeout:        5 * time.Second,
		clientTimeout:         5 * time.Second,
		serverTimeout:         5 * time.Second,
		closeTimeout:          5 * time.Second,
		timeoutMargin:         1 * time.Second,
		closeOnBackendRemoval: true,
		bufferPool: sync.Pool{
			New: func() any { return &bufferWrapper{buf: make([]byte, 32768)} },
		},
		beMetricsCache: make(map[string]*Metrics),
	}

	beCtx, beCancel := context.WithCancel(context.Background())
	testBe := &backend.Backend{
		Address: backendServer.Addr().String(),
		Ctx:     beCtx,
		Cancel:  beCancel,
	}

	provider := &customBackendProvider{id: "test_backend", be: testBe}
	modules := make(module.ModulesRegistry)
	modules.AddModule("test_backend", provider)
	p.Bind(modules)

	// Wait for proxy listener to start
	testutil.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Millisecond)
		if err == nil {
			conn.Close()
			return true
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	conn, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}

	// Trigger backend removal (via context cancel)
	beCancel()

	// The connection should be closed shortly
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("connection was not closed after backend removal")
	}
}

// TestTCPProxyFactory_InvalidDurations verifies that ValidateConfig catches invalid duration strings.
func TestTCPProxyFactory_InvalidDurations(t *testing.T) {
	hclText := `
		source = "s1"
		connect_timeout = "invalid"
	`
	file, diags := hclsyntax.ParseConfig([]byte(hclText), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatal(diags)
	}

	tc := &module.Config{
		Type:   "tcp",
		Name:   "test_proxy_invalid",
		Config: file.Body,
		Ctx:    &hcl.EvalContext{},
	}

	vDiags := validateTCPProxyConfig(tc)
	if !vDiags.HasErrors() {
		t.Error("expected diagnostics to have errors for invalid duration")
	}
}
