package memcache

import (
	"context"
	"mlb/backend"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMemcacheBackendConnectionPool_Del(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = b1L.Close() }()
	go dummyMemcacheServer(b1L, "v1")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		connectTimeout:           time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		ctx:                      ctx,
		cancel:                   cancel,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backends.Add(b1)

	pool := NewMemcacheBackendConnectionPool(proxy)
	pool.Update()

	conn := pool.Get(b1.Address)
	if conn == nil {
		t.Fatalf("Expected connection, got nil")
	}

	pool.Del(conn)

	conn2 := pool.Get(b1.Address)
	if conn2 != nil {
		t.Fatalf("Expected nil connection after Del, got %v", conn2)
	}
}

func TestMemcacheBackendConnectionPool_UpdateRemovesDeadBackends(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = b1L.Close() }()
	go dummyMemcacheServer(b1L, "v1")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		connectTimeout:           time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		ctx:                      ctx,
		cancel:                   cancel,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backends.Add(b1)

	pool := NewMemcacheBackendConnectionPool(proxy)
	pool.Update()

	conn := pool.Get(b1.Address)
	if conn == nil {
		t.Fatalf("Expected connection")
	}

	proxy.backends.Remove(b1.Address)
	pool.Update()

	conn2 := pool.Get(b1.Address)
	if conn2 != nil {
		t.Fatalf("Expected connection to be removed, but got one")
	}
}

func TestMemcacheMinMaxPoolGrowth(t *testing.T) {
	// Start a dummy backend
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	addr := l.Addr().String()
	b1 := backend.NewBackend(addr, nil)

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_minmax",
		log:                      zerolog.Nop(),
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		backendMinConnections:    1,
		backendMaxConnections:    2,
		backendInputQueueSize:    1, // Tiny queue to trigger saturation easily
		backendInflightQueueSize: 10,
		connectTimeout:           time.Second,
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		beMetricsCache:           make(map[string]*Metrics),
		readyChan:                make(chan struct{}),
	}
	proxy.backends.Add(b1)
	pool := NewMemcacheBackendConnectionPool(proxy)
	pool.Update()

	// 1. Should have exactly 1 connection initially
	pool.mutex.RLock()
	if len(pool.pools[addr]) != 1 {
		t.Errorf("Expected 1 connection, got %d", len(pool.pools[addr]))
	}
	pool.mutex.RUnlock()

	conn1 := pool.Get(addr)
	if conn1 == nil {
		t.Fatal("Expected connection")
	}

	// 2. Saturate conn1
	// We need to fill inputChan. Since it's read by a goroutine, we need to block that goroutine.
	// The goroutine blocks on conn.Write or on inFlight <- query.
	// Let's not accept connections on our dummy server to block Dial or Write?
	// No, Dial already happened.
	// If we don't read from the socket in dummy server, conn.Write will eventually block.
	// But it might take a lot of data.

	// Easier: just manually fill the channel if we can? No, it's private.
	// But we can send queries until it's full.

	// To make it block faster, we can use a huge query if needed, or just many small ones.
	// However, the background goroutine reads from inputChan and then blocks on Write.

	// Let's use a non-reading backend to block Write.
	go func() {
		conn, _ := l.Accept()
		if conn != nil {
			// Don't read anything, let the proxy fill the TCP buffer
			time.Sleep(2 * time.Second)
			_ = conn.Close()
		}
	}()

	// Send queries until conn1.IsFull() is true
	for i := 0; i < 100; i++ {
		q := NewMemcacheQuery([]byte("set k 0 0 1\r\nv\r\n"), nil, nil)
		_ = conn1.Query(q)
		if conn1.IsFull() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	if !conn1.IsFull() {
		t.Log("Connection not full yet, might be due to TCP buffers. Trying growth anyway.")
	}

	// 3. Get() should now see it's full and open a second connection
	conn2 := pool.Get(addr)
	if conn2 == nil {
		t.Fatal("Expected second connection")
	}
	if conn2 == conn1 && conn1.IsFull() {
		t.Error("Get() returned the same full connection instead of growing")
	}

	pool.mutex.RLock()
	if len(pool.pools[addr]) != 2 {
		t.Errorf("Expected 2 connections, got %d", len(pool.pools[addr]))
	}
	pool.mutex.RUnlock()

	// 4. Growth should stop at max=2
	// Even if both are full, Get() should return one of them and NOT grow to 3.
	// (Saturate conn2 first)
	for i := 0; i < 100; i++ {
		q := NewMemcacheQuery([]byte("set k 0 0 1\r\nv\r\n"), nil, nil)
		_ = conn2.Query(q)
		if conn2.IsFull() {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	_ = pool.Get(addr)
	pool.mutex.RLock()
	if len(pool.pools[addr]) > 2 {
		t.Errorf("Expected at most 2 connections, got %d", len(pool.pools[addr]))
	}
	pool.mutex.RUnlock()
}

func TestMemcacheBackendConnectionPool_UpdateParallel(t *testing.T) {
	// One good backend, one faulty (not listening)
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer func() { _ = b1L.Close() }()
	go dummyMemcacheServer(b1L, "v1")

	b1 := backend.NewBackend(b1L.Addr().String(), nil)
	b2 := backend.NewBackend("127.0.0.1:1", nil) // Faulty address

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_parallel",
		connectTimeout:           100 * time.Millisecond,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		ctx:                      ctx,
		cancel:                   cancel,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backends.Add(b1)
	proxy.backends.Add(b2)

	pool := NewMemcacheBackendConnectionPool(proxy)

	// Update should return relatively quickly even if b2 is failing (due to parallelism and try limit)
	pool.Update()

	// b1 should be ready because it was processed in parallel with b2
	pool.mutex.Lock()
	p1Len := len(pool.pools[b1.Address])
	pool.mutex.Unlock()

	if p1Len < 1 {
		t.Errorf("Expected backend 1 to be ready, but it has 0 connections")
	}
}

// TestMemcacheBackendConnectionPool_Get_SkipUnhealthy verifies that Get skips connections
// that have had their context cancelled (marked as unhealthy).
func TestMemcacheBackendConnectionPool_Get_SkipUnhealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &MemcacheProxy{
		id:             "test-proxy",
		log:            zerolog.Nop(),
		ctx:            ctx,
		beMetricsCache: make(map[string]*Metrics),
		readyChan:      make(chan struct{}),
	}

	pool := NewMemcacheBackendConnectionPool(p)

	addr := "127.0.0.1:11211"
	b1 := backend.NewBackend(addr, nil)
	pool.backends[addr] = b1

	// Add an unhealthy connection
	unhealthyCtx, unhealthyCancel := context.WithCancel(context.Background())
	unhealthyCancel() // Mark it as unhealthy immediately
	unhealthyConn := &MemcacheBackendConnection{
		backend: b1,
		ctx:     unhealthyCtx,
	}

	// Add a healthy connection
	healthyConn := &MemcacheBackendConnection{
		backend: b1,
		ctx:     context.Background(),
	}

	pool.mutex.Lock()
	pool.pools[addr] = []*MemcacheBackendConnection{unhealthyConn, healthyConn}
	pool.mutex.Unlock()

	// Get should skip unhealthyConn and return healthyConn
	mbc := pool.Get(addr)
	if mbc != healthyConn {
		t.Errorf("expected healthy connection, got %v", mbc)
	}

	// If only unhealthy connections are left, it should still return one as fallback (matching Get behavior)
	pool.mutex.Lock()
	pool.pools[addr] = []*MemcacheBackendConnection{unhealthyConn}
	pool.mutex.Unlock()

	mbc = pool.Get(addr)
	if mbc != unhealthyConn {
		t.Errorf("expected unhealthy connection as fallback, got %v", mbc)
	}
}

func TestMemcacheBackendConnectionPool_NotifyFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &MemcacheProxy{
		id:             "test-proxy",
		log:            zerolog.Nop(),
		ctx:            ctx,
		beMetricsCache: make(map[string]*Metrics),
		backends:       backend.NewRegistry(zerolog.Nop(), false),
		readyChan:      make(chan struct{}),
	}

	pool := NewMemcacheBackendConnectionPool(p)

	addr := "127.0.0.1:11211"
	b1 := backend.NewBackend(addr, nil)
	p.backends.Add(b1)
	pool.backends[addr] = b1

	dummyConn := &MemcacheBackendConnection{
		backend: b1,
		ctx:     context.Background(),
	}

	pool.mutex.Lock()
	pool.pools[addr] = []*MemcacheBackendConnection{dummyConn}
	pool.mutex.Unlock()

	// Notify failure
	pool.NotifyFailure(dummyConn, context.Canceled, false)

	// Wait for the background goroutine to process the failure and remove the connection
	time.Sleep(100 * time.Millisecond)

	pool.mutex.RLock()
	defer pool.mutex.RUnlock()
	for _, c := range pool.pools[addr] {
		if c == dummyConn {
			t.Errorf("dummyConn still in pool after NotifyFailure")
		}
	}
}
