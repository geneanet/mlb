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
	defer b1L.Close()
	go dummyMemcacheServer(b1L, "v1")

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                        "test_proxy",
		connectTimeout:            time.Second,
		backendMinConnections:     1,
		backendMaxConnections:     2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		ctx:                       ctx,
		cancel:                    cancel,
		backends:                  backend.NewRegistry(),
		log:                       zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
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
	defer b1L.Close()
	go dummyMemcacheServer(b1L, "v1")

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                        "test_proxy",
		connectTimeout:            time.Second,
		backendMinConnections:     1,
		backendMaxConnections:     2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		ctx:                       ctx,
		cancel:                    cancel,
		backends:                  backend.NewRegistry(),
		log:                       zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
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
	defer l.Close()

	addr := l.Addr().String()
	b1 := &backend.Backend{Address: addr}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_minmax",
		log:                      zerolog.Nop(),
		backends:                 backend.NewRegistry(),
		backendMinConnections:    1,
		backendMaxConnections:    2,
		backendInputQueueSize:    1, // Tiny queue to trigger saturation easily
		backendInflightQueueSize: 10,
		connectTimeout:           time.Second,
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		beMetricsCache:           make(map[string]*Metrics),
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
			conn.Close()
		}
	}()

	// Send queries until conn1.IsFull() is true
	for i := 0; i < 100; i++ {
		q := NewMemcacheQuery([]byte("set k 0 0 1\r\nv\r\n"), nil, nil)
		conn1.Query(q)
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
		conn2.Query(q)
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
	defer b1L.Close()
	go dummyMemcacheServer(b1L, "v1")

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}
	b2 := &backend.Backend{Address: "127.0.0.1:1", Meta: backend.NewMetaMap(nil)} // Faulty address

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_parallel",
		connectTimeout:           100 * time.Millisecond,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		ctx:                       ctx,
		cancel:                   cancel,
		backends:                  backend.NewRegistry(),
		log:                       zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
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

