package proxy

import (
	"context"
	"errors"
	"mlb/backend"
	"mlb/testutil"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestNewRedisBackendConnectionPool verifies the correct initialization of a RedisBackendConnectionPool.
// It checks that all internal fields (proxy reference, pool map, failure channel, and semaphore)
// are properly instantiated.
func TestNewRedisBackendConnectionPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                 "test-proxy",
		log:                zerolog.Nop(),
		ctx:                ctx,
		backendWaitTimeout: 10 * time.Millisecond,
	}

	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool
	if pool == nil {
		t.Fatal("expected pool not to be nil")
	}
	if pool.proxy != p {
		t.Errorf("expected proxy %v, got %v", p, pool.proxy)
	}
	if pool.pool == nil {
		t.Errorf("expected pool.pool not to be nil")
	}
	if pool.chanFailure == nil {
		t.Errorf("expected pool.chanFailure not to be nil")
	}
	if pool.waitBackendsSemaphore == nil {
		t.Errorf("expected pool.waitBackendsSemaphore not to be nil")
	}
}

// TestRedisBackendConnectionPool_GetRandom verifies the selection logic for obtaining a connection from the pool.
// It tests two scenarios:
// 1. When the pool is empty, GetRandom should wait up to the configured backendWaitTimeout and then return nil.
// 2. When a connection is available, GetRandom should return it immediately.
func TestRedisBackendConnectionPool_GetRandom(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                 "test-proxy",
		log:                zerolog.Nop(),
		ctx:                ctx,
		backendWaitTimeout: 50 * time.Millisecond,
	}

	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool

	// Test wait timeout when pool is empty
	start := time.Now()
	rbc := pool.GetRandom(true)
	duration := time.Since(start)

	if rbc != nil {
		t.Errorf("expected rbc to be nil, got %v", rbc)
	}
	if duration < 50*time.Millisecond {
		t.Errorf("expected duration >= 50ms, got %v", duration)
	}

	// Add a dummy connection to the pool and signal availability
	dummyConn := &RedisBackendConnection{}
	pool.mutex.Lock()
	pool.pool[dummyConn] = struct{}{}
	pool.waitBackendsSemaphore.Release(1) // Release since it was blocked by New
	pool.mutex.Unlock()

	rbc = pool.GetRandom(false)
	if rbc != dummyConn {
		t.Errorf("expected rbc %v, got %v", dummyConn, rbc)
	}
}

// TestRedisBackendConnectionPool_GetRandom_SuccessWait verifies that GetRandom correctly blocks
// and then returns a connection when one becomes available within the timeout period.
func TestRedisBackendConnectionPool_GetRandom_SuccessWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                 "test-proxy-success-wait",
		log:                zerolog.Nop(),
		ctx:                ctx,
		backendWaitTimeout: 500 * time.Millisecond,
	}

	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool

	dummyConn := &RedisBackendConnection{}

	// Add a connection after a short delay to trigger the successful wait path
	go func() {
		time.Sleep(50 * time.Millisecond)
		pool.mutex.Lock()
		pool.pool[dummyConn] = struct{}{}
		pool.waitBackendsSemaphore.Release(1)
		pool.mutex.Unlock()
	}()

	start := time.Now()
	rbc := pool.GetRandom(true)
	duration := time.Since(start)

	if rbc != dummyConn {
		t.Errorf("expected rbc %v, got %v", dummyConn, rbc)
	}
	if duration < 50*time.Millisecond {
		t.Errorf("expected duration >= 50ms, got %v", duration)
	}
}

// TestRedisBackendConnectionPool_Del verifies that removing a connection from the pool works correctly.
// It checks that:
// 1. The connection is removed from the internal map.
// 2. If the pool becomes empty, the semaphore is correctly re-acquired to block further GetRandom calls.
func TestRedisBackendConnectionPool_Del(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                 "test-proxy",
		log:                zerolog.Nop(),
		ctx:                ctx,
		backendWaitTimeout: 50 * time.Millisecond,
	}

	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool

	dummyConn := &RedisBackendConnection{}
	pool.mutex.Lock()
	pool.pool[dummyConn] = struct{}{}
	// Release the initially acquired lock in New
	pool.waitBackendsSemaphore.Release(1)
	pool.mutex.Unlock()

	if len(pool.pool) != 1 {
		t.Errorf("expected pool size 1, got %d", len(pool.pool))
	}

	pool.Del(dummyConn)

	if len(pool.pool) != 0 {
		t.Errorf("expected pool size 0, got %d", len(pool.pool))
	}

	// Ensure the semaphore was acquired again because the pool is now empty
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	err := pool.waitBackendsSemaphore.Acquire(ctx2, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

// TestRedisBackendConnectionPool_Update verifies the pool's ability to sync with the backend inventory.
// It tests:
// 1. Adding new connections when backends are discovered.
// 2. Removing existing connections when backends are removed from the inventory.
func TestRedisBackendConnectionPool_Update(t *testing.T) {
	// Start local TCP server to act as a backend
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backendsMap := backend.NewBackendsMap()
	backendsMap.Add(&backend.Backend{Address: listener.Addr().String()})

	p := &RedisProxy{
		id:                        "test-proxy",
		log:                       zerolog.Nop(),
		ctx:                       ctx,
		backendWaitTimeout:        50 * time.Millisecond,
		backendConnectionPoolSize: 2,
		backends:                  backendsMap,
		connectTimeout:            1 * time.Second,
		bufferSize:                1024,
		backendInflightQueueSize:  10,
		retryPeriod:               time.Millisecond,
		retryMaxPeriod:            10 * time.Millisecond,
		retryBackoffFactor:        1,
	}

	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool
 // Needed for failure channel

	// Accept connections in server goroutine
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	}()

	pool.Update()

	if len(pool.pool) != 2 {
		t.Errorf("expected pool size 2, got %d", len(pool.pool))
	}

	// Now remove the backend and run update again to ensure connections are closed and removed
	backendsMap.Remove(listener.Addr().String())

	go pool.Update()

	testutil.Eventually(t, func() bool {
		pool.mutex.RLock()
		defer pool.mutex.RUnlock()
		return len(pool.pool) == 0
	}, 1*time.Second, 10*time.Millisecond)
}

// TestRedisBackendConnectionPool_NotifyFailure verifies the asynchronous failure notification mechanism.
// It ensures that when NotifyFailure is called for a connection, it is eventually removed from the pool.
func TestRedisBackendConnectionPool_NotifyFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                 "test-proxy",
		log:                zerolog.Nop(),
		ctx:                ctx,
		backendWaitTimeout: 50 * time.Millisecond,
	}

	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool
 // Needed by the failure listening goroutine

	dummyConn := &RedisBackendConnection{
		backend: &backend.Backend{Address: "127.0.0.1:1234"},
	}

	// Add the connection to the pool map manually and signal availability
	pool.mutex.Lock()
	pool.pool[dummyConn] = struct{}{}
	pool.waitBackendsSemaphore.Release(1) // Release initial lock from New
	pool.mutex.Unlock()

	if len(pool.pool) != 1 {
		t.Errorf("expected pool size 1, got %d", len(pool.pool))
	}

	// Notify failure - this sends to pool.chanFailure which is processed by a background goroutine
	pool.NotifyFailure(dummyConn)

	// Wait for the background goroutine to process the failure and remove the connection
	testutil.Eventually(t, func() bool {
		pool.mutex.RLock()
		defer pool.mutex.RUnlock()
		return len(pool.pool) == 0
	}, 1*time.Second, 10*time.Millisecond)
}
