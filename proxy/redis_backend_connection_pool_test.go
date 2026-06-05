package proxy

import (
	"context"
	"errors"
	"mlb/backend"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

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
	
	// Test wait timeout
	start := time.Now()
	rbc := pool.GetRandom(true)
	duration := time.Since(start)
	
	if rbc != nil {
		t.Errorf("expected rbc to be nil, got %v", rbc)
	}
	if duration < 50*time.Millisecond {
		t.Errorf("expected duration >= 50ms, got %v", duration)
	}

	// Add dummy connection
	dummyConn := &RedisBackendConnection{}
	pool.mutex.Lock()
	pool.pool[dummyConn] = struct{}{}
	pool.waitBackendsSemaphore.Release(1) // Release since it was blocked
	pool.mutex.Unlock()

	rbc = pool.GetRandom(false)
	if rbc != dummyConn {
		t.Errorf("expected rbc %v, got %v", dummyConn, rbc)
	}
}

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
	
	dummyConn := &RedisBackendConnection{}
	pool.mutex.Lock()
	pool.pool[dummyConn] = struct{}{}
	// release the initially acquired lock in New
	pool.waitBackendsSemaphore.Release(1)
	pool.mutex.Unlock()

	if len(pool.pool) != 1 {
		t.Errorf("expected pool size 1, got %d", len(pool.pool))
	}

	pool.Del(dummyConn)

	if len(pool.pool) != 0 {
		t.Errorf("expected pool size 0, got %d", len(pool.pool))
	}
	
	// Ensure the semaphore was acquired again
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	err := pool.waitBackendsSemaphore.Acquire(ctx2, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

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
	p.backendConnectionPool = pool // Needed for failure channel

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

	// Now remove the backend and run update again
	backendsMap.Remove(listener.Addr().String())
	
	go pool.Update()

	time.Sleep(100 * time.Millisecond)

	pool.mutex.RLock()
	size := len(pool.pool)
	pool.mutex.RUnlock()
	if size != 0 {
		t.Errorf("expected pool size 0, got %d", size)
	}
}

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
	p.backendConnectionPool = pool // Needed by the failure listening goroutine

	dummyConn := &RedisBackendConnection{
		backend: &backend.Backend{Address: "127.0.0.1:1234"},
	}

	// Add the connection to the pool map manually
	pool.mutex.Lock()
	pool.pool[dummyConn] = struct{}{}
	pool.waitBackendsSemaphore.Release(1) // Release initial
	pool.mutex.Unlock()

	if len(pool.pool) != 1 {
		t.Errorf("expected pool size 1, got %d", len(pool.pool))
	}

	// Notify failure
	pool.NotifyFailure(dummyConn)

	// Wait for the goroutine to process
	time.Sleep(50 * time.Millisecond)

	pool.mutex.RLock()
	size := len(pool.pool)
	pool.mutex.RUnlock()
	if size != 0 {
		t.Errorf("expected pool size 0, got %d", size)
	}
}
