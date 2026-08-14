package redis

import (
	"context"
	"mlb/backend"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

func TestRedisBackendConnectionPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := backend.NewRegistry()
	p := &RedisProxy{
		id:                 "test-pool",
		backends:           reg,
		ctx:                ctx,
		log:                log.With().Str("test", "pool").Logger(),
		idleTimeout:        100 * time.Millisecond,
		connectTimeout:     time.Second,
		healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
		beMetricsCache:     make(map[string]*Metrics),
	}
	pool := NewRedisBackendConnectionPool(p)

	t.Run("Get from empty pool fails if no backends", func(t *testing.T) {
		_, err := pool.Get(ctx)
		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	t.Run("Put and Get (LIFO)", func(t *testing.T) {
		rbc1 := &RedisBackendConnection{pool: pool, backend: &backend.Backend{Address: "1"}, lastUsed: time.Now(), ctx: ctx}
		rbc2 := &RedisBackendConnection{pool: pool, backend: &backend.Backend{Address: "2"}, lastUsed: time.Now(), ctx: ctx}

		p.backends.Add(rbc1.backend); p.backends.Add(rbc2.backend); pool.Put(rbc1)
		pool.Put(rbc2)

		p.backends.Remove("1")
		p.backends.Remove("2")

		got, err := pool.Get(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != rbc2 {
			t.Errorf("expected %v, got %v (Should get the last one put (LIFO))", rbc2, got)
		}

		got, err = pool.Get(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != rbc1 {
			t.Errorf("expected %v, got %v", rbc1, got)
		}
	})

	t.Run("Idle Cleanup", func(t *testing.T) {
		rbc := &RedisBackendConnection{
			pool:     pool,
			backend:  &backend.Backend{Address: "127.0.0.1:6379"},
			lastUsed: time.Now().Add(-1 * time.Hour),
		}
		rbc.ctx, rbc.cancel = context.WithCancel(ctx)
		
		p.backends.Add(rbc.backend); pool.Put(rbc)
		if len(pool.pool) != 1 {
			t.Errorf("expected pool size 1, got %d", len(pool.pool))
		}

		pool.cleanupIdle()
		if len(pool.pool) != 0 {
			t.Errorf("expected pool size 0, got %d", len(pool.pool))
		}
		p.backends.Remove("127.0.0.1:6379")
		if rbc.ctx.Err() == nil {
			t.Error("expected connection to be cancelled")
		}
	})

	t.Run("Update and Preconnect", func(t *testing.T) {
		// This requires a real connection attempt if we want to test NewRedisBackendConnection
		// So we might just test the filtering logic
		rbc := &RedisBackendConnection{
			pool: pool,
			backend: &backend.Backend{Address: "127.0.0.1:6379"},
			ctx: ctx,
		}
		p.backends.Add(rbc.backend); pool.Put(rbc)
		
		// Remove backend from registry
		p.backends.Remove("127.0.0.1:6379")
		pool.Update()
		
		if len(pool.pool) != 0 {
			t.Errorf("expected pool size 0, got %d (Connection to removed backend should be filtered out)", len(pool.pool))
		}
	})

	t.Run("Preconnect", func(t *testing.T) {
		// Mock a backend server to allow connection
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer ln.Close()

		p.backends.Add(&backend.Backend{Address: ln.Addr().String()})
		p.preconnect = 2
		
		pool.Update()
		
		// Note: Update might fail if NewRedisBackendConnection fails, but here it should work
		// Actually, we need to be careful with concurrency and random picking.
		if len(pool.pool) == 0 {
			t.Error("expected pool to have connections")
		}
	})

	t.Run("Wait Timeout", func(t *testing.T) {
		reg := backend.NewRegistry()
		p := &RedisProxy{
			id:                 "test-wait-timeout",
			backends:           reg,
			ctx:                ctx,
			log:                log.With().Str("test", "wait").Logger(),
			backendWaitTimeout: 500 * time.Millisecond,
			connectTimeout:     time.Second,
			healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
			beMetricsCache:     make(map[string]*Metrics),
		}
		pool := NewRedisBackendConnectionPool(p)

		// Start a fake backend server
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer ln.Close()
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()

		// Try to Get in a goroutine
		start := time.Now()
		done := make(chan struct{})
		go func() {
			_, err := pool.Get(ctx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			close(done)
		}()

		// Wait 100ms then add backend
		time.Sleep(100 * time.Millisecond)
		reg.Add(&backend.Backend{Address: ln.Addr().String()})

		select {
		case <-done:
			elapsed := time.Since(start)
			if elapsed < 100*time.Millisecond {
				t.Errorf("expected to wait at least 100ms, got %v", elapsed)
			}
		case <-time.After(1 * time.Second):
			t.Error("timed out waiting for Get to return")
		}
	})

	t.Run("Wait Timeout Exceeded", func(t *testing.T) {
		reg := backend.NewRegistry()
		p := &RedisProxy{
			id:                 "test-wait-timeout-exceeded",
			backends:           reg,
			ctx:                ctx,
			log:                log.With().Str("test", "wait-exceeded").Logger(),
			backendWaitTimeout: 100 * time.Millisecond,
			connectTimeout:     time.Second,
			healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
			beMetricsCache:     make(map[string]*Metrics),
		}
		pool := NewRedisBackendConnectionPool(p)

		start := time.Now()
		_, err := pool.Get(ctx)
		elapsed := time.Since(start)

		if err == nil {
			t.Error("expected error, got nil")
		}
		if elapsed < 100*time.Millisecond {
			t.Errorf("expected to wait at least 100ms, got %v", elapsed)
		}
	})
}

