package redis

import (
	"context"
	"mlb/backend"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

func TestPool_GetPut(t *testing.T) {
	// Setup a mock redis server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				// Just echo or handle PING for healthcheck if needed
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if string(buf[:n]) == "PING\r\n" {
						c.Write([]byte("+PONG\r\n"))
					}
				}
			}(conn)
		}
	}()

	proxy := &RedisProxy{
		id:                 "test",
		backends:           backend.NewRegistry(),
		log:                log.Logger,
		idleTimeout:        1 * time.Minute,
		connectTimeout:     1 * time.Second,
		healthcheckTimeout: 1 * time.Second,
		resetTimeout:       2 * time.Second,
		healthcheck:        true,
		bufferSize:         4096,
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()
	proxy.beMetricsCache = make(map[string]*Metrics)

	pool := NewRedisBackendConnectionPool(proxy)

	// Add backend
	proxy.backends.Add(&backend.Backend{Address: addr})

	// Get connection
	rbc, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rbc == nil {
		t.Fatalf("expected rbc to be not nil")
	}
	if rbc.backend.Address != addr {
		t.Errorf("expected %v, got %v", addr, rbc.backend.Address)
	}

	// Put it back
	pool.Put(rbc)
	if len(pool.pool) != 1 {
		t.Errorf("expected pool size 1, got %d", len(pool.pool))
	}

	// Get it again (should be the same one, LIFO)
	rbc2, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rbc != rbc2 {
		t.Errorf("expected %v, got %v", rbc, rbc2)
	}
	if len(pool.pool) != 0 {
		t.Errorf("expected pool size 0, got %d", len(pool.pool))
	}
}

func TestPool_EmptyBackends(t *testing.T) {
	proxy := &RedisProxy{
		backends:           backend.NewRegistry(),
		healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()

	pool := &RedisBackendConnectionPool{
		proxy: proxy,
		ctx:   proxy.ctx,
	}

	_, err := pool.Get(context.Background())
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestPool_CleanupIdle(t *testing.T) {
	proxy := &RedisProxy{
		idleTimeout:        10 * time.Millisecond,
		healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
		log:                log.Logger,
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()

	pool := &RedisBackendConnectionPool{
		proxy: proxy,
		ctx:   proxy.ctx,
	}

	ctx, cancel := context.WithCancel(context.Background())
	rbc := &RedisBackendConnection{
		lastUsed: time.Now().Add(-1 * time.Hour),
		cancel:   cancel,
		backend:  &backend.Backend{Address: "127.0.0.1:6379"},
	}
	pool.pool = append(pool.pool, rbc)

	pool.cleanupIdle()
	if len(pool.pool) != 0 {
		t.Errorf("expected pool size 0, got %d", len(pool.pool))
	}
	if ctx.Err() == nil {
		t.Error("expected context to be cancelled")
	}
}

func TestPool_UpdatePreconnect(t *testing.T) {
	// Setup mock server
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	proxy := &RedisProxy{
		backends:           backend.NewRegistry(),
		log:                log.Logger,
		preconnect:         2,
		idleTimeout:        1 * time.Minute,
		connectTimeout:     1 * time.Second,
		healthcheckTimeout: 1 * time.Second,
		resetTimeout:       2 * time.Second,
		bufferSize:         4096,
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()
	proxy.beMetricsCache = make(map[string]*Metrics)

	pool := NewRedisBackendConnectionPool(proxy)
	proxy.backends.Add(&backend.Backend{Address: addr})

	pool.Update()
	// Preconnect should have added 2 connections
	if len(pool.pool) != 2 {
		t.Errorf("expected pool size 2, got %d", len(pool.pool))
	}

	// Remove backend and update
	proxy.backends.Remove(addr)
	pool.Update()
	if len(pool.pool) != 0 {
		t.Errorf("expected pool size 0, got %d", len(pool.pool))
	}
}

func TestConnection_HealthcheckFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		conn, _ := ln.Accept()
		// Send wrong response
		buf := make([]byte, 1024)
		conn.Read(buf)
		conn.Write([]byte("+ERROR\r\n"))
		conn.Close()
	}()

	proxy := &RedisProxy{
		connectTimeout:     100 * time.Millisecond,
		healthcheckTimeout: 100 * time.Millisecond,
		beMetricsCache:     make(map[string]*Metrics),
		log:                log.Logger,
		bufferSize:         4096,
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()

	pool := &RedisBackendConnectionPool{proxy: proxy, ctx: proxy.ctx}
	rbc, err := NewRedisBackendConnection(pool, &backend.Backend{Address: addr})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = rbc.Healthcheck()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestConnection_ResetAndRelease(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := NewRedisProtocolReader(conn, 1024)
		defer reader.Release()

		// Expect RESET
		msg, err := reader.ReadMessage(false)
		if err != nil {
			return
		}
		if string(msg) == "*1\r\n$5\r\nRESET\r\n" {
			conn.Write([]byte("+RESET\r\n"))
		}
		ReleaseBuffer(msg)

		// Expect ECHO
		msg, err = reader.ReadMessage(false)
		if err != nil {
			return
		}
		smsg := string(msg)
		if len(smsg) > 14 && smsg[:14] == "*2\r\n$4\r\nECHO\r\n" {
			idx := 14
			for idx < len(smsg) && smsg[idx] != '$' {
				idx++
			}
			if idx < len(smsg) {
				conn.Write([]byte(smsg[idx:]))
			}
		}
		ReleaseBuffer(msg)
	}()

	proxy := &RedisProxy{
		beMetricsCache:     make(map[string]*Metrics),
		log:                log.Logger,
		bufferSize:         4096,
		healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
		backends:           backend.NewRegistry(),
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()

	pool := &RedisBackendConnectionPool{
		proxy: proxy,
		ctx:   proxy.ctx,
		pool:  make([]*RedisBackendConnection, 0),
	}
	be := &backend.Backend{Address: addr}
	proxy.backends.Add(be)
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rbc.ResetAndRelease()
	
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if len(pool.pool) != 1 {
		t.Errorf("expected pool size 1, got %d", len(pool.pool))
	}
}

func TestPool_GetStaleConnection(t *testing.T) {
	proxy := &RedisProxy{
		backends:           backend.NewRegistry(),
		log:                log.Logger,
		healthcheck:        false,
		healthcheckTimeout: time.Second,
		resetTimeout:       2 * time.Second,
	}
	proxy.ctx, proxy.cancel = context.WithCancel(context.Background())
	defer proxy.cancel()

	pool := &RedisBackendConnectionPool{
		proxy: proxy,
		ctx:   proxy.ctx,
		pool:  make([]*RedisBackendConnection, 0),
	}

	// Add a cancelled connection to pool
	ctx, cancel := context.WithCancel(proxy.ctx)
	cancel() // Make it stale
	rbc := &RedisBackendConnection{
		ctx:     ctx,
		backend: &backend.Backend{Address: "127.0.0.1:6379"},
	}
	pool.pool = append(pool.pool, rbc)

	// Add a fresh backend for when Get needs to create one
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ln.Close()
	proxy.backends.Add(&backend.Backend{Address: ln.Addr().String()})
	proxy.beMetricsCache = make(map[string]*Metrics)

	// Get should skip the stale one and create a new one
	rbc2, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rbc == rbc2 {
		t.Error("expected new connection, got the stale one")
	}
}
