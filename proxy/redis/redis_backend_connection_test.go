package redis

import (
	"context"
	"mlb/backend"
	"net"
	"testing"
	"time"
)

func TestRedisBackendConnection(t *testing.T) {
	// 1. Mock Backend
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
			go func(c net.Conn) {
				defer c.Close()
				reader := NewRedisProtocolReader(c, 1024)
				defer reader.Release()
				for {
					msg, err := reader.ReadMessage(true)
					if err != nil {
						return
					}
					smsg := string(msg)
					if smsg == "PING\r\n" || smsg == "*1\r\n$4\r\nPING\r\n" {
						c.Write([]byte("+PONG\r\n"))
					} else if smsg == "*1\r\n$5\r\nRESET\r\n" {
						c.Write([]byte("+RESET\r\n"))
					} else if len(smsg) > 14 && smsg[:14] == "*2\r\n$4\r\nECHO\r\n" {
						// ECHO command: *2\r\n$4\r\nECHO\r\n$length\r\ntoken\r\n
						idx := 14
						for idx < len(smsg) && smsg[idx] != '$' {
							idx++
						}
						if idx < len(smsg) {
							c.Write([]byte(smsg[idx:]))
						}
					} else {
						c.Write(msg) // Echo
					}
					ReleaseBuffer(msg)
				}
			}(conn)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                 "test-rbc",
		ctx:                ctx,
		connectTimeout:     time.Second,
		healthcheckTimeout: time.Second,
		bufferSize:         1024,
		beMetricsCache:     make(map[string]*Metrics),
		backends:           backend.NewRegistry(),
	}
	pool := NewRedisBackendConnectionPool(p)
	be := &backend.Backend{Address: ln.Addr().String()}
	p.backends.Add(be)

	t.Run("Lifecycle and Healthcheck", func(t *testing.T) {
		rbc, err := NewRedisBackendConnection(pool, be)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer rbc.cancel()

		err = rbc.Healthcheck()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("ResetAndRelease", func(t *testing.T) {
		rbc, err := NewRedisBackendConnection(pool, be)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Actually we can just write to the real mock backend
		rbc.conn.Write([]byte("PING\r\n"))

		rbc.ResetAndRelease()

		if len(pool.pool) != 1 {
			t.Errorf("expected pool size 1, got %d (Should be back in pool)", len(pool.pool))
		}
	})

	t.Run("ResetAndRelease With Backlog", func(t *testing.T) {
		// Clear pool
		pool.mutex.Lock()
		pool.pool = pool.pool[:0]
		pool.mutex.Unlock()

		rbc, err := NewRedisBackendConnection(pool, be)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Send multiple PINGs to create a backlog of PONGs in the backend.
		// These will be sitting in the connection buffer when ResetAndRelease is called.
		for i := 0; i < 5; i++ {
			rbc.conn.Write([]byte("PING\r\n"))
		}

		// Now ResetAndRelease should drain all 5 PONGs, then the +RESET, and finally catch the ECHO.
		rbc.ResetAndRelease()

		if len(pool.pool) != 1 {
			t.Errorf("expected pool size 1, got %d", len(pool.pool))
		}

		// Verify that the connection is truly clean by taking it back and doing a fresh PING.
		rbcClean, err := pool.Get(context.Background())
		if err != nil {
			t.Fatalf("failed to get connection from pool: %v", err)
		}
		if err := rbcClean.Healthcheck(); err != nil {
			t.Errorf("healthcheck failed on cleaned connection: %v", err)
		}
	})
}
