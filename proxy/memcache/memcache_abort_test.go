package memcache

import (
	"context"
	"mlb/backend"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMemcacheBackendConnection_AbortInflightQueries_WithPending(t *testing.T) {
	// Setup a dummy backend
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                        "test-proxy",
		log:                       zerolog.Nop(),
		ctx:                       ctx,
		connectTimeout:            time.Second,
		backendInputQueueSize:    10,
		backendInflightQueueSize:  10,
		beMetricsCache:            make(map[string]*Metrics),
	}

	pool := NewMemcacheBackendConnectionPool(proxy)
	proxy.backendConnectionPool = pool

	b := &backend.Backend{Address: l.Addr().String()}
	
	// Accept connection to avoid dial error
	go func() {
		conn, _ := l.Accept()
		if conn != nil {
			defer func() { _ = conn.Close() }()
			// Don't read anything
			time.Sleep(1 * time.Second)
		}
	}()

	mbc, err := NewMemcacheBackendConnection(pool, b)
	if err != nil {
		t.Fatal(err)
	}
	defer mbc.cancel()

	// 1. Add some queries to inFlight
	q1 := NewMemcacheQuery([]byte("get key1\r\n"), make(chan MemcacheResponse, 1), make(chan struct{}))
	q2 := NewMemcacheQuery([]byte("get key2\r\n"), make(chan MemcacheResponse, 1), make(chan struct{}))
	mbc.inFlight <- q1
	mbc.inFlight <- q2

	// 2. Add some queries to inputChan
	q3 := NewMemcacheQuery([]byte("get key3\r\n"), make(chan MemcacheResponse, 1), make(chan struct{}))
	q4 := NewMemcacheQuery([]byte("get key4\r\n"), make(chan MemcacheResponse, 1), make(chan struct{}))
	mbc.inputChan <- q3
	mbc.inputChan <- q4

	// ponytail: give background writer a chance to move them to inFlight
	time.Sleep(10 * time.Millisecond)

	// 3. Abort all
	count := mbc.AbortInflightQueries()

	if count != 4 {
		t.Errorf("Expected 4 aborted queries, got %d", count)
	}

	// Verify they all received an error
	queries := []MemcacheQuery{q1, q2, q3, q4}
	for i, q := range queries {
		select {
		case resp := <-q.responseChan:
			if string(resp.item) != "SERVER_ERROR backend failure\r\n" {
				t.Errorf("Query %d: expected error response, got %s", i+1, string(resp.item))
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Query %d: timeout waiting for abortion response", i+1)
		}
	}
}
