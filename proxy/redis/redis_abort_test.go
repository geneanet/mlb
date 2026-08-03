package redis

import (
	"context"
	"mlb/backend"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestRedisBackendConnection_AbortInflightQueries_WithPending(t *testing.T) {
	// Setup a dummy backend
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &RedisProxy{
		id:                        "test-proxy",
		log:                       zerolog.Nop(),
		ctx:                       ctx,
		connectTimeout:            time.Second,
		backendInputQueueSize:    10,
		backendInflightQueueSize:  10,
		beMetricsCache:            make(map[string]*Metrics),
	}

	pool := NewRedisBackendConnectionPool(proxy)
	proxy.backendConnectionPool = pool

	b := &backend.Backend{Address: l.Addr().String()}
	
	// Accept connection to avoid dial error
	go func() {
		conn, _ := l.Accept()
		if conn != nil {
			defer conn.Close()
			// Don't read anything to let inputChan fill up or queries sit there
			time.Sleep(1 * time.Second)
		}
	}()

	rbc, err := NewRedisBackendConnection(pool, b)
	if err != nil {
		t.Fatal(err)
	}
	defer rbc.cancel()

	// 1. Add some queries to inFlight
	q1 := NewRedisQuery([]byte("PING1\r\n"), make(chan RedisReponse, 1), make(chan struct{}))
	q2 := NewRedisQuery([]byte("PING2\r\n"), make(chan RedisReponse, 1), make(chan struct{}))
	rbc.inFlight <- q1
	rbc.inFlight <- q2

	// 2. Add some queries to inputChan
	q3 := NewRedisQuery([]byte("PING3\r\n"), make(chan RedisReponse, 1), make(chan struct{}))
	q4 := NewRedisQuery([]byte("PING4\r\n"), make(chan RedisReponse, 1), make(chan struct{}))
	rbc.inputChan <- q3
	rbc.inputChan <- q4

	// 3. Abort all
	count := rbc.AbortInflightQueries()

	if count != 4 {
		t.Errorf("Expected 4 aborted queries, got %d", count)
	}

	// Verify they all received an error
	queries := []RedisQuery{q1, q2, q3, q4}
	for i, q := range queries {
		select {
		case resp := <-q.responseChan:
			if string(resp.item) != "-ERR Backend connection failed\r\n" {
				t.Errorf("Query %d: expected error response, got %s", i+1, string(resp.item))
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Query %d: timeout waiting for abortion response", i+1)
		}
	}
}
