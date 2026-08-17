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

func TestMemcacheBackendConnection_QueryAndAbort(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()

	// Server that just hangs
	go func() {
		conn, err := b1L.Accept()
		if err == nil {
			time.Sleep(1 * time.Second)
			_ = conn.Close()
		}
	}()

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		connectTimeout:           time.Second,
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

	pool := NewMemcacheBackendConnectionPool(proxy)
	conn, err := NewMemcacheBackendConnection(pool, b1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	respChan := make(chan MemcacheResponse, 1)
	stopChan := make(chan struct{})
	q := NewMemcacheQuery([]byte("get key\r\n"), respChan, stopChan)

	err = conn.Query(q)
	if err != nil {
		t.Fatalf("Unexpected error querying: %v", err)
	}

	// Wait a tiny bit for it to be in-flight
	time.Sleep(50 * time.Millisecond)

	// Now abort
	conn.AbortInflightQueries()

	resp := <-respChan
	expectedErr := "SERVER_ERROR backend failure\r\n"
	if string(resp.item) != expectedErr {
		t.Fatalf("Expected protocol error on abort %q, got: %q", expectedErr, string(resp.item))
	}

	// Test cancellation handling
	conn.cancel()
}

func TestMemcacheBackendConnection_ReadFull(t *testing.T) {
	b1L, _ := net.Listen("tcp", "127.0.0.1:0")
	defer b1L.Close()

	go dummyMemcacheServer(b1L, "v1")

	b1 := &backend.Backend{Address: b1L.Addr().String(), Meta: backend.NewMetaMap(nil)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_proxy",
		connectTimeout:           time.Second,
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

	pool := NewMemcacheBackendConnectionPool(proxy)
	conn, err := NewMemcacheBackendConnection(pool, b1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	respChan := make(chan MemcacheResponse, 1)
	stopChan := make(chan struct{})
	q := NewMemcacheQuery([]byte("get key\r\n"), respChan, stopChan)

	err = conn.Query(q)
	if err != nil {
		t.Fatalf("Unexpected error querying: %v", err)
	}

	resp := <-respChan
	if string(resp.item) != "VALUE key 0 2\r\nv1\r\nEND\r\n" {
		t.Fatalf("Unexpected response: %s", string(resp.item))
	}

	// Test non-value response (e.g. STORED)
	respChan2 := make(chan MemcacheResponse, 1)
	stopChan2 := make(chan struct{})
	q2 := NewMemcacheQuery([]byte("set key 0 0 2\r\nv1\r\n"), respChan2, stopChan2)
	_ = conn.Query(q2)

	resp2 := <-respChan2
	if string(resp2.item) != "STORED\r\n" {
		t.Fatalf("Unexpected response: %s", string(resp2.item))
	}
}
