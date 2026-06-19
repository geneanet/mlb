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
		closeTimeout:              time.Second,
		backendConnectionPoolSize: 1,
		ctx:                       ctx,
		cancel:                    cancel,
		backends:                  backend.NewRegistry(),
		log:                       zerolog.Nop(),
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
		backendConnectionPoolSize: 1,
		ctx:                       ctx,
		cancel:                    cancel,
		backends:                  backend.NewRegistry(),
		log:                       zerolog.Nop(),
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
