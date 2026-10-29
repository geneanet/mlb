package memcache

import (
	"bufio"
	"bytes"
	"context"
	"mlb/backend"
	"mlb/metrics"
	"mlb/testutil"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMemcacheProxyFlushAll_Broadcast(t *testing.T) {
	const backendCount = 3
	var flushCounts [backendCount]atomic.Int32
	var listeners [backendCount]net.Listener

	for i := 0; i < backendCount; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		listeners[i] = l

		idx := i
		go func(ln net.Listener) {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					reader := bufio.NewReader(c)
					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							return
						}
						if bytes.HasPrefix([]byte(line), []byte("flush_all")) {
							flushCounts[idx].Add(1)
							_, _ = c.Write([]byte("OK\r\n"))
						} else {
							_, _ = c.Write([]byte("ERROR\r\n"))
						}
					}
				}(conn)
			}
		}(l)
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_flush_all_proxy",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	for _, l := range listeners {
		b := backend.NewBackend(l.Addr().String(), nil)
		proxy.backends.Add(b)
	}
	proxy.ring.update(proxy.backends.GetList())
	proxy.backendConnectionPool.Update()

	lFront, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lFront.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lFront.Accept()
			if err != nil {
				return
			}
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, &Metrics{
				processed: metrics.FeCnxProcessed.WithLabelValues("test_flush", "test_flush"),
				active:    metrics.FeActCnx.WithLabelValues("test_flush", "test_flush"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test_flush", "test_flush"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test_flush", "test_flush"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test_flush", "test_flush"),
				requests:  metrics.FeRequests.WithLabelValues("test_flush", "test_flush"),
			})
		}
	}()

	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := bufio.NewReader(client)

	// Send flush_all
	_, _ = client.Write([]byte("flush_all\r\n"))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if resp != "OK\r\n" {
		t.Fatalf("expected 'OK\\r\\n', got %q", resp)
	}

	// Verify ALL backends received flush_all
	for i := 0; i < backendCount; i++ {
		count := flushCounts[i].Load()
		if count != 1 {
			t.Errorf("backend %d received %d flush_all, expected 1", i, count)
		}
	}

	// Send flush_all with noreply
	_, _ = client.Write([]byte("flush_all noreply\r\n"))

	// Verify ALL backends received second flush_all asynchronously
	for i := 0; i < backendCount; i++ {
		idx := i
		testutil.Eventually(t, func() bool {
			return flushCounts[idx].Load() == 2
		}, 1*time.Second, 10*time.Millisecond)
	}
}

func TestMemcacheProxyFlushAll_NoBackend(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_flush_nobackend",
		source:                   "mock",
		addresses:                []string{"127.0.0.1:0"},
		ctx:                      ctx,
		cancel:                   cancel,
		wg:                       wg,
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    2,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendUpdatesChan:       make(chan backend.BackendUpdate, 10),
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	lFront, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lFront.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lFront.Accept()
			if err != nil {
				return
			}
			proxy.connectionsWG.Add(1)
			go proxy.handleConnection(conn, &Metrics{
				processed: metrics.FeCnxProcessed.WithLabelValues("test_flush_nb", "test_flush_nb"),
				active:    metrics.FeActCnx.WithLabelValues("test_flush_nb", "test_flush_nb"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test_flush_nb", "test_flush_nb"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test_flush_nb", "test_flush_nb"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test_flush_nb", "test_flush_nb"),
				requests:  metrics.FeRequests.WithLabelValues("test_flush_nb", "test_flush_nb"),
			})
		}
	}()

	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := bufio.NewReader(client)

	_, _ = client.Write([]byte("flush_all\r\n"))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if resp != "SERVER_ERROR no backend available\r\n" {
		t.Fatalf("expected 'SERVER_ERROR no backend available\\r\\n', got %q", resp)
	}
}
