package memcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"mlb/backend"
	"mlb/metrics"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type testWriterMG struct {
	t      *testing.T
	mu     sync.RWMutex
	closed bool
}

func (tw *testWriterMG) Write(p []byte) (n int, err error) {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	if !tw.closed {
		tw.t.Log(string(bytes.TrimSpace(p)))
	}
	return len(p), nil
}

func (tw *testWriterMG) Close() {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.closed = true
}

func TestMemcacheProxy_MultiGetPartialFailure(t *testing.T) {
	tw := &testWriterMG{t: t}
	defer tw.Close()

	// 1. Setup Mock Backend 1 (Success)
	lBack1, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lBack1.Close()
	go func() {
		conn, _ := lBack1.Accept()
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if bytes.HasPrefix([]byte(line), []byte("get ")) {
				// Return data for k1
				_, _ = conn.Write([]byte("VALUE k1 0 2\r\nv1\r\nEND\r\n"))
			}
		}
	}()

	// 2. Setup Mock Backend 2 (Failure during request)
	lBack2, _ := net.Listen("tcp", "127.0.0.1:0")
	defer lBack2.Close()
	go func() {
		for {
			conn, err := lBack2.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				reader := bufio.NewReader(c)
				line, _ := reader.ReadString('\n')
				if line != "" {
					// Close connection WHILE processing a request to trigger failure
					_ = c.Close()
				}
			}(conn)
		}
	}()

	// 3. Setup Proxy
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: tw}).With().Timestamp().Logger()

	proxy := &MemcacheProxy{
		id:                       "test_multiget_fail",
		log:                      logger,
		closeTimeout:             time.Second,
		connectTimeout:           time.Second,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		wg:                       wg,
		ctx:                      ctx,
		cancel:                   cancel,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		ring:      newMemcacheHashRing(),
		backends:  backend.NewRegistry(zerolog.Nop(), false),
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	b1 := backend.NewBackend(lBack1.Addr().String(), nil)
	b2 := backend.NewBackend(lBack2.Addr().String(), nil)
	proxy.backends.Add(b1)
	proxy.backends.Add(b2)
	proxy.ring.update(proxy.backends.GetList())
	proxy.backendConnectionPool.Update()

	lFront, _ := net.Listen("tcp", "127.0.0.1:0")
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
				processed: metrics.FeCnxProcessed.WithLabelValues("test", "test"),
				active:    metrics.FeActCnx.WithLabelValues("test", "test"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test", "test"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test", "test"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test", "test"),
				requests:  metrics.FeRequests.WithLabelValues("test", "test"),
			})
		}
	}()

	// 4. Test Client
	client, _ := net.Dial("tcp", lFront.Addr().String())
	defer client.Close()

	// Send a multi-get that will hit BOTH backends by using many keys
	var keys bytes.Buffer
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&keys, " k%d", i)
	}
	_, _ = fmt.Fprintf(client, "get%s\r\n", keys.String())

	reader := bufio.NewReader(client)
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))

	var output bytes.Buffer
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Logf("Read error: %v", err)
			break
		}
		output.WriteString(line)
		if line == "END\r\n" {
			break
		}
		// Safety break to avoid infinite loop on bad responses
		if output.Len() > 65536 {
			break
		}
	}

	result := output.String()
	t.Logf("Combined Response: %q", result)

	// Valid outcomes:
	// 1. Both succeed (unlikely here as b2 fails)
	// 2. b1 succeeds, b2 fails -> VALUE k1...END (b2 treated as miss)
	// 3. Command fails entirely -> SERVER_ERROR...
	
	// INVALID outcome:
	// "VALUE k1 0 2\r\nv1\r\nSERVER_ERROR backend failure\r\nEND\r\n"
	
	if bytes.Contains(output.Bytes(), []byte("SERVER_ERROR")) && bytes.Contains(output.Bytes(), []byte("VALUE")) {
		t.Errorf("PROTOCOL VIOLATION: Mixed data and error in multi-get: %q", result)
	}
}

func TestMemcacheProxy_MultiGetAllBackendsFail(t *testing.T) {
	// Backend that immediately closes listeners
	lBack, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lBack.Addr().String()
	_ = lBack.Close()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_multiget_all_fail",
		log:                      zerolog.Nop(),
		closeTimeout:             time.Second,
		connectTimeout:           100 * time.Millisecond,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		wg:                       wg,
		ctx:                      ctx,
		cancel:                   cancel,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		ring:      newMemcacheHashRing(),
		backends:  backend.NewRegistry(zerolog.Nop(), false),
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	b := backend.NewBackend(addr, nil)
	proxy.backends.Add(b)
	proxy.ring.update(proxy.backends.GetList())
	// Note: backendConnectionPool will fail to connect

	lFront, _ := net.Listen("tcp", "127.0.0.1:0")
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
				processed: metrics.FeCnxProcessed.WithLabelValues("test_all_fail", "test_all_fail"),
				active:    metrics.FeActCnx.WithLabelValues("test_all_fail", "test_all_fail"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test_all_fail", "test_all_fail"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test_all_fail", "test_all_fail"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test_all_fail", "test_all_fail"),
				requests:  metrics.FeRequests.WithLabelValues("test_all_fail", "test_all_fail"),
			})
		}
	}()

	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, _ = client.Write([]byte("get key1 key2\r\n"))
	reader := bufio.NewReader(client)
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if resp != "SERVER_ERROR backend failure\r\n" {
		t.Fatalf("expected 'SERVER_ERROR backend failure\\r\\n', got %q", resp)
	}
}

func TestMemcacheProxy_MultiGetNoBackend(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_multiget_no_be",
		log:                      zerolog.Nop(),
		closeTimeout:             time.Second,
		connectTimeout:           100 * time.Millisecond,
		backendMinConnections:    1,
		backendMaxConnections:    1,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		wg:                       wg,
		ctx:                      ctx,
		cancel:                   cancel,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
		ring:      newMemcacheHashRing(),
		backends:  backend.NewRegistry(zerolog.Nop(), false),
		readyChan: make(chan struct{}),
	}
	proxy.backendConnectionPool = NewMemcacheBackendConnectionPool(proxy)

	lFront, _ := net.Listen("tcp", "127.0.0.1:0")
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
				processed: metrics.FeCnxProcessed.WithLabelValues("test_no_be", "test_no_be"),
				active:    metrics.FeActCnx.WithLabelValues("test_no_be", "test_no_be"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test_no_be", "test_no_be"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test_no_be", "test_no_be"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test_no_be", "test_no_be"),
				requests:  metrics.FeRequests.WithLabelValues("test_no_be", "test_no_be"),
			})
		}
	}()

	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, _ = client.Write([]byte("get key1 key2\r\n"))
	reader := bufio.NewReader(client)
	_ = client.SetDeadline(time.Now().Add(2 * time.Second))
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if resp != "SERVER_ERROR no backend available\r\n" {
		t.Fatalf("expected 'SERVER_ERROR no backend available\\r\\n', got %q", resp)
	}
}
