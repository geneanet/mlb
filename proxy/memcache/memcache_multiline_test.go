package memcache

import (
	"bufio"
	"bytes"
	"context"
	"mlb/backend"
	"mlb/metrics"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMemcacheProxy_MultiLineDesync(t *testing.T) {
	// 1. Setup Mock Backend
	lBack, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lBack.Close()

	go func() {
		for {
			conn, err := lBack.Accept()
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

					if bytes.HasPrefix([]byte(line), []byte("stats cachedump")) {
						// Return multiple ITEM lines followed by END
						c.Write([]byte("ITEM key1 [1 b; 0 s]\r\n"))
						c.Write([]byte("ITEM key2 [1 b; 0 s]\r\n"))
						c.Write([]byte("END\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("get ")) {
						c.Write([]byte("VALUE key 0 2\r\nhi\r\nEND\r\n"))
					} else {
						c.Write([]byte("ERROR\r\n"))
					}
				}
			}(conn)
		}
	}()

	// 2. Setup Proxy
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_multiline",
		log:                      zerolog.Nop(),
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

	b := backend.NewBackend(lBack.Addr().String(), nil)
	proxy.backends.Add(b)
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
				processed: metrics.FeCnxProcessed.WithLabelValues("test", "test"),
				active:    metrics.FeActCnx.WithLabelValues("test", "test"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test", "test"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test", "test"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test", "test"),
				requests:  metrics.FeRequests.WithLabelValues("test", "test"),
			})
		}
	}()

	// 3. Test Client
	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := bufio.NewReader(client)

	// Step 1: Send a stats cachedump command (MultiLine)
	_, err = client.Write([]byte("stats cachedump 1 100\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Read response. Should be the 3 lines.
	lines := []string{
		"ITEM key1 [1 b; 0 s]\r\n",
		"ITEM key2 [1 b; 0 s]\r\n",
		"END\r\n",
	}
	for i, expected := range lines {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read stats line %d: %v", i+1, err)
		}
		if line != expected {
			t.Errorf("Expected stats line %d to be %q, got %q", i+1, expected, line)
		}
	}

	// Step 3: Send a regular get command
	_, err = client.Write([]byte("get key\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Step 4: Read response. MUST be the response to "get key".
	// If desynchronized (e.g. proxy stopped reading after first ITEM), this would be ITEM key2...
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read get response: %v", err)
	}
	if resp != "VALUE key 0 2\r\n" {
		t.Errorf("Expected VALUE key 0 2\\r\\n, got %q. Desynchronization occurred!", resp)
	}
}
