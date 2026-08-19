package memcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/metrics"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type testingWriter struct {
	t *testing.T
}

func (tw testingWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(bytes.TrimSpace(p)))
	return len(p), nil
}

func TestMemcacheProxy_NoReplyDesync(t *testing.T) {
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
					// If command has "noreply", we send NOTHING (simulating memcached)
					if bytes.Contains([]byte(line), []byte("noreply")) {
						// Read payload if any
						if bytes.HasPrefix([]byte(line), []byte("set ")) {
							fields := bytes.Fields([]byte(line))
							size := 0
							fmt.Sscanf(string(fields[4]), "%d", &size)
							payload := make([]byte, size+2)
							io.ReadFull(reader, payload)
						}
						// Send nothing
						continue
					}

					// Otherwise, reply STORED or VALUE
					if bytes.HasPrefix([]byte(line), []byte("set ")) {
						fields := bytes.Fields([]byte(line))
						size := 0
						fmt.Sscanf(string(fields[4]), "%d", &size)
						payload := make([]byte, size+2)
						io.ReadFull(reader, payload)
						c.Write([]byte("STORED\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("get ")) {
						c.Write([]byte("VALUE key 0 2\r\nhi\r\nEND\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("ms ")) {
						// Meta set
						fields := bytes.Fields([]byte(line))
						size := 0
						fmt.Sscanf(string(fields[2]), "%d", &size)
						payload := make([]byte, size+2)
						io.ReadFull(reader, payload)
						c.Write([]byte("HD\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("mg ")) {
						// Meta get
						c.Write([]byte("VA 2\r\nhi\r\n"))
					}
				}
			}(conn)
		}
	}()

	// 2. Setup Proxy
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: testingWriter{t}}).With().Timestamp().Logger()

	proxy := &MemcacheProxy{
		id:                       "test_noreply",
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
	var resp string

	// Step 1: Send a noreply command
	_, err = client.Write([]byte("set key 0 0 2 noreply\r\nhi\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Send a regular get command
	_, err = client.Write([]byte("get key\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	// Step 3: Read responses.
	client.SetDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 3; i++ {
		resp, err = reader.ReadString('\n')
		if err != nil {
			t.Fatalf("Failed to read response %d: %v", i+1, err)
		}
		t.Logf("Client received line %d: %q", i+1, resp)
	}
	if resp != "END\r\n" {
		t.Errorf("Expected last line to be END\\r\\n, got %q", resp)
	}

	// Step 4: Test Meta quiet command
	_, err = client.Write([]byte("ms key 2 q\r\nhi\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Write([]byte("mg key\r\n"))
	if err != nil {
		t.Fatal(err)
	}

	resp, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("Failed to read meta response: %v", err)
	}
	t.Logf("Client received meta line: %q", resp)
	if resp != "VA 2\r\n" {
		t.Errorf("Expected VA 2\\r\\n, got %q. Meta desynchronization occurred!", resp)
	}
}
