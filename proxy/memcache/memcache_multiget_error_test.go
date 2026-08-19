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
	t *testing.T
}

func (tw testWriterMG) Write(p []byte) (n int, err error) {
	tw.t.Log(string(bytes.TrimSpace(p)))
	return len(p), nil
}

func TestMemcacheProxy_MultiGetPartialFailure(t *testing.T) {
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
				conn.Write([]byte("VALUE k1 0 2\r\nv1\r\nEND\r\n"))
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
					c.Close()
				}
			}(conn)
		}
	}()

	// 3. Setup Proxy
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: testWriterMG{t}}).With().Timestamp().Logger()

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
		keys.WriteString(fmt.Sprintf(" k%d", i))
	}
	_, _ = client.Write([]byte(fmt.Sprintf("get%s\r\n", keys.String())))

	reader := bufio.NewReader(client)
	client.SetDeadline(time.Now().Add(5 * time.Second))

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
