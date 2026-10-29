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

func TestMemcacheProxyGAT_ParsingAndRouting(t *testing.T) {
	// Backend server that verifies GAT / GATS command format:
	// "gat <exptime> <key>*\r\n" or "gats <exptime> <key>*\r\n"
	receivedCmds := make([]string, 0)
	var mu sync.Mutex

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
					mu.Lock()
					receivedCmds = append(receivedCmds, line)
					mu.Unlock()

					fields := bytes.Fields([]byte(line))
					if len(fields) < 3 {
						// According to ASCII protocol: gat/gats requires at least exptime AND one key
						_, _ = c.Write([]byte("CLIENT_ERROR bad command line format\r\n"))
						continue
					}
					cmd := string(fields[0])
					exptime := string(fields[1])
					_ = exptime

					switch cmd {
					case "gat":
						for _, k := range fields[2:] {
							_, _ = fmt.Fprintf(c, "VALUE %s 0 2\r\nok\r\n", string(k))
						}
						_, _ = c.Write([]byte("END\r\n"))
					case "gats":
						for _, k := range fields[2:] {
							_, _ = fmt.Fprintf(c, "VALUE %s 0 2 12345\r\nok\r\n", string(k))
						}
						_, _ = c.Write([]byte("END\r\n"))
					default:
						_, _ = c.Write([]byte("ERROR\r\n"))
					}
				}
			}(conn)
		}
	}()

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := &MemcacheProxy{
		id:                       "test_gat_proxy",
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

	b1 := backend.NewBackend(lBack.Addr().String(), nil)
	lBack2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lBack2.Close()
	go func() {
		for {
			conn, err := lBack2.Accept()
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
					mu.Lock()
					receivedCmds = append(receivedCmds, line)
					mu.Unlock()

					fields := bytes.Fields([]byte(line))
					if len(fields) < 3 {
						_, _ = c.Write([]byte("CLIENT_ERROR bad command line format\r\n"))
						continue
					}
					cmd := string(fields[0])
					switch cmd {
					case "gat":
						for _, k := range fields[2:] {
							_, _ = fmt.Fprintf(c, "VALUE %s 0 2\r\nok\r\n", string(k))
						}
						_, _ = c.Write([]byte("END\r\n"))
					case "gats":
						for _, k := range fields[2:] {
							_, _ = fmt.Fprintf(c, "VALUE %s 0 2 12345\r\nok\r\n", string(k))
						}
						_, _ = c.Write([]byte("END\r\n"))
					}
				}
			}(conn)
		}
	}()
	b2 := backend.NewBackend(lBack2.Addr().String(), nil)
	proxy.backends.Add(b1)
	proxy.backends.Add(b2)
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
				processed: metrics.FeCnxProcessed.WithLabelValues("test_gat", "test_gat"),
				active:    metrics.FeActCnx.WithLabelValues("test_gat", "test_gat"),
				bytesIn:   metrics.FeBytesIn.WithLabelValues("test_gat", "test_gat"),
				bytesOut:  metrics.FeBytesOut.WithLabelValues("test_gat", "test_gat"),
				cnxErrors: metrics.FeCnxErrors.WithLabelValues("test_gat", "test_gat"),
				requests:  metrics.FeRequests.WithLabelValues("test_gat", "test_gat"),
			})
		}
	}()

	client, err := net.Dial("tcp", lFront.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	reader := bufio.NewReader(client)

	// Test 1: gat key with exptime
	_, _ = client.Write([]byte("gat 3600 testkey\r\n"))
	resp1, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if resp1 != "VALUE testkey 0 2\r\n" {
		t.Fatalf("expected 'VALUE testkey 0 2\\r\\n', got %q", resp1)
	}
	val1, _ := reader.ReadString('\n')
	if val1 != "ok\r\n" {
		t.Fatalf("expected 'ok\\r\\n', got %q", val1)
	}
	end1, _ := reader.ReadString('\n')
	if end1 != "END\r\n" {
		t.Fatalf("expected 'END\\r\\n', got %q", end1)
	}

	// Test 2: gats key with exptime
	_, _ = client.Write([]byte("gats 120 testkey2\r\n"))
	resp2, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if resp2 != "VALUE testkey2 0 2 12345\r\n" {
		t.Fatalf("expected 'VALUE testkey2 0 2 12345\\r\\n', got %q", resp2)
	}
	val2, _ := reader.ReadString('\n')
	if val2 != "ok\r\n" {
		t.Fatalf("expected 'ok\\r\\n', got %q", val2)
	}
	end2, _ := reader.ReadString('\n')
	if end2 != "END\r\n" {
		t.Fatalf("expected 'END\\r\\n', got %q", end2)
	}

	// Test 3: format errors (missing exptime or key)
	for _, badCmd := range []string{"gat\r\n", "gat 3600\r\n", "gats\r\n", "gats 120\r\n"} {
		_, _ = client.Write([]byte(badCmd))
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("badCmd %q read error: %v", badCmd, err)
		}
		if line != "CLIENT_ERROR bad command line format\r\n" {
			t.Fatalf("badCmd %q: expected CLIENT_ERROR bad command line format\\r\\n, got %q", badCmd, line)
		}
	}

	// Verify backend received exact reconstructed command "gat 3600 testkey\r\n"
	mu.Lock()
	defer mu.Unlock()
	foundGAT := false
	for _, cmd := range receivedCmds {
		if cmd == "gat 3600 testkey\r\n" {
			foundGAT = true
		}
	}
	if !foundGAT {
		t.Fatalf("backend never received 'gat 3600 testkey\\r\\n', received: %v", receivedCmds)
	}
}
