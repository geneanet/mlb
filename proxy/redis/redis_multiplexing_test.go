package redis

import (
	"context"
	"mlb/backend"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

func TestRedisProxyMultiplexing(t *testing.T) {
	// 1. Setup Mock Backend
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer ln.Close()
	backendAddr := ln.Addr().String()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				reader := NewRedisProtocolReader(c, 4096)
				defer reader.Release()
				for {
					msg, err := reader.ReadMessage(true)
					if err != nil {
						return
					}
					// Very simple mock redis
					smsg := string(msg)
					if smsg == "*1\r\n$4\r\nPING\r\n" || smsg == "PING\r\n" {
						c.Write([]byte("+PONG\r\n"))
					} else if smsg == "*1\r\n$5\r\nRESET\r\n" {
						c.Write([]byte("+OK\r\n"))
					} else if smsg == "*2\r\n$4\r\nECHO\r\n$2\r\nHI\r\n" {
						c.Write([]byte("$2\r\nHI\r\n"))
					} else if smsg == "*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n" {
						// RESP2 subscribe response
						c.Write([]byte("*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n"))
						// Push a message after a short delay
						time.Sleep(100 * time.Millisecond)
						c.Write([]byte("*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$4\r\ndata\r\n"))
					} else {
						// fmt.Printf("Mock received unknown: %q\n", smsg)
						c.Write([]byte("-ERR unknown\r\n"))
					}
					ReleaseBuffer(msg)
				}
			}(conn)
		}
	}()

	// 2. Setup Proxy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}

	reg := backend.NewRegistry()
	reg.Add(&backend.Backend{Address: backendAddr})

	p := &RedisProxy{
		id:                  "test-proxy",
		addresses:           []string{"127.0.0.1:0"},
		log:                 log.With().Str("test", "multiplexing").Logger(),
		bufferSize:          4096,
		backends:            reg,
		wg:                  wg,
		ctx:                 ctx,
		cancel:              cancel,
		idleTimeout:         time.Minute,
		connectTimeout:      time.Second,
		healthcheckTimeout:  time.Second,
		backendTCPKeepAlive: time.Second,
		beMetricsCache:      make(map[string]*Metrics),
	}
	p.backendConnectionPool = NewRedisBackendConnectionPool(p)

	// Manually trigger listen to get the random port
	lnProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	proxyAddr := lnProxy.Addr().String()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := lnProxy.Accept()
			if err != nil {
				return
			}
			p.connectionsWG.Add(1)
			go p.handleConnection(conn, &Metrics{
				processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_processed"}),
				active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_active"}),
				bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_in"}),
				bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_out"}),
				requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_requests"}),
				cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_cnx_errors"}),
			})
		}
	}()
	defer lnProxy.Close()

	// 3. Test Cases
	t.Run("Normal Command", func(t *testing.T) {
		conn, err := net.Dial("tcp", proxyAddr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer conn.Close()

		_, err = conn.Write([]byte("*2\r\n$4\r\nECHO\r\n$2\r\nHI\r\n"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := string(buf[:n]); got != "$2\r\nHI\r\n" {
			t.Errorf("expected %q, got %q", "$2\r\nHI\r\n", got)
		}
	})

	t.Run("PubSub", func(t *testing.T) {
		conn, err := net.Dial("tcp", proxyAddr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer conn.Close()

		_, err = conn.Write([]byte("*2\r\n$9\r\nSUBSCRIBE\r\n$2\r\nch\r\n"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		reader := NewRedisProtocolReader(conn, 1024)
		defer reader.Release()

		// 1. Subscribe confirmation
		resp, err := reader.ReadMessage(false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(string(resp), "subscribe") {
			t.Errorf("expected response to contain 'subscribe', got %q", string(resp))
		}
		ReleaseBuffer(resp)

		// 2. Async message
		resp, err = reader.ReadMessage(false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(string(resp), "message") {
			t.Errorf("expected response to contain 'message', got %q", string(resp))
		}
		if !strings.Contains(string(resp), "data") {
			t.Errorf("expected response to contain 'data', got %q", string(resp))
		}
		ReleaseBuffer(resp)
	})

	// ponytail: ensure everything is cleaned up before finishing the test
	cancel()
	lnProxy.Close()
	wg.Wait()
	p.connectionsWG.Wait()
}
