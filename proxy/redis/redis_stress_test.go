package redis

import (
	"bufio"
	"context"
	"io"
	"mlb/backend"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestRedisStressPipelining(t *testing.T) {
	// Setup mock backend
	lBack, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lBack.Close()
	backendAddr := lBack.Addr().String()

	go func() {
		for {
			conn, err := lBack.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				reader := NewRedisProtocolReader(c, 1024)
				for {
					_, err := reader.ReadMessage(true)
					if err != nil {
						return
					}
					// Reply +PONG\r\n
					c.Write([]byte("+PONG\r\n"))
				}
			}(conn)
		}
	}()

	// Setup proxy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &RedisProxy{
		id:                        "stress-test",
		log:                       zerolog.Nop(),
		connectTimeout:            time.Second,
		closeTimeout:              time.Second,
		ctx:                       ctx,
		cancel:                    cancel,
		backends:                  backend.NewRegistry(),
		backendMinConnections:     4,
		backendMaxConnections:     4,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		bufferSize:                4096,
		clientQueueSize:           128,
		beMetricsCache:           make(map[string]*Metrics),
	}
	p.backends.Add(&backend.Backend{Address: backendAddr})
	p.backendConnectionPool = NewRedisBackendConnectionPool(p)
	p.backendConnectionPool.Update()

	// Frontend listener
	lFront, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lFront.Close()

	go func() {
		for {
			conn, err := lFront.Accept()
			if err != nil {
				return
			}
			p.connectionsWG.Add(1)
			go p.handleConnection(conn, dummyMetrics())
		}
	}()

	// Multiple clients
	numClients := 10
	requestsPerClient := 1000
	wg := sync.WaitGroup{}
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			client, err := net.Dial("tcp", lFront.Addr().String())
			if err != nil {
				t.Errorf("Client %d failed to connect: %v", clientID, err)
				return
			}
			defer client.Close()

			reader := bufio.NewReader(client)
			
			// Send PINGs in batches to test pipelining
			batchSize := 50
			for b := 0; b < requestsPerClient/batchSize; b++ {
				for j := 0; b*batchSize+j < requestsPerClient && j < batchSize; j++ {
					client.Write([]byte("PING\r\n"))
				}
				
				for j := 0; b*batchSize+j < requestsPerClient && j < batchSize; j++ {
					resp := make([]byte, 7)
					_, err := io.ReadFull(reader, resp)
					if err != nil {
						t.Errorf("Client %d failed to read response %d: %v", clientID, b*batchSize+j, err)
						return
					}
					if string(resp) != "+PONG\r\n" {
						t.Errorf("Client %d got unexpected response %d: %q", clientID, b*batchSize+j, string(resp))
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
}
