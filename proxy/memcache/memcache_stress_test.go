package memcache

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mlb/backend"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMemcacheStressPipelining(t *testing.T) {
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
				reader := bufio.NewReader(c)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if bytes.HasPrefix([]byte(line), []byte("set ")) {
						// Simplistic: read payload and reply STORED
						// set <key> <flags> <exptime> <bytes> [noreply]\r\n
						fields := bytes.Fields([]byte(line))
						if len(fields) < 5 {
							_, _ = c.Write([]byte("ERROR\r\n"))
							continue
						}
						// bytes is fields[4]
						size := 0
						for _, b := range fields[4] {
							if b >= '0' && b <= '9' {
								size = size*10 + int(b-'0')
							}
						}
						payload := make([]byte, size+2)
						_, _ = io.ReadFull(reader, payload)
						_, _ = c.Write([]byte("STORED\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("get ")) {
						// Reply END\r\n (empty get for simplicity, or we could echo)
						_, _ = c.Write([]byte("END\r\n"))
					} else if bytes.HasPrefix([]byte(line), []byte("delete ")) {
						_, _ = c.Write([]byte("DELETED\r\n"))
					} else {
						_, _ = c.Write([]byte("OK\r\n"))
					}
				}
			}(conn)
		}
	}()

	// Setup proxy
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &MemcacheProxy{
		id:                       "stress-test",
		log:                      zerolog.Nop(),
		connectTimeout:           time.Second,
		closeTimeout:             time.Second,
		ctx:                      ctx,
		cancel:                   cancel,
		backends:                 backend.NewRegistry(zerolog.Nop(), false),
		ring:                     newMemcacheHashRing(),
		backendMinConnections:    4,
		backendMaxConnections:    4,
		backendInputQueueSize:    1024,
		backendInflightQueueSize: 512,
		bufferSize:               4096,
		clientQueueSize:          128,
		beMetricsCache:           make(map[string]*Metrics),
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
	}
	p.backends.Add(&backend.Backend{Address: backendAddr})
	p.ring.update(p.backends.GetList())
	p.backendConnectionPool = NewMemcacheBackendConnectionPool(p)
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
	requestsPerClient := 500
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

			// Send commands in batches to test pipelining
			batchSize := 20
			for b := 0; b < requestsPerClient/batchSize; b++ {
				for j := 0; b*batchSize+j < requestsPerClient && j < batchSize; j++ {
					// Alternate between SET and GET
					if j%2 == 0 {
						_, _ = fmt.Fprintf(client, "set k%d_%d 0 0 2\r\nhi\r\n", clientID, b*batchSize+j)
					} else {
						_, _ = fmt.Fprintf(client, "get k%d_%d\r\n", clientID, b*batchSize+j)
					}
				}

				for j := 0; b*batchSize+j < requestsPerClient && j < batchSize; j++ {
					if j%2 == 0 {
						// Expect STORED\r\n
						resp := make([]byte, 8)
						_, err := io.ReadFull(reader, resp)
						if err != nil {
							t.Errorf("Client %d failed to read STORED %d: %v", clientID, b*batchSize+j, err)
							return
						}
						if string(resp) != "STORED\r\n" {
							t.Errorf("Client %d got unexpected response %d: %q", clientID, b*batchSize+j, string(resp))
							return
						}
					} else {
						// Expect END\r\n
						resp := make([]byte, 5)
						_, err := io.ReadFull(reader, resp)
						if err != nil {
							t.Errorf("Client %d failed to read END %d: %v", clientID, b*batchSize+j, err)
							return
						}
						if string(resp) != "END\r\n" {
							t.Errorf("Client %d got unexpected response %d: %q", clientID, b*batchSize+j, string(resp))
							return
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
}
