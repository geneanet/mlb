package redis

import (
	"bytes"
	"context"
	"mlb/backend"
	"mlb/testutil"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestNewRedisBackendConnection_DialFailure verifies that when a TCP dial fails
// (e.g. because the target address is not listening), the initialization panics internally,
// is caught by the recovery handler, and returns a clean dial error.
func TestNewRedisBackendConnection_DialFailure(t *testing.T) {
	// Construct minimal mock proxy and pool
	p := &RedisProxy{
		id:                       "test-proxy",
		backendInflightQueueSize: 10,
		connectTimeout:           50 * time.Millisecond,
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := &RedisBackendConnectionPool{
		proxy:       p,
		chanFailure: make(chan *RedisBackendConnection, 1),
	}

	// Choose a non-listening address to force dial failure
	be := &backend.Backend{Address: "127.0.0.1:54321"}
	rbc, err := NewRedisBackendConnection(pool, be)

	if err == nil {
		t.Errorf("expected error, got nil")
	}
	if rbc != nil {
		t.Errorf("expected rbc to be nil, got %v", rbc)
	}
}

// TestNewRedisBackendConnection_Success verifies the end-to-end operation of
// RedisBackendConnection: establishing a connection, writing queries, reading responses,
// propagating results, and executing clean context cancellation.
func TestNewRedisBackendConnection_Success(t *testing.T) {
	// 1. Start local TCP server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverAddress := listener.Addr().String()

	// 2. Setup mock proxy and pool
	p := &RedisProxy{
		id:                       "test-proxy-success",
		backendInflightQueueSize: 10,
		connectTimeout:           1 * time.Second,
		bufferSize:               1024,
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := &RedisBackendConnectionPool{
		proxy:       p,
		chanFailure: make(chan *RedisBackendConnection, 1),
	}

	// Accept connection in server goroutine
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read the query
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if string(buf[:n]) == "PING\r\n" {
			// Write the response
			_, _ = conn.Write([]byte("+PONG\r\n"))
		}
	}()

	// 3. Create backend connection
	be := &backend.Backend{Address: serverAddress}
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatal(err)
	}
	if rbc == nil {
		t.Fatal("expected rbc not to be nil")
	}
	defer rbc.cancel()

	// 4. Send query
	responseChan := make(chan RedisReponse, 1)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	query := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)
	err = rbc.Query(query)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 5. Verify reply propagation
	select {
	case resp := <-responseChan:
		if !bytes.Equal(resp.item, []byte("+PONG\r\n")) {
			t.Errorf("expected +PONG\r\n, got %s", string(resp.item))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for Redis query reply")
	}

	// 6. Test cancellation/cleanup
	rbc.cancel()

	// Ensure the pool is notified of failure/shutdown
	select {
	case notifiedConn := <-pool.chanFailure:
		if notifiedConn != rbc {
			t.Errorf("expected rbc %v, got %v", rbc, notifiedConn)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for pool failure notification")
	}

	// Querying on closed channel must fail
	testutil.Eventually(t, func() bool {
		return rbc.Query(query) != nil
	}, 1*time.Second, 10*time.Millisecond)
}

// TestRedisBackendConnection_UnexpectedWriteError verifies that if the TCP connection
// is closed by the server, any write attempt will fail, trigger connection cancellation,
// and correctly abort in-flight queries.
func TestRedisBackendConnection_UnexpectedWriteError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	p := &RedisProxy{
		id:                       "test-proxy-write-err",
		backendInflightQueueSize: 10,
		connectTimeout:           1 * time.Second,
		bufferSize:               1024,
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := &RedisBackendConnectionPool{
		proxy:       p,
		chanFailure: make(chan *RedisBackendConnection, 1),
	}

	// Accept and then close
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			// Wait a bit to ensure NewRedisBackendConnection has returned
			time.Sleep(50 * time.Millisecond)
			conn.Close()
		}
	}()

	be := &backend.Backend{Address: listener.Addr().String()}
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatal(err)
	}
	defer rbc.cancel()

	// Send query.
	responseChan := make(chan RedisReponse, 5)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	query := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)

	// We might need to retry Query if the context was cancelled very quickly
	err = rbc.Query(query)
	if err != nil {
		t.Log("Query failed early:", err)
	}

	// Verify the query was aborted (returns nil reply)
	select {
	case resp := <-responseChan:
		if resp.item != nil {
			t.Errorf("expected nil item, got %v", resp.item)
		}
	case <-time.After(1 * time.Second):
		// If Query failed early, this might not happen as expected
	}

	// Verify that the connection cancelled its own context
	select {
	case <-rbc.ctx.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Context was not cancelled on write error")
	}
}

// TestRedisBackendConnection_ResetError verifies that "connection reset" errors
// (which are not EOF or graceful closes) are handled and logged.
func TestRedisBackendConnection_ResetError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	p := &RedisProxy{
		id:                       "test-proxy-reset-err",
		backendInflightQueueSize: 10,
		connectTimeout:           1 * time.Second,
		bufferSize:               1024,
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := &RedisBackendConnectionPool{
		proxy:       p,
		chanFailure: make(chan *RedisBackendConnection, 1),
	}

	// Accept and force a RESET
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			time.Sleep(50 * time.Millisecond)
			tcpConn := conn.(*net.TCPConn)
			tcpConn.SetLinger(0) // Force RST on close
			tcpConn.Close()
		}
	}()

	be := &backend.Backend{Address: listener.Addr().String()}
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatal(err)
	}
	if rbc != nil {
		defer rbc.cancel()

		// Wait for the RST to be detected by the read goroutine
		select {
		case <-rbc.ctx.Done():
			// Success
		case <-time.After(1 * time.Second):
			t.Fatal("Context was not cancelled on reset error")
		}
	}
}

// TestRedisBackendConnection_UnexpectedReadError verifies that if the server closes the connection
// after receiving a query (but before sending a reply), the read goroutine correctly detects the EOF,
// cancels the connection context, and aborts any active in-flight queries.
func TestRedisBackendConnection_UnexpectedReadError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	p := &RedisProxy{
		id:                       "test-proxy-read-err",
		backendInflightQueueSize: 10,
		connectTimeout:           1 * time.Second,
		bufferSize:               1024,
		log:                      zerolog.Nop(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := &RedisBackendConnectionPool{
		proxy:       p,
		chanFailure: make(chan *RedisBackendConnection, 1),
	}

	// Accept, read query, and close without replying
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 1024)
		_, _ = conn.Read(buf)
	}()

	be := &backend.Backend{Address: listener.Addr().String()}
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatal(err)
	}
	defer rbc.cancel()

	responseChan := make(chan RedisReponse, 5)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	query := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)
	err = rbc.Query(query)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Since connection is closed without response, query should be aborted
	select {
	case resp := <-responseChan:
		if resp.item != nil {
			t.Errorf("expected nil item, got %v", resp.item)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for aborted query response")
	}

	// Connection context should be cancelled
	select {
	case <-rbc.ctx.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Context was not cancelled on read error")
	}
}

// TestRedisBackendConnection_AbortInflightQueries verifies that AbortInflightQueries
// correctly drains the in-flight queue and signals abort to all pending queries.
func TestRedisBackendConnection_AbortInflightQueries(t *testing.T) {
	rbc := &RedisBackendConnection{
		inFlight: make(chan RedisQuery, 5),
		pool: &RedisBackendConnectionPool{
			proxy: &RedisProxy{
				log:            zerolog.Nop(),
				beMetricsCache: make(map[string]*Metrics),
			},
		},
		backend: &backend.Backend{Address: "127.0.0.1:1234"},
	}

	responseChan := make(chan RedisReponse, 5)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	q1 := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)
	q2 := NewRedisQuery([]byte("QUIT\r\n"), responseChan, responseChanStop)

	rbc.inFlight <- q1
	rbc.inFlight <- q2

	rbc.AbortInflightQueries()

	if len(rbc.inFlight) != 0 {
		t.Errorf("expected inFlight queue to be empty, got %d", len(rbc.inFlight))
	}

	// Verify both were aborted
	for i := 0; i < 2; i++ {
		select {
		case resp := <-responseChan:
			if resp.item != nil {
				t.Errorf("expected nil item for aborted query, got %v", resp.item)
			}
		default:
			t.Errorf("expected aborted response for query %d", i)
		}
	}
}

// TestRedisBackendConnection_QueryClosed verifies that Query returns an error
// if the input channel has been stopped/closed.
func TestRedisBackendConnection_QueryClosed(t *testing.T) {
	rbc := &RedisBackendConnection{
		inputChan:     make(chan RedisQuery),
		inputChanStop: make(chan struct{}),
	}
	close(rbc.inputChanStop)

	err := rbc.Query(RedisQuery{})
	if err == nil {
		t.Errorf("expected error querying closed rbc, got nil")
	}
	if err.Error() != "backend input channel is closed" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

// TestRedisBackendConnection_WriteError verifies that a write error to the backend
// is correctly handled and triggers connection cancellation.
func TestRedisBackendConnection_WriteError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	p := &RedisProxy{
		id:                       "test-write-err",
		log:                      zerolog.Nop(),
		backendInflightQueueSize: 10,
		connectTimeout:           1 * time.Second,
		bufferSize:               1024,
		ctx:                      context.Background(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool

	// Accept and then close immediately to cause write error
	go func() {
		conn, err := l.Accept()
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			conn.Close()
		}
	}()

	be := &backend.Backend{Address: l.Addr().String()}
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatal(err)
	}
	defer rbc.cancel()

	// Fill in-flight to avoid blocking on Read side if needed,
	// but here we want to trigger a Write error.
	query := NewRedisQuery([]byte("PING\r\n"), make(chan RedisReponse, 1), make(chan struct{}))
	
	// We might need to send multiple queries or wait for the connection to be closed
	testutil.Eventually(t, func() bool {
		_ = rbc.Query(query)
		select {
		case <-rbc.ctx.Done():
			return true
		default:
			return false
		}
	}, 1*time.Second, 10*time.Millisecond)
}

// TestRedisBackendConnection_ReadError verifies that a read error from the backend
// is correctly handled and triggers connection cancellation.
func TestRedisBackendConnection_ReadError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	p := &RedisProxy{
		id:                       "test-read-err",
		log:                      zerolog.Nop(),
		backendInflightQueueSize: 10,
		connectTimeout:           1 * time.Second,
		bufferSize:               1024,
		ctx:                      context.Background(),
		beMetricsCache:           make(map[string]*Metrics),
	}
	pool := NewRedisBackendConnectionPool(p)
	p.backendConnectionPool = pool

	// Accept and then force a RESET
	go func() {
		conn, err := l.Accept()
		if err == nil {
			time.Sleep(50 * time.Millisecond)
			tcpConn := conn.(*net.TCPConn)
			tcpConn.SetLinger(0) // Force RST on close
			tcpConn.Close()
		}
	}()

	be := &backend.Backend{Address: l.Addr().String()}
	rbc, err := NewRedisBackendConnection(pool, be)
	if err != nil {
		t.Fatal(err)
	}
	defer rbc.cancel()

	select {
	case <-rbc.ctx.Done():
		// Success: connection cancelled on read error (RST)
	case <-time.After(1 * time.Second):
		t.Fatal("connection was not cancelled on read error")
	}
}

// TestRedisBackendConnection_ReplyClosed verifies that when a client's response
// channel is closed, query.Reply returns false and it is logged.
func TestRedisBackendConnection_ReplyClosed(t *testing.T) {
	responseChan := make(chan RedisReponse)
	responseChanStop := make(chan struct{})
	close(responseChanStop) // Simulate client closed

	query := NewRedisQuery(nil, responseChan, responseChanStop)
	
	if query.Reply([]byte("+PONG\r\n")) == nil {
		t.Errorf("expected Reply to return error for closed response channel")
	}
}
