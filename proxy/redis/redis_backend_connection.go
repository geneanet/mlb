package redis

import (
        "bytes"
        "context"
        "fmt"
        "net"
        "strconv"
        "sync"
        "sync/atomic"
        "time"

        "mlb/backend"
)

//--------------------------
// Redis Backend Connection
//--------------------------

// RedisBackendConnection represents a single persistent connection to a Redis backend.
// It manages the TCP connection, metrics, and lifecycle (context-based).
type RedisBackendConnection struct {
	pool        *RedisBackendConnectionPool
	backend     *backend.Backend
	conn        net.Conn           // The actual TCP connection to the Redis backend
	ctx         context.Context    // Connection-scoped context
	cancel      context.CancelFunc // Used to close the connection and signal cleanup
	metrics     *Metrics           // Prometheus metrics for this backend
	failureErr  error              // The error that caused the connection to fail, if any
	failureOnce sync.Once          // Ensures we only process failure once
	lastUsed    time.Time          // Timestamp of when it was last returned to the pool
	resetCount  uint64             // Counter used to generate unique ECHO markers without allocation
	}

// fail marks the connection as failed, cancels its context, and logs the error.
func (rbc *RedisBackendConnection) fail(err error) {
	rbc.failureOnce.Do(func() {
		rbc.pool.proxy.log.Warn().Err(err).Str("peer", rbc.backend.Address).Msg("Backend connection failed")
		rbc.failureErr = err
		rbc.cancel()
	})
}

// NewRedisBackendConnection creates a new RedisBackendConnection, dials the backend,
// and starts a background cleanup routine that closes the TCP connection when the context is done.
func NewRedisBackendConnection(pool *RedisBackendConnectionPool, backend *backend.Backend) (rbc *RedisBackendConnection, e error) {
	rbc = &RedisBackendConnection{
		pool:    pool,
		backend: backend,
		metrics: pool.proxy.getBackendMetrics(backend.Address),
	}

	rbc.ctx, rbc.cancel = context.WithCancel(pool.ctx)

	// Increment Prometheus processed connection counter.
	rbc.metrics.processed.Inc()

	// Open the TCP connection to the backend.
	pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Opening Backend connection")
	dialer := &net.Dialer{
		Timeout:   pool.proxy.connectTimeout,
		KeepAlive: pool.proxy.backendTCPKeepAlive,
	}
	connBack, err := dialer.DialContext(rbc.ctx, "tcp", rbc.backend.Address)
	if err != nil {
		return nil, err
	}

	rbc.conn = connBack
	rbc.lastUsed = time.Now()

	// Increment Prometheus active connection gauge.
	rbc.metrics.active.Inc()

	// Background cleanup routine: close the connection when the context is cancelled.
	go func() {
		<-rbc.ctx.Done()
		pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Closing Backend connection")
		_ = rbc.conn.Close()
		rbc.metrics.active.Dec()
	}()

	return rbc, nil
}

// Healthcheck sends a PING command to the backend and waits for a PONG.
// It is used to verify connection viability before reusing it from the pool.
func (rbc *RedisBackendConnection) Healthcheck() error {
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Performing healthcheck")
	_ = rbc.conn.SetDeadline(time.Now().Add(rbc.pool.proxy.healthcheckTimeout))
	defer func() { _ = rbc.conn.SetDeadline(time.Time{}) }()

	_, err := rbc.conn.Write([]byte("PING\r\n"))
	if err != nil {
		return err
	}

	reader := NewRedisProtocolReader(rbc.conn, rbc.pool.proxy.bufferSize)
	defer reader.Release()

	resp, err := reader.ReadMessage(false)
	if err != nil {
		return err
	}
	defer ReleaseBuffer(resp)

	if string(resp) != "+PONG\r\n" {
		return fmt.Errorf("unexpected healthcheck response: %s", string(resp))
	}

	return nil
}

// ResetAndRelease prepares the connection for reuse by sending a Redis RESET command.
// This clears any client state (like subscriptions or transactions) before returning
// the connection to the idle pool.
func (rbc *RedisBackendConnection) ResetAndRelease() {
	if rbc.ctx.Err() != nil {
		rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Not releasing backend connection (context cancelled)")
		return
	}

	// Generate a unique token to synchronize the connection state without allocation.
	count := atomic.AddUint64(&rbc.resetCount, 1)
	var tokenBuf [64]byte // Stack buffer for token and RESP encoding
	token := append(tokenBuf[:0], "MLB_RESET_FLAG_"...)
	token = strconv.AppendUint(token, count, 10)

	// Send RESET and ECHO <token> commands to ensure the connection is in a clean state.
	// We use ECHO as a marker to know when the RESET command has been fully processed.
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Bytes("token", token).Msg("Sending RESET and ECHO to backend connection")

	// Construct the pipelined command using a stack buffer to avoid allocations.
	// RESET: *1\r\n$5\r\nRESET\r\n
	// ECHO:  *2\r\n$4\r\nECHO\r\n$<token_len>\r\n<token>\r\n
	var cmdBuf [128]byte
	cmd := append(cmdBuf[:0], "*1\r\n$5\r\nRESET\r\n*2\r\n$4\r\nECHO\r\n$"...)
	cmd = strconv.AppendInt(cmd, int64(len(token)), 10)
	cmd = append(cmd, "\r\n"...)
	cmd = append(cmd, token...)
	cmd = append(cmd, "\r\n"...)

	_, err := rbc.conn.Write(cmd)
	if err != nil {
	        rbc.fail(fmt.Errorf("failed to send RESET/ECHO: %w", err))
	        return
	}

	// Drain responses from the backend until we receive the ECHO confirmation.
	// This is necessary because there might be pending data (like Pub/Sub messages) in the buffer.
	reader := NewRedisProtocolReader(rbc.conn, rbc.pool.proxy.bufferSize)
	defer reader.Release()

	// Set a short deadline for the reset operation to prevent hanging.
	_ = rbc.conn.SetDeadline(time.Now().Add(rbc.pool.proxy.resetTimeout))
	defer func() { _ = rbc.conn.SetDeadline(time.Time{}) }()

	// Pre-calculate expected ECHO response to avoid allocations in the loop
	var expectBuf [64]byte
	expectedResponse := append(expectBuf[:0], '$')
	expectedResponse = strconv.AppendInt(expectedResponse, int64(len(token)), 10)
	expectedResponse = append(expectedResponse, "\r\n"...)
	expectedResponse = append(expectedResponse, token...)
	expectedResponse = append(expectedResponse, "\r\n"...)

	for {
		item, err := reader.ReadMessage(false)
		if err != nil {
			rbc.fail(fmt.Errorf("error while draining backend connection: %w", err))
			return
		}

		// When we see the expected ECHO response, we know the connection is clean and ready to be reused.
		if bytes.Equal(item, expectedResponse) {
			rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Backend connection reset successful")
			ReleaseBuffer(item)
			rbc.lastUsed = time.Now()
			rbc.pool.Put(rbc)
			return
		}

		// Discard any other messages received while waiting for the reset response.
		ReleaseBuffer(item)
	}
}
