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
// It proceeds in two steps:
// 1. Synchronize the connection by sending an ECHO marker and draining any pending responses.
// 2. Send the RESET command and verify it returns "+OK\r\n".
// If any step fails or returns an unexpected response, the connection is marked as failed.
func (rbc *RedisBackendConnection) ResetAndRelease() {
	if rbc.ctx.Err() != nil {
		rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Not releasing backend connection (context cancelled)")
		return
	}

	// Set a deadline for the entire reset operation to prevent hanging.
	_ = rbc.conn.SetDeadline(time.Now().Add(rbc.pool.proxy.resetTimeout))
	defer func() { _ = rbc.conn.SetDeadline(time.Time{}) }()

	reader := NewRedisProtocolReader(rbc.conn, rbc.pool.proxy.bufferSize)
	defer reader.Release()

	// Step 1: Sync by draining pending responses from the backend until we receive an ECHO confirmation.
	// This is necessary because there might be pending data (like Pub/Sub messages) in the buffer.
	count := atomic.AddUint64(&rbc.resetCount, 1)
	var tokenBuf [64]byte // Stack buffer for token and RESP encoding
	token := append(tokenBuf[:0], "MLB_SYNC_FLAG_"...)
	token = strconv.AppendUint(token, count, 10)

	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Bytes("token", token).Msg("Syncing backend connection with ECHO")

	// Construct the ECHO command: *2\r\n$4\r\nECHO\r\n$<token_len>\r\n<token>\r\n
	var echoCmdBuf [128]byte
	echoCmd := append(echoCmdBuf[:0], "*2\r\n$4\r\nECHO\r\n$"...)
	echoCmd = strconv.AppendInt(echoCmd, int64(len(token)), 10)
	echoCmd = append(echoCmd, "\r\n"...)
	echoCmd = append(echoCmd, token...)
	echoCmd = append(echoCmd, "\r\n"...)

	if _, err := rbc.conn.Write(echoCmd); err != nil {
		rbc.fail(fmt.Errorf("failed to send ECHO sync: %w", err))
		return
	}

	// Pre-calculate expected ECHO response to avoid allocations in the loop
	var expectEchoBuf [64]byte
	expectedEcho := append(expectEchoBuf[:0], '$')
	expectedEcho = strconv.AppendInt(expectedEcho, int64(len(token)), 10)
	expectedEcho = append(expectedEcho, "\r\n"...)
	expectedEcho = append(expectedEcho, token...)
	expectedEcho = append(expectedEcho, "\r\n"...)

	for {
		item, err := reader.ReadMessage(false)
		if err != nil {
			rbc.fail(fmt.Errorf("error while syncing backend connection: %w", err))
			return
		}
		ReleaseBuffer(item)

		isEcho := bytes.Equal(item, expectedEcho)

		if isEcho {
			break
		}
	}

	// Step 2: Send RESET to clear any client state (like subscriptions or transactions) and verify its response.
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Sending RESET to backend connection")
	if _, err := rbc.conn.Write([]byte("*1\r\n$5\r\nRESET\r\n")); err != nil {
		rbc.fail(fmt.Errorf("failed to send RESET: %w", err))
		return
	}

	resp, err := reader.ReadMessage(false)
	if err != nil {
		rbc.fail(fmt.Errorf("error while reading RESET response: %w", err))
		return
	}
	defer ReleaseBuffer(resp)

	if !bytes.Equal(resp, []byte("+RESET\r\n")) {
		rbc.fail(fmt.Errorf("unexpected RESET response: %q", string(resp)))
		return
	}

	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Backend connection reset successful")
	rbc.lastUsed = time.Now()
	rbc.pool.Put(rbc)
}
