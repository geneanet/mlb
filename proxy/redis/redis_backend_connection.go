package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"mlb/backend"
	"net"
	"sync"
	"time"
)

//--------------------------
// Redis Backend Connection
//--------------------------

type responseExpectation int

const (
	expectNormal responseExpectation = iota
	expectSubConfirmation
)

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
		rbc.conn.Close()
		rbc.metrics.active.Dec()
	}()

	return rbc, nil
}

// Healthcheck sends a PING command to the backend and waits for a PONG.
// It is used to verify connection viability before reusing it from the pool.
func (rbc *RedisBackendConnection) Healthcheck() error {
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Performing healthcheck")
	rbc.conn.SetDeadline(time.Now().Add(rbc.pool.proxy.healthcheckTimeout))
	defer rbc.conn.SetDeadline(time.Time{})

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

	// Generate a unique token to synchronize the connection state.
	token := make([]byte, 8)
	if _, err := rand.Read(token); err != nil {
		rbc.fail(fmt.Errorf("failed to generate reset token: %w", err))
		return
	}
	hexToken := hex.EncodeToString(token)

	// Send RESET and ECHO <token> commands to ensure the connection is in a clean state.
	// We use ECHO as a marker to know when the RESET command has been fully processed.
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Str("token", hexToken).Msg("Sending RESET and ECHO to backend connection")

	// Construct the pipelined command:
	// RESET: *1\r\n$5\r\nRESET\r\n
	// ECHO:  *2\r\n$4\r\nECHO\r\n$16\r\n<hexToken>\r\n
	var buf bytes.Buffer
	buf.WriteString("*1\r\n$5\r\nRESET\r\n")
	buf.WriteString("*2\r\n$4\r\nECHO\r\n$16\r\n")
	buf.WriteString(hexToken)
	buf.WriteString("\r\n")

	_, err := rbc.conn.Write(buf.Bytes())
	if err != nil {
		rbc.fail(fmt.Errorf("failed to send RESET/ECHO: %w", err))
		return
	}

	// Drain responses from the backend until we receive the ECHO confirmation.
	// This is necessary because there might be pending data (like Pub/Sub messages) in the buffer.
	reader := NewRedisProtocolReader(rbc.conn, rbc.pool.proxy.bufferSize)
	defer reader.Release()

	// Set a short deadline for the reset operation to prevent hanging.
	rbc.conn.SetDeadline(time.Now().Add(2 * time.Second))
	defer rbc.conn.SetDeadline(time.Time{})

	expectedResponse := []byte("$16\r\n" + hexToken + "\r\n")

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
