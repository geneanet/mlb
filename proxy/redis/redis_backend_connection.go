package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"net"
	"sync"
)

//--------------------------
// Redis Backend Connection
//--------------------------

// RedisBackendConnection represents a single persistent connection to a Redis backend.
type RedisBackendConnection struct {
	pool          *RedisBackendConnectionPool
	backend       *backend.Backend
	conn          net.Conn
	inputChanStop chan struct{}
	inputChan     chan RedisQuery
	inFlight      chan RedisQuery
	ctx           context.Context
	cancel        context.CancelFunc
	metrics       *Metrics
	failureErr    error
	failureOnce   sync.Once
}

func (rbc *RedisBackendConnection) fail(err error) {
	rbc.failureOnce.Do(func() {
		rbc.failureErr = err
		rbc.cancel()
	})
}

// NewRedisBackendConnection creates a new RedisBackendConnection and starts its lifecycle.
func NewRedisBackendConnection(pool *RedisBackendConnectionPool, backend *backend.Backend) (rbc *RedisBackendConnection, e error) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				e = err
			} else {
				e = fmt.Errorf("%v", r)
			}
			rbc = nil
		}
	}()

	rbc = &RedisBackendConnection{
		pool:          pool,
		backend:       backend,
		inputChan:     make(chan RedisQuery, pool.proxy.backendInputQueueSize), // ponytail: buffered to allow saturation check
		inputChanStop: make(chan struct{}),
		inFlight:      make(chan RedisQuery, pool.proxy.backendInflightQueueSize),
		metrics:       pool.proxy.getBackendMetrics(backend.Address),
	}

	rbc.ctx, rbc.cancel = context.WithCancel(context.Background())

	// Prometheus
	rbc.metrics.processed.Inc()

	// Open backend connection
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Opening Backend connection")
	dialer := &net.Dialer{
		Timeout:   rbc.pool.proxy.connectTimeout,
		KeepAlive: rbc.pool.proxy.backendTCPKeepAlive,
	}
	connBack, err := dialer.DialContext(rbc.ctx, "tcp", rbc.backend.Address)
	if err != nil {
		panic(err)
	}

	rbc.conn = connBack

	// Prometheus
	rbc.metrics.active.Inc()

	// Cleanup routine: If the connection context is closed, ensure the connection is closed, abort all in flight request and notify the pool
	context.AfterFunc(rbc.ctx, func() {
		// Ensure the connection is closed
		rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Closing Backend connection")
		rbc.conn.Close()

		// Ensure no new queries can be added to the input queue
		close(rbc.inputChanStop)

		// Abort all in flight requests
		abortedCount := rbc.AbortInflightQueries()

		// Notify the pool
		rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Notifying pool")
		err := rbc.failureErr
		if err == nil {
			err = rbc.ctx.Err()
		}
		rbc.pool.NotifyFailure(rbc, err, abortedCount > 0)

		// Prometheus
		rbc.metrics.active.Dec()
	})

	// Read queries and send them to the backend
	go func() {
		batch := make([]RedisQuery, 0, 32)
		for {
			select {
			case query := <-rbc.inputChan:
				batch = append(batch[:0], query)
				rbc.inFlight <- query

				// Drain available queries
				for len(rbc.inputChan) > 0 && len(batch) < cap(batch) && len(rbc.inFlight) < cap(rbc.inFlight) {
					next := <-rbc.inputChan
					batch = append(batch, next)
					rbc.inFlight <- next
				}

				var buffers net.Buffers
				for _, q := range batch {
					buffers = append(buffers, q.item)
				}

				n, err := buffers.WriteTo(rbc.conn)
				for _, q := range batch {
					ReleaseBuffer(q.item)
				}

				if err != nil {
					if err != io.EOF && !errors.Is(err, net.ErrClosed) {
						rbc.pool.proxy.log.Error().Str("peer", rbc.backend.Address).Err(err).Msg("Unexpected error while sending query to the backend")
					}
					rbc.fail(err)
					rbc.AbortInflightQueries() // Extra call to AbortInflightQueries in case the query we were processing has not been aborted by the "cleanup" goroutine
					return
				}
				rbc.metrics.requests.Add(float64(len(batch)))
				rbc.metrics.bytesOut.Add(float64(n))
			case <-rbc.ctx.Done():
				return
			}
		}
	}()

	// Read backend responses and send them to the client
	go func() {
		reader := NewRedisProtocolReader(rbc.conn, rbc.pool.proxy.bufferSize)
		defer reader.Release()

		for {
			item, err := reader.ReadMessage(false)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					rbc.pool.proxy.log.Error().Str("peer", rbc.backend.Address).Err(err).Msg("Unexpected error while reading from the backend")
				}
				rbc.fail(err)
				return
			}
			rbc.metrics.bytesIn.Add(float64(len(item)))
			var query RedisQuery
			select {
			case query = <-rbc.inFlight:
			case <-rbc.ctx.Done():
				return
			}

			err = query.Reply(item)
			if err != nil {
				if err.Error() == "response channel is closed" {
					rbc.pool.proxy.log.Debug().Uint64("queryId", query.id).Msg("Unable to reply to client: response channel is closed")
				} else {
					rbc.pool.proxy.log.Warn().Uint64("queryId", query.id).Err(err).Msg("Unable to reply to client")
				}
			}
		}
	}()

	return rbc, nil
}

// Query sends a query to the backend.
func (rbc *RedisBackendConnection) Query(q RedisQuery) (retError error) {
	select {
	case rbc.inputChan <- q:
		return nil
	case <-rbc.inputChanStop:
		return fmt.Errorf("backend input channel is closed")
	}
}

// IsFull returns true if the connection's input channel is full.
func (rbc *RedisBackendConnection) IsFull() bool {
	return len(rbc.inputChan) >= cap(rbc.inputChan)
}

// AbortInflightQueries aborts all queries that are currently waiting for a response from the backend
// or are still in the input queue.
// It returns the number of queries that were aborted.
func (rbc *RedisBackendConnection) AbortInflightQueries() int {
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Aborting in-flight requests")
	count := 0
	// 1. Abort queries in the input queue
	for {
		select {
		case query := <-rbc.inputChan:
			query.Abort()
			ReleaseBuffer(query.item)
			count++
		default:
			goto inFlight
		}
	}

inFlight:
	// 2. Abort queries waiting for response
	for {
		select {
		case query := <-rbc.inFlight:
			query.Abort()
			ReleaseBuffer(query.item)
			count++
		default:
			return count
		}
	}
}
