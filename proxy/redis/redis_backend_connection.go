package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/metrics"
	"net"
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
	}

	rbc.ctx, rbc.cancel = context.WithCancel(context.Background())

	// Prometheus
	metrics.BeCnxProcessed.WithLabelValues(backend.Address, rbc.pool.proxy.id).Inc()
	metrics.BeActCnx.WithLabelValues(backend.Address, rbc.pool.proxy.id).Inc()

	// Open backend connection
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Opening Backend connection")
	connBack, err := net.DialTimeout("tcp", rbc.backend.Address, rbc.pool.proxy.connectTimeout)
	if err != nil {
		panic(err)
	}

	rbc.conn = connBack

	// Cleanup routine: If the connection context is closed, ensure the connection is closed, abort all in flight request and notify the pool
	context.AfterFunc(rbc.ctx, func() {
		// Ensure the connection is closed
		rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Closing Backend connection")
		rbc.conn.Close()

		// Ensure no new queries can be added to the input queue
		close(rbc.inputChanStop)

		// Abort all in flight requests
		rbc.AbortInflightQueries()

		// Notify the pool
		rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Notifying pool")
		rbc.pool.NotifyFailure(rbc)

		// Prometheus
		metrics.BeActCnx.WithLabelValues(rbc.backend.Address, rbc.pool.proxy.id).Dec()
	})

	// Read queries and send them to the backend
	go func() {
		for {
			select {
			case query := <-rbc.inputChan:
				rbc.inFlight <- query
				n, err := rbc.conn.Write(query.item)
				if err != nil {
					if err != io.EOF && !errors.Is(err, net.ErrClosed) {
						rbc.pool.proxy.log.Error().Str("peer", rbc.backend.Address).Err(err).Msg("Unexpected error while sending query to the backend")
					}
					rbc.cancel()
					rbc.AbortInflightQueries() // Extra call to AbortInflightQueries in case the query we were processing has not been aborted by the "cleanup" goroutine
					return
				}
				metrics.BeRequests.WithLabelValues(rbc.backend.Address, rbc.pool.proxy.id).Inc()
				metrics.BeBytesOut.WithLabelValues(rbc.backend.Address, rbc.pool.proxy.id).Add(float64(n))
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
				rbc.cancel()
				return
			}
			metrics.BeBytesIn.WithLabelValues(rbc.backend.Address, rbc.pool.proxy.id).Add(float64(len(item)))
			var query RedisQuery
			select {
			case query = <-rbc.inFlight:
			case <-rbc.ctx.Done():
				return
			}

			err = query.Reply(item)
			if err != nil {
				rbc.pool.proxy.log.Warn().Uint64("queryId", query.id).Err(err).Msg("Unable to reply to client")
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

// AbortInflightQueries aborts all queries that are currently waiting for a response from the backend.
func (rbc *RedisBackendConnection) AbortInflightQueries() {
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Aborting in-flight requests")
	for {
		select {
		case query := <-rbc.inFlight:
			query.Abort()
		default:
			return
		}
	}
}
