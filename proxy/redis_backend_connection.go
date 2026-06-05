package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/metrics"
	"mlb/misc"
	"net"
)

//--------------------------
// Redis Backend Connection
//--------------------------

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

func NewRedisBackendConnection(pool *RedisBackendConnectionPool, backend *backend.Backend) (rbc *RedisBackendConnection, e error) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			e = misc.EnsureError(r)
			rbc = nil
		}
	}()

	rbc = &RedisBackendConnection{
		pool:          pool,
		backend:       backend,
		inputChan:     make(chan RedisQuery),
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
	misc.PanicIfErr(err)

	rbc.conn = connBack

	// Set TCPNoDelay
	err = rbc.conn.(*net.TCPConn).SetNoDelay(true)
	misc.PanicIfErr(err)

	// Cleanup routine: If the connection context is closed, ensure the connection is closed, abort all in flight request and notify the pool
	go func() {
		// Wait for the context to be done
		<-rbc.ctx.Done()

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
		metrics.BeActCnx.WithLabelValues(backend.Address, rbc.pool.proxy.id).Dec()
	}()

	// Read queries and send them to the backend
	go func() {
		for {
			select {
			case query := <-rbc.inputChan:
				rbc.inFlight <- query
				_, err := rbc.conn.Write(query.item)
				if err != nil {
					if err != io.EOF && !errors.Is(err, net.ErrClosed) {
						rbc.pool.proxy.log.Error().Str("peer", rbc.backend.Address).Err(err).Msg("Unexpected error while sending query to the backend")
					}
					rbc.cancel()
					rbc.AbortInflightQueries() // Extra call to AbortInflightQueries in case the query we were processing has not been aborted by the "cleanup" goroutine
					return
				}
			case <-rbc.ctx.Done():
				return
			}
		}
	}()

	// Read backend responses and send them to the client
	go func() {
		reader := NewRedisProtocolReader(rbc.conn, rbc.pool.proxy.bufferSize)

		for {
			item, err := reader.ReadMessage(false)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					rbc.pool.proxy.log.Error().Str("peer", rbc.backend.Address).Err(err).Msg("Unexpected error while reading from the backend")
				}
				rbc.cancel()
				return
			}
			query := <-rbc.inFlight

			query.Reply(item)
		}
	}()

	return rbc, nil
}

func (rbc *RedisBackendConnection) Query(q RedisQuery) (retError error) {
	select {
	case rbc.inputChan <- q:
		return nil
	case <-rbc.inputChanStop:
		return fmt.Errorf("backend input channel is closed")
	}
}

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
