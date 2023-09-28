package proxy

import (
	"bufio"
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
	pool            *RedisBackendConnectionPool
	backend         *backend.Backend
	conn            net.Conn
	input_chan_stop chan struct{}
	input_chan      chan RedisQuery
	in_flight       chan RedisQuery
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewRedisBackendConnection(pool *RedisBackendConnectionPool, backend *backend.Backend) (rbc *RedisBackendConnection, e error) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			e = misc.EnsureError(r)
		}
	}()

	rbc = &RedisBackendConnection{
		pool:            pool,
		backend:         backend,
		input_chan:      make(chan RedisQuery),
		input_chan_stop: make(chan struct{}),
		in_flight:       make(chan RedisQuery, pool.proxy.backendInflightQueueSize),
	}

	rbc.ctx, rbc.cancel = context.WithCancel(context.Background())

	// Prometheus
	metrics.BeCnxProcessed.WithLabelValues(backend.Address, rbc.pool.proxy.id).Inc()
	metrics.BeActCnx.WithLabelValues(backend.Address, rbc.pool.proxy.id).Inc()

	// Open backend connection
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Opening Backend connection")
	conn_back, err := net.DialTimeout("tcp", rbc.backend.Address, rbc.pool.proxy.connect_timeout)
	misc.PanicIfErr(err)

	rbc.conn = conn_back

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
		close(rbc.input_chan_stop)

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
			case query := <-rbc.input_chan:
				rbc.in_flight <- query
				_, err := rbc.conn.Write(query.item)
				if err == io.EOF || errors.Is(err, net.ErrClosed) {
					rbc.cancel()
					rbc.AbortInflightQueries() // Extra call to AbortInflightQueries in case the query we were processing has not been aborted by the "cleanup" goroutine
					return
				}
				misc.PanicIfErr(err)
			case <-rbc.ctx.Done():
				return
			}
		}
	}()

	// Read backend responses and send them to the client
	go func() {
		reader := bufio.NewReaderSize(rbc.conn, rbc.pool.proxy.buffer_size)

		for {
			item, err := redisReadItem(reader, false)
			if err == io.EOF || errors.Is(err, net.ErrClosed) || item == nil {
				rbc.cancel()
				return
			}
			misc.PanicIfErr(err)
			query := <-rbc.in_flight

			query.Reply(item)
		}
	}()

	return rbc, nil
}

func (rbc *RedisBackendConnection) Query(q RedisQuery) (ret_e error) {
	select {
	case rbc.input_chan <- q:
		return nil
	case <-rbc.input_chan_stop:
		return fmt.Errorf("backend input channel is closed")
	}
}

func (rbc *RedisBackendConnection) AbortInflightQueries() {
	rbc.pool.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Aborting in-flight requests")
	for {
		select {
		case query := <-rbc.in_flight:
			query.Abort()
		default:
			return
		}
	}
}
