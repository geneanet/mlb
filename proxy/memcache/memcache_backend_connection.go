package memcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"net"
	"sync"
)

// MemcacheBackendConnection represents a single persistent connection to a Memcache backend.
// It implements a multiplexed protocol where multiple queries can be sent to the backend
// concurrently without waiting for each response. Responses are matched back to queries
// using a FIFO queue (inFlight), which is compatible with the Memcache ASCII protocol.
type MemcacheBackendConnection struct {
	pool          *MemcacheBackendConnectionPool
	backend       *backend.Backend
	conn          net.Conn
	inputChanStop chan struct{}      // Signal to stop the input loop
	inputChan     chan MemcacheQuery // Buffer for incoming queries from the proxy
	inFlight      chan MemcacheQuery // Queue of queries waiting for backend response
	ctx           context.Context
	cancel        context.CancelFunc
	metrics       *Metrics
	failureErr    error
	failureOnce   sync.Once
}

func (mbc *MemcacheBackendConnection) fail(err error) {
	mbc.failureOnce.Do(func() {
		mbc.failureErr = err
		mbc.cancel()
	})
}

// NewMemcacheBackendConnection creates a new MemcacheBackendConnection and starts its
// background goroutines for reading and writing. It returns an error if the initial
// connection to the backend fails.
func NewMemcacheBackendConnection(pool *MemcacheBackendConnectionPool, backend *backend.Backend) (*MemcacheBackendConnection, error) {
	mbc := &MemcacheBackendConnection{
		pool:          pool,
		backend:       backend,
		inputChan:     make(chan MemcacheQuery, pool.proxy.backendInputQueueSize),
		inputChanStop: make(chan struct{}),
		inFlight:      make(chan MemcacheQuery, pool.proxy.backendInflightQueueSize),
		metrics:       pool.proxy.getBackendMetrics(backend.Address),
	}

	mbc.ctx, mbc.cancel = context.WithCancel(context.Background())

	// Prometheus
	mbc.metrics.processed.Inc()

	mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Opening Backend connection")
	dialer := &net.Dialer{
		Timeout:   mbc.pool.proxy.connectTimeout,
		KeepAlive: mbc.pool.proxy.backendTCPKeepAlive,
	}
	connBack, err := dialer.DialContext(mbc.ctx, "tcp", mbc.backend.Address)
	if err != nil {
		return nil, err
	}

	// Prometheus
	mbc.metrics.active.Inc()

	mbc.conn = connBack

	context.AfterFunc(mbc.ctx, func() {
		mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Closing Backend connection")
		mbc.conn.Close()
		close(mbc.inputChanStop)

		// Abort all in flight requests
		abortedCount := mbc.AbortInflightQueries()

		// Notify the pool
		mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Notifying pool")
		err := mbc.failureErr
		if err == nil {
			err = mbc.ctx.Err()
		}
		mbc.pool.NotifyFailure(mbc, err, abortedCount > 0)

		// Prometheus
		mbc.metrics.active.Dec()
	})

	// Read queries and send them to the backend
	go func() {
		batch := make([]MemcacheQuery, 0, 32)
		writer := NewMemcacheProtocolWriter(mbc.conn, mbc.pool.proxy.bufferSize)
		defer writer.Release()
		for {
			select {
			case query := <-mbc.inputChan:
				batch = append(batch, query)

				// Try to gather more queries if available without blocking
			gather:
				for len(batch) < cap(batch) {
					select {
					case q := <-mbc.inputChan:
						batch = append(batch, q)
					default:
						break gather
					}
				}

				for _, q := range batch {
					select {
					case mbc.inFlight <- q:
					case <-mbc.ctx.Done():
						q.Abort()
						q.Release()
						continue
					}
					n, err := writer.Write(q.item)
					q.Release() // ponytail: release pooled query buffer if any
					if err != nil {
						if err != io.EOF && !errors.Is(err, net.ErrClosed) {
							mbc.pool.proxy.log.Error().Str("peer", mbc.backend.Address).Err(err).Msg("Unexpected error while sending query to the backend")
						}
						mbc.fail(err)
						mbc.AbortInflightQueries()
						return
					}
					mbc.metrics.requests.Inc()
					mbc.metrics.bytesOut.Add(float64(n))
				}

				if err := writer.Flush(); err != nil {
					mbc.fail(err)
					mbc.AbortInflightQueries()
					return
				}

				batch = batch[:0]
			case <-mbc.ctx.Done():
				return
			}
		}
	}()

	// Read backend responses and send them to the client
	go func() {
		reader := NewMemcacheProtocolReader(mbc.conn, mbc.pool.proxy.bufferSize)
		defer reader.Release()

		for {
			respBuffer := bufferPool.Get().(*bytes.Buffer)
			respBuffer.Reset()
			err := mbc.pool.proxy.readMemcacheResponseFull(reader, respBuffer)
			if err != nil {
				ReleaseBuffer(respBuffer)
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					mbc.pool.proxy.log.Error().Str("peer", mbc.backend.Address).Err(err).Msg("Unexpected error while reading from the backend")
				}
				mbc.fail(err)
				return
			}
			mbc.metrics.bytesIn.Add(float64(respBuffer.Len()))

			var query MemcacheQuery
			select {
			case query = <-mbc.inFlight:
			case <-mbc.ctx.Done():
				ReleaseBuffer(respBuffer)
				return
			}

			// ponytail: pass buffer ownership to avoid bytes.Clone
			err = query.ReplyWithBuffer(respBuffer.Bytes(), respBuffer)
			if err != nil {
				if err.Error() == "response channel is closed" {
					mbc.pool.proxy.log.Debug().Uint64("queryId", query.id).Msg("Unable to reply to client: response channel is closed")
				} else {
					mbc.pool.proxy.log.Warn().Uint64("queryId", query.id).Err(err).Msg("Unable to reply to client")
				}
			}
		}
	}()

	return mbc, nil
}

// Query sends a query to the backend.
func (mbc *MemcacheBackendConnection) Query(q MemcacheQuery) error {
	select {
	case mbc.inputChan <- q:
		return nil
	case <-mbc.inputChanStop:
		return fmt.Errorf("backend input channel is closed")
	}
}

// IsFull returns true if the connection's input channel is full.
func (mbc *MemcacheBackendConnection) IsFull() bool {
	return len(mbc.inputChan) >= cap(mbc.inputChan)
}

// AbortInflightQueries aborts all queries that are currently waiting for a response from the backend
// or are still in the input queue.
// It returns the number of queries that were aborted.
func (mbc *MemcacheBackendConnection) AbortInflightQueries() int {
	mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Aborting in-flight requests")
	count := 0
	// 1. Abort queries in the input queue
	for {
		select {
		case query := <-mbc.inputChan:
			query.Abort()
			query.Release()
			count++
		default:
			goto inFlight
		}
	}

inFlight:
	// 2. Abort queries waiting for response
	for {
		select {
		case query := <-mbc.inFlight:
			query.Abort()
			query.Release()
			count++
		default:
			return count
		}
	}
}

// readMemcacheResponseFull reads a complete memcache response into a buffer.
// It handles:
// 1. Simple one-line responses (STORED, END, HD, etc.)
// 2. Data blocks (VALUE <key> ... \r\n<data>\r\n)
// 3. Meta data blocks (VA <size> ... \r\n<data>\r\n)
// 4. Multi-line responses like STATS.
func (p *MemcacheProxy) readMemcacheResponseFull(r *MemcacheProtocolReader, w io.Writer) error {
	for {
		line, err := r.ReadLine()
		if err != nil {
			return err
		}

		w.Write(line)

		// End of retrieval command (ASCII protocol)
		if bytes.HasPrefix(line, []byte("END\r\n")) {
			return nil
		}

		// Data block (ASCII protocol): VALUE <key> <flags> <bytes> [<cas unique>]\r\n<data>\r\n
		if bytes.HasPrefix(line, []byte("VALUE ")) {
			fieldsPtr := p.getFields(line)
			fields := *fieldsPtr
			if len(fields) >= 4 {
				// size is fields[3] in ASCII protocol
				size := 0
				for _, b := range fields[3] {
					if b >= '0' && b <= '9' {
						size = size*10 + int(b-'0')
					}
				}
				buf, err := r.ReadFull(size + 2) // data + \r\n
				if err != nil {
					p.releaseFields(fieldsPtr)
					return err
				}
				w.Write(buf)
			}
			p.releaseFields(fieldsPtr)
		} else if bytes.HasPrefix(line, []byte("VA ")) {
			// Meta data block: VA <size> [flags]... \r\n<data>\r\n
			// ponytail: Meta Value (VA) uses fields[1] for size.
			fieldsPtr := p.getFields(line)
			fields := *fieldsPtr
			if len(fields) >= 2 {
				// size is fields[1] in Meta protocol
				size := 0
				for _, b := range fields[1] {
					if b >= '0' && b <= '9' {
						size = size*10 + int(b-'0')
					}
				}
				buf, err := r.ReadFull(size + 2) // data + \r\n
				if err != nil {
					p.releaseFields(fieldsPtr)
					return err
				}
				w.Write(buf)
			}
			p.releaseFields(fieldsPtr)
			// VA is a final response for a single mg command
			// ponytail: Meta protocol commands are usually single-line or single-payload,
			// unlike multi-get which requires END.
			return nil
		} else if bytes.HasPrefix(line, []byte("STORED")) || bytes.HasPrefix(line, []byte("NOT_STORED")) || bytes.HasPrefix(line, []byte("EXISTS")) || bytes.HasPrefix(line, []byte("NOT_FOUND")) || bytes.HasPrefix(line, []byte("DELETED")) || bytes.HasPrefix(line, []byte("ERROR")) || bytes.HasPrefix(line, []byte("CLIENT_ERROR")) || bytes.HasPrefix(line, []byte("SERVER_ERROR")) || bytes.HasPrefix(line, []byte("OK")) || bytes.HasPrefix(line, []byte("HD")) || bytes.HasPrefix(line, []byte("NF")) || bytes.HasPrefix(line, []byte("EX")) || bytes.HasPrefix(line, []byte("NS")) || bytes.HasPrefix(line, []byte("EN")) || bytes.HasPrefix(line, []byte("VERSION ")) {
			// One-line responses (standard and meta) indicate completion of a command.
			return nil
		} else if bytes.HasPrefix(line, []byte("STAT ")) {
			// Multi-line responses (STAT) - keep reading until END
			continue
		} else {
			// Catch-all for unknown or unexpected responses
			return nil
		}
	}
}
