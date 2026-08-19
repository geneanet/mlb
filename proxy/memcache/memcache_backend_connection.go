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
	inFlightMu    sync.Mutex         // Protects currentQuery
	currentQuery  *MemcacheQuery     // Query currently being read from the backend
	ctx           context.Context
	cancel        context.CancelFunc
	metrics       *Metrics
	failureErr    error
	failureMu     sync.RWMutex
	failureOnce   sync.Once
}

func (mbc *MemcacheBackendConnection) getFailureErr() error {
	mbc.failureMu.RLock()
	defer mbc.failureMu.RUnlock()
	return mbc.failureErr
}

func (mbc *MemcacheBackendConnection) fail(err error) {
	mbc.failureOnce.Do(func() {
		mbc.failureMu.Lock()
		mbc.failureErr = err
		mbc.failureMu.Unlock()
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
		_ = mbc.conn.Close()
		close(mbc.inputChanStop)

		// Abort all in flight requests
		abortedCount := mbc.AbortInflightQueries()

		// Notify the pool
		mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Notifying pool")
		err := mbc.getFailureErr()
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
					// ponytail: prevent double-free by passing a copy with nil buffer to inFlight.
					// The write loop owns and releases the buffer after writing.
					inFlightQuery := q
					inFlightQuery.buffer = nil
					select {
					case mbc.inFlight <- inFlightQuery:
					case <-mbc.ctx.Done():
						_ = q.Abort()
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

			var q MemcacheQuery
			select {
			case q = <-mbc.inFlight:
			case <-mbc.ctx.Done():
				ReleaseBuffer(respBuffer)
				return
			}

			mbc.inFlightMu.Lock()
			mbc.currentQuery = &q
			mbc.inFlightMu.Unlock()

			err := mbc.pool.proxy.readMemcacheResponseFull(reader, respBuffer, q)
			if err != nil {
				ReleaseBuffer(respBuffer)
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					mbc.pool.proxy.log.Error().Str("peer", mbc.backend.Address).Err(err).Msg("Unexpected error while reading from the backend")
				}

				// ponytail: ensure the query we were processing is aborted before we exit.
				// AbortInflightQueries will handle others, but this one is in our hands.
				_ = q.Abort()
				q.Release()

				mbc.inFlightMu.Lock()
				mbc.currentQuery = nil
				mbc.inFlightMu.Unlock()

				mbc.fail(err)
				return
			}
			mbc.metrics.bytesIn.Add(float64(respBuffer.Len()))

			// ponytail: pass buffer ownership to avoid bytes.Clone
			err = q.ReplyWithBuffer(respBuffer.Bytes(), respBuffer)
			if err != nil {
				if err.Error() == "response channel is closed" {
					mbc.pool.proxy.log.Debug().Uint64("queryId", q.id).Msg("Unable to reply to client: response channel is closed")
				} else {
					mbc.pool.proxy.log.Warn().Uint64("queryId", q.id).Err(err).Msg("Unable to reply to client")
				}
			}

			mbc.inFlightMu.Lock()
			mbc.currentQuery = nil
			mbc.inFlightMu.Unlock()
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
			_ = query.Abort()
			query.Release()
			count++
		default:
			goto current
		}
	}

current:
	// 2. Abort queries waiting for response or currently being read
	mbc.inFlightMu.Lock()
	if mbc.currentQuery != nil {
		_ = mbc.currentQuery.Abort()
		mbc.currentQuery.Release()
		count++
	}
	mbc.inFlightMu.Unlock()

	for {
		select {
		case query := <-mbc.inFlight:
			_ = query.Abort()
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
// 4. Multi-line responses like STATS, draining until END.
func (p *MemcacheProxy) readMemcacheResponseFull(r *MemcacheProtocolReader, w io.Writer, q MemcacheQuery) error {
	for {
		line, err := r.ReadLine()
		if err != nil {
			return err
		}

		if _, err := w.Write(line); err != nil {
			return err
		}

		// End of retrieval command (ASCII protocol)
		if bytes.HasPrefix(line, []byte("END\r\n")) {
			return nil
		}

		// Generic Multi-line handling (e.g. stats)
		// Drain everything until END\r\n (above) or an error response.
		if q.MultiLine {
			if bytes.HasPrefix(line, []byte("ERROR")) || bytes.HasPrefix(line, []byte("CLIENT_ERROR")) || bytes.HasPrefix(line, []byte("SERVER_ERROR")) {
				return nil
			}
			continue
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
				if _, err := w.Write(buf); err != nil {
					p.releaseFields(fieldsPtr)
					return err
				}
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
				if _, err := w.Write(buf); err != nil {
					p.releaseFields(fieldsPtr)
					return err
				}
			}
			p.releaseFields(fieldsPtr)
			// VA is a final response for a single mg command
			// ponytail: Meta protocol commands are usually single-line or single-payload,
			// unlike multi-get which requires END.
			return nil
		} else if bytes.HasPrefix(line, []byte("STORED")) || bytes.HasPrefix(line, []byte("NOT_STORED")) || bytes.HasPrefix(line, []byte("EXISTS")) || bytes.HasPrefix(line, []byte("NOT_FOUND")) || bytes.HasPrefix(line, []byte("DELETED")) || bytes.HasPrefix(line, []byte("ERROR")) || bytes.HasPrefix(line, []byte("CLIENT_ERROR")) || bytes.HasPrefix(line, []byte("SERVER_ERROR")) || bytes.HasPrefix(line, []byte("OK")) || bytes.HasPrefix(line, []byte("HD")) || bytes.HasPrefix(line, []byte("NF")) || bytes.HasPrefix(line, []byte("EX")) || bytes.HasPrefix(line, []byte("NS")) || bytes.HasPrefix(line, []byte("EN")) || bytes.HasPrefix(line, []byte("VERSION ")) || bytes.HasPrefix(line, []byte("RESET")) {
			// One-line responses (standard and meta) indicate completion of a command.
			return nil
		} else {
			// Catch-all for unknown or unexpected responses.
			// In ASCII protocol, unknown lines in a stream are usually single lines.
			return nil
		}
	}
}
