package memcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/metrics"
	"net"
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
}

// NewMemcacheBackendConnection creates a new MemcacheBackendConnection and starts its
// background goroutines for reading and writing. It returns an error if the initial
// connection to the backend fails.
func NewMemcacheBackendConnection(pool *MemcacheBackendConnectionPool, backend *backend.Backend) (*MemcacheBackendConnection, error) {
	queueSize := pool.proxy.backendInflightQueueSize
	if queueSize == 0 { // TODO: that should not be useful except for the tests ?
		queueSize = 512
	}

	mbc := &MemcacheBackendConnection{
		pool:          pool,
		backend:       backend,
		inputChan:     make(chan MemcacheQuery, pool.proxy.backendInputQueueSize),
		inputChanStop: make(chan struct{}),
		inFlight:      make(chan MemcacheQuery, queueSize),
	}

	mbc.ctx, mbc.cancel = context.WithCancel(context.Background())

	// Prometheus
	metrics.BeCnxProcessed.WithLabelValues(backend.Address, mbc.pool.proxy.id).Inc()
	metrics.BeActCnx.WithLabelValues(backend.Address, mbc.pool.proxy.id).Inc()

	mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Opening Backend connection")
	connBack, err := net.DialTimeout("tcp", mbc.backend.Address, mbc.pool.proxy.connectTimeout)
	if err != nil {
		return nil, err
	}

	mbc.conn = connBack

	context.AfterFunc(mbc.ctx, func() {
		mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Closing Backend connection")
		mbc.conn.Close()
		close(mbc.inputChanStop)
		mbc.AbortInflightQueries()
		mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Notifying pool")
		mbc.pool.NotifyFailure(mbc)
		metrics.BeActCnx.WithLabelValues(mbc.backend.Address, mbc.pool.proxy.id).Dec()
	})

	// Read queries and send them to the backend
	go func() {
		for {
			select {
			case query := <-mbc.inputChan:
				mbc.inFlight <- query
				_, err := mbc.conn.Write(query.item)
				query.Release() // ponytail: release pooled query buffer if any
				if err != nil {
					if err != io.EOF && !errors.Is(err, net.ErrClosed) {
						mbc.pool.proxy.log.Error().Str("peer", mbc.backend.Address).Err(err).Msg("Unexpected error while sending query to the backend")
					}
					mbc.cancel()
					mbc.AbortInflightQueries()
					return
				}
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
				bufferPool.Put(respBuffer)
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					mbc.pool.proxy.log.Error().Str("peer", mbc.backend.Address).Err(err).Msg("Unexpected error while reading from the backend")
				}
				mbc.cancel()
				return
			}

			var query MemcacheQuery
			select {
			case query = <-mbc.inFlight:
			case <-mbc.ctx.Done():
				bufferPool.Put(respBuffer)
				return
			}

			// ponytail: pass buffer ownership to avoid bytes.Clone
			err = query.ReplyWithBuffer(respBuffer.Bytes(), respBuffer)
			if err != nil {
				mbc.pool.proxy.log.Warn().Uint64("queryId", query.id).Err(err).Msg("Unable to reply to client")
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

// AbortInflightQueries aborts all queries that are currently waiting for a response from the backend.
func (mbc *MemcacheBackendConnection) AbortInflightQueries() {
	mbc.pool.proxy.log.Debug().Str("peer", mbc.backend.Address).Msg("Aborting in-flight requests")
	for {
		select {
		case query := <-mbc.inFlight:
			query.Abort()
		default:
			return
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
		} else if bytes.HasPrefix(line, []byte("STORED")) || bytes.HasPrefix(line, []byte("NOT_STORED")) || bytes.HasPrefix(line, []byte("EXISTS")) || bytes.HasPrefix(line, []byte("NOT_FOUND")) || bytes.HasPrefix(line, []byte("DELETED")) || bytes.HasPrefix(line, []byte("ERROR")) || bytes.HasPrefix(line, []byte("CLIENT_ERROR")) || bytes.HasPrefix(line, []byte("SERVER_ERROR")) || bytes.HasPrefix(line, []byte("OK")) || bytes.HasPrefix(line, []byte("HD")) || bytes.HasPrefix(line, []byte("NF")) || bytes.HasPrefix(line, []byte("EX")) || bytes.HasPrefix(line, []byte("NS")) || bytes.HasPrefix(line, []byte("EN")) {
			// One-line responses (standard and meta) indicate completion of a command.
			return nil
		} else if bytes.HasPrefix(line, []byte("STAT ")) || bytes.HasPrefix(line, []byte("VERSION ")) {
			// Multi-line responses (STAT) or single line (VERSION) - keep reading until END or next relevant prefix
			continue
		} else {
			// Catch-all for unknown or unexpected responses
			return nil
		}
	}
}
