package memcache

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

var MemcacheQueryCounter atomic.Uint64

// MemcacheQuery represents a single Memcache command query.
type MemcacheQuery struct {
	id               uint64
	item             []byte
	buffer           *bytes.Buffer // ponytail: optional pooled buffer to be released
	responseChan     chan MemcacheResponse
	responseChanStop chan struct{}
}

// NewMemcacheQuery creates a new MemcacheQuery with a unique ID.
func NewMemcacheQuery(item []byte, responseChan chan MemcacheResponse, responseChanStop chan struct{}) MemcacheQuery {
	return MemcacheQuery{
		id:               MemcacheQueryCounter.Add(1),
		item:             item,
		responseChan:     responseChan,
		responseChanStop: responseChanStop,
	}
}

// Release releases resources associated with the query.
func (q *MemcacheQuery) Release() {
	if q.buffer != nil {
		bufferPool.Put(q.buffer)
		q.buffer = nil
	}
}

// Reply sends the backend response back to the client.
func (q MemcacheQuery) Reply(item []byte) error {
	return q.ReplyWithBuffer(item, nil)
}

// ReplyWithBuffer sends the backend response back to the client with a pooled buffer.
func (q MemcacheQuery) ReplyWithBuffer(item []byte, buffer *bytes.Buffer) error {
	select {
	case q.responseChan <- MemcacheResponse{
		query:  q,
		item:   item,
		buffer: buffer,
	}:
		return nil
	case <-q.responseChanStop:
		if buffer != nil {
			bufferPool.Put(buffer)
		}
		return fmt.Errorf("response channel is closed")
	}
}

// Abort sends a nil response to the client, effectively aborting the query.
func (q MemcacheQuery) Abort() error {
	return q.Reply(nil)
}

// MemcacheResponse represents a response from a Memcache backend for a specific query.
type MemcacheResponse struct {
	query  MemcacheQuery
	item   []byte
	buffer *bytes.Buffer // ponytail: optional pooled buffer to be released
}

// Release releases resources associated with the response.
func (r *MemcacheResponse) Release() {
	if r.buffer != nil {
		bufferPool.Put(r.buffer)
		r.buffer = nil
	}
}
