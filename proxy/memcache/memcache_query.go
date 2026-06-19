package memcache

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

var MemcacheQueryCounter atomic.Uint64

// MemcacheQuery represents a single Memcache command query being processed.
// It carries the command payload and provides mechanisms to send the response
// back to the client connection.
type MemcacheQuery struct {
	id               uint64        // Unique query ID for tracking
	item             []byte        // The raw command payload
	buffer           *bytes.Buffer // Optional pooled buffer to be released
	responseChan     chan MemcacheResponse
	responseChanStop chan struct{}
}

// NewMemcacheQuery creates a new MemcacheQuery with a unique ID.
// The responseChan is used to send the MemcacheResponse back to the client handler.
func NewMemcacheQuery(item []byte, responseChan chan MemcacheResponse, responseChanStop chan struct{}) MemcacheQuery {
	return MemcacheQuery{
		id:               MemcacheQueryCounter.Add(1),
		item:             item,
		responseChan:     responseChan,
		responseChanStop: responseChanStop,
	}
}

// Release releases resources associated with the query, specifically returning
// the pooled buffer if one was used.
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

// ReplyWithBuffer sends the backend response back to the client and provides
// a pooled buffer to be released after the response is sent.
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
// This is typically called when a backend connection is lost.
func (q MemcacheQuery) Abort() error {
	return q.Reply(nil)
}

// MemcacheResponse represents a response from a Memcache backend for a specific query.
type MemcacheResponse struct {
	query  MemcacheQuery // The original query this response is for
	item   []byte        // The raw response payload
	buffer *bytes.Buffer // Optional pooled buffer to be released
}

// Release releases resources associated with the response.
func (r *MemcacheResponse) Release() {
	if r.buffer != nil {
		bufferPool.Put(r.buffer)
		r.buffer = nil
	}
}
