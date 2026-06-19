package proxy

import (
	"fmt"
	"sync/atomic"
)

var MemcacheQueryCounter atomic.Uint64

// MemcacheQuery represents a single Memcache command query.
type MemcacheQuery struct {
	id               uint64
	item             []byte
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

// Reply sends the backend response back to the client.
func (q MemcacheQuery) Reply(item []byte) error {
	select {
	case q.responseChan <- MemcacheResponse{
		query: q,
		item:  item,
	}:
		return nil
	case <-q.responseChanStop:
		return fmt.Errorf("response channel is closed")
	}
}

// Abort sends a nil response to the client, effectively aborting the query.
func (q MemcacheQuery) Abort() error {
	return q.Reply(nil)
}

// MemcacheResponse represents a response from a Memcache backend for a specific query.
type MemcacheResponse struct {
	query MemcacheQuery
	item  []byte
}
