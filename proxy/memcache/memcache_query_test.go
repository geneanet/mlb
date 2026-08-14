package memcache

import (
	"testing"
)

func TestMemcacheQuery_Reply(t *testing.T) {
	respChan := make(chan MemcacheResponse, 1)
	stopChan := make(chan struct{})

	q := NewMemcacheQuery([]byte("get key"), respChan, stopChan)
	err := q.Reply([]byte("VALUE key 0 5\r\nvalue\r\nEND\r\n"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	resp := <-respChan
	if string(resp.item) != "VALUE key 0 5\r\nvalue\r\nEND\r\n" {
		t.Fatalf("Unexpected response item: %s", string(resp.item))
	}

	close(stopChan)
	// Fill the buffer so sending blocks and it must pick the stopChan
	respChan <- MemcacheResponse{}
	err = q.Reply([]byte("test"))
	if err == nil || err.Error() != "response channel is closed" {
		t.Fatalf("Expected error response channel is closed, got: %v", err)
	}
}

func TestMemcacheQuery_Abort(t *testing.T) {
	respChan := make(chan MemcacheResponse, 1)
	stopChan := make(chan struct{})

	q := NewMemcacheQuery([]byte("get key"), respChan, stopChan)
	err := q.Abort()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	resp := <-respChan
	expectedErr := "SERVER_ERROR backend failure\r\n"
	if string(resp.item) != expectedErr {
		t.Fatalf("Expected protocol error item %q, got: %q", expectedErr, string(resp.item))
	}
}
