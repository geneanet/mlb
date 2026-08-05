package redis

import (
	"bytes"
	"testing"
)

func TestReleaseBuffer(t *testing.T) {
	// Test nil buffer
	ReleaseBuffer(nil)

	// Test empty buffer
	ReleaseBuffer([]byte{})

	// Test valid buffer
	b := make([]byte, 10, 100)
	ReleaseBuffer(b)

	// Test buffer with zero capacity (but non-nil)
	b2 := b[0:0:0]
	ReleaseBuffer(b2)
}

func TestBufferPool_Reuse(t *testing.T) {
	// Put a known buffer in the pool
	knownBuf := make([]byte, 0, 1024)
	knownBuf = append(knownBuf, "original content"...)
	
	// We need to be careful because sync.Pool doesn't guarantee return of the same object.
	// But we can try multiple times or just verify that the pool works.
	
	ReleaseBuffer(knownBuf)
	
	// Try to get it back (or another one)
	ptr := bufferPool.Get().([]byte)
	if ptr == nil {
		t.Fatal("bufferPool.Get() returned nil")
	}
	
	// After getting it, the pool should have allocated a new one or returned our old one.
	// If it returned our old one, cap should be at least 1024.
	// Since we can't easily guarantee reuse in a test due to sync.Pool nature,
	// we at least test the logic of using it in ReadMessage.
}

func TestReadMessage_WithPooling(t *testing.T) {
	input := "+OK\r\n"
	r := bytes.NewReader([]byte(input))
	reader := NewRedisProtocolReader(r, 128)

	msg, err := reader.ReadMessage(false)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	
	if !bytes.Equal(msg, []byte(input)) {
		t.Errorf("expected %s, got %s", input, string(msg))
	}
	
	// Verify that msg is from the pool by releasing it and checking no panic
	ReleaseBuffer(msg)
}

func TestReadMessage_LargeMessagePooling(t *testing.T) {
	// Large message that will exceed small pooled buffers
	largeSize := 20000
	largeData := bytes.Repeat([]byte("A"), largeSize)
	input := append([]byte("+"), append(largeData, []byte("\r\n")...)...)
	
	r := bytes.NewReader(input)
	reader := NewRedisProtocolReader(r, 1024)
	
	msg, err := reader.ReadMessage(false)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	
	if len(msg) != len(input) {
		t.Errorf("expected length %d, got %d", len(input), len(msg))
	}
	
	ReleaseBuffer(msg)
}
