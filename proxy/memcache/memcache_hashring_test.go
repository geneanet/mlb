package memcache

import (
	"fmt"
	"mlb/backend"
	"testing"
)

func TestMemcacheHashRing_Distribution(t *testing.T) {
	ring := newMemcacheHashRing()
	b1 := backend.NewBackend("127.0.0.1:11211", nil)
	b2 := backend.NewBackend("127.0.0.1:11212", nil)
	b3 := backend.NewBackend("127.0.0.1:11213", nil)

	ring.update([]*backend.Backend{b1, b2, b3})

	counts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		b := ring.getBackend(key)
		counts[b.Address]++
	}

	// Distribution should be somewhat balanced
	for addr, count := range counts {
		if count < 2000 {
			t.Errorf("Backend %s has too few keys: %d", addr, count)
		}
	}
}

func TestMemcacheHashRing_Empty(t *testing.T) {
	ring := newMemcacheHashRing()
	if b := ring.getBackend([]byte("any")); b != nil {
		t.Errorf("Expected nil backend for empty ring, got %v", b)
	}
}

func TestMemcacheHashRing_Stability(t *testing.T) {
	ring := newMemcacheHashRing()
	b1 := backend.NewBackend("127.0.0.1:11211", nil)
	b2 := backend.NewBackend("127.0.0.1:11212", nil)

	ring.update([]*backend.Backend{b1, b2})

	key := []byte("stable-key")
	firstBackend := ring.getBackend(key)

	// Update with same backends, should map to same backend
	ring.update([]*backend.Backend{b1, b2})
	if ring.getBackend(key) != firstBackend {
		t.Error("Backend changed after identity update")
	}

	// Add a backend, most keys should still map to the same backend
	b3 := backend.NewBackend("127.0.0.1:11213", nil)
	ring.update([]*backend.Backend{b1, b2, b3})

	changed := 0
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		ring.update([]*backend.Backend{b1, b2})
		bOld := ring.getBackend(k)
		ring.update([]*backend.Backend{b1, b2, b3})
		bNew := ring.getBackend(k)
		if bOld != bNew {
			changed++
		}
	}

	// For 2->3 backends, roughly 1/3 of keys should change
	if changed > 500 || changed < 100 {
		t.Errorf("Unexpected number of changed keys: %d/1000", changed)
	}
}
