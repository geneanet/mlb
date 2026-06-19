package memcache

import (
	"crypto/md5"
	"fmt"
	"mlb/backend"
	"sort"
	"sync"
)

type memcacheRingNode struct {
	hash    uint32
	backend *backend.Backend
}

// memcacheHashRing implements Ketama consistent hashing.
// It maps keys to backends by hashing them and finding the closest node on the ring.
type memcacheHashRing struct {
	nodes []memcacheRingNode
	mu    sync.RWMutex
}

func newMemcacheHashRing() *memcacheHashRing {
	return &memcacheHashRing{}
}

// update rebuilds the hash ring with the given list of backends.
// It creates 160 virtual nodes (40 hashes * 4 uint32) per backend for better distribution.
func (r *memcacheHashRing) update(backends []*backend.Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes = nil
	for _, b := range backends {
		for i := 0; i < 40; i++ {
			h := md5.Sum([]byte(fmt.Sprintf("%s-%d", b.Address, i)))
			for j := 0; j < 4; j++ {
				val := uint32(h[3+j*4])<<24 | uint32(h[2+j*4])<<16 | uint32(h[1+j*4])<<8 | uint32(h[0+j*4])
				r.nodes = append(r.nodes, memcacheRingNode{hash: val, backend: b})
			}
		}
	}

	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i].hash < r.nodes[j].hash
	})
}

// getBackend returns the backend responsible for the given key.
func (r *memcacheHashRing) getBackend(key []byte) *backend.Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil
	}

	h := md5.Sum(key)
	val := uint32(h[3])<<24 | uint32(h[2])<<16 | uint32(h[1])<<8 | uint32(h[0])

	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].hash >= val
	})

	if idx == len(r.nodes) {
		idx = 0
	}
	return r.nodes[idx].backend
}
