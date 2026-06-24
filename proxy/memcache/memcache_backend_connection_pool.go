package memcache

import (
	"context"
	"mlb/backend"
	"mlb/misc"
	"sync"
	"time"
)

// MemcacheBackendConnectionPool manages a pool of multiplexed connections to Memcache backends.
// It ensures that a minimum number of connections are maintained for each active backend.
type MemcacheBackendConnectionPool struct {
	pools       map[string][]*MemcacheBackendConnection
	backends    map[string]*backend.Backend
	indices     map[string]uint64
	mutex       sync.RWMutex
	updateMutex sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	proxy       *MemcacheProxy
}

// NewMemcacheBackendConnectionPool creates a new MemcacheBackendConnectionPool.
func NewMemcacheBackendConnectionPool(proxy *MemcacheProxy) *MemcacheBackendConnectionPool {
	mbcp := &MemcacheBackendConnectionPool{
		pools:    make(map[string][]*MemcacheBackendConnection),
		backends: make(map[string]*backend.Backend),
		indices:  make(map[string]uint64),
		proxy:    proxy,
	}
	mbcp.ctx, mbcp.cancel = context.WithCancel(proxy.ctx)
	return mbcp
}

// Get returns an available connection from the pool for the given address using round-robin.
func (mbcp *MemcacheBackendConnectionPool) Get(address string) *MemcacheBackendConnection {
	mbcp.mutex.Lock()

	pool := mbcp.pools[address]
	if len(pool) == 0 {
		mbcp.mutex.Unlock()
		return nil
	}

	// 1. Try to find a non-full connection using round-robin
	startIdx := mbcp.indices[address] % uint64(len(pool))
	for i := 0; i < len(pool); i++ {
		idx := (startIdx + uint64(i)) % uint64(len(pool))
		mbc := pool[idx]
		if !mbc.IsFull() {
			mbcp.indices[address] = idx + 1
			mbcp.mutex.Unlock()
			return mbc
		}
	}

	// 2. All connections are full. Try to grow if below max.
	if len(pool) < mbcp.proxy.backendMaxConnections {
		backend := mbcp.backends[address]
		mbcp.mutex.Unlock()

		// ponytail: growth is synchronous to ensure the current request can benefit from the new connection.
		mbc, err := NewMemcacheBackendConnection(mbcp, backend)
		if err != nil {
			// Failed to open a new one, fallback to round-robin on existing (even if full)
			mbcp.mutex.Lock()
			pool = mbcp.pools[address]
			if len(pool) == 0 {
				mbcp.mutex.Unlock()
				return nil
			}
			idx := mbcp.indices[address] % uint64(len(pool))
			mbc = pool[idx]
			mbcp.indices[address] = idx + 1
			mbcp.mutex.Unlock()
			return mbc
		}

		mbcp.mutex.Lock()
		mbcp.pools[address] = append(mbcp.pools[address], mbc)
		mbcp.indices[address] = uint64(len(mbcp.pools[address])) // point to next after new one
		mbcp.mutex.Unlock()
		return mbc
	}

	// 3. Already at max connections and all are full. Fallback to round-robin.
	idx := mbcp.indices[address] % uint64(len(pool))
	mbc := pool[idx]
	mbcp.indices[address] = idx + 1
	mbcp.mutex.Unlock()
	return mbc
}

// Del removes a connection from the pool.
func (mbcp *MemcacheBackendConnectionPool) Del(mbc *MemcacheBackendConnection) {
	mbcp.mutex.Lock()
	defer mbcp.mutex.Unlock()
	addr := mbc.backend.Address
	pool := mbcp.pools[addr]
	for i, c := range pool {
		if c == mbc {
			mbcp.pools[addr] = append(pool[:i], pool[i+1:]...)
			break
		}
	}
}

// NotifyFailure is called by a connection when it encounters a fatal error.
func (mbcp *MemcacheBackendConnectionPool) NotifyFailure(mbc *MemcacheBackendConnection) {
	mbcp.proxy.log.Error().Str("peer", mbc.backend.Address).Msg("Backend connection failed")
	mbcp.Del(mbc)
	go mbcp.Update()
}

// Update reconciles the current connection pools with the latest backend list.
// It closes connections to removed backends and opens new ones to reach the desired pool size.
func (mbcp *MemcacheBackendConnectionPool) Update() {
	mbcp.updateMutex.Lock()
	defer mbcp.updateMutex.Unlock()

	mbcp.mutex.Lock()
	backends := mbcp.proxy.backends.GetList()

	validAddresses := make(map[string]bool)
	for _, b := range backends {
		validAddresses[b.Address] = true
		mbcp.backends[b.Address] = b
	}

	// Remove pools for backends that are no longer active
	for addr, pool := range mbcp.pools {
		if !validAddresses[addr] {
			for _, conn := range pool {
				conn.cancel()
			}
			delete(mbcp.pools, addr)
			delete(mbcp.backends, addr)
			delete(mbcp.indices, addr)
		}
	}
	mbcp.mutex.Unlock()

	// Ensure each backend has the required number of connections
	// ponytail: backends are processed in parallel to ensure one faulty backend doesn't block others.
	// We give up after 3 consecutive failures per backend to avoid blocking the update process indefinitely.
	var wg sync.WaitGroup
	for _, be := range backends {
		wg.Add(1)
		go func(b *backend.Backend) {
			defer wg.Done()
			backoff := misc.NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)
			tries := 0
			for tries < 3 {
				mbcp.mutex.Lock()
				poolLen := len(mbcp.pools[b.Address])
				mbcp.mutex.Unlock()

				if poolLen >= mbcp.proxy.backendMinConnections {
					break
				}

				mbc, err := NewMemcacheBackendConnection(mbcp, b)
				if err != nil {
					mbcp.proxy.log.Warn().Err(err).Str("peer", b.Address).Msg("Unable to connect to backend")
					tries++
					if tries < 3 {
						backoff.Sleep(mbcp.ctx)
					}
				} else {
					mbcp.mutex.Lock()
					mbcp.pools[b.Address] = append(mbcp.pools[b.Address], mbc)
					mbcp.mutex.Unlock()
					backoff.Reset()
					tries = 0
				}

				select {
				case <-mbcp.ctx.Done():
					return
				default:
				}
			}
		}(be)
	}
	wg.Wait()
}
