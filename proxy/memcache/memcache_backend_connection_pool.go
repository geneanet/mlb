package memcache

import (
	"context"
	"mlb/misc"
	"sync"
	"time"
)

// MemcacheBackendConnectionPool manages a pool of multiplexed connections to Memcache backends.
// It ensures that a minimum number of connections are maintained for each active backend.
type MemcacheBackendConnectionPool struct {
	pools       map[string][]*MemcacheBackendConnection
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
		pools:   make(map[string][]*MemcacheBackendConnection),
		indices: make(map[string]uint64),
		proxy:   proxy,
	}
	mbcp.ctx, mbcp.cancel = context.WithCancel(proxy.ctx)
	return mbcp
}

// Get returns an available connection from the pool for the given address using round-robin.
func (mbcp *MemcacheBackendConnectionPool) Get(address string) *MemcacheBackendConnection {
	mbcp.mutex.Lock()
	defer mbcp.mutex.Unlock()

	pool := mbcp.pools[address]
	if len(pool) == 0 {
		return nil
	}

	idx := mbcp.indices[address] % uint64(len(pool))
	mbc := pool[idx]
	mbcp.indices[address] = idx + 1
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
	}

	// Remove pools for backends that are no longer active
	for addr, pool := range mbcp.pools {
		if !validAddresses[addr] {
			for _, conn := range pool {
				conn.cancel()
			}
			delete(mbcp.pools, addr)
			delete(mbcp.indices, addr)
		}
	}
	mbcp.mutex.Unlock()

	backoff := misc.NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	// Ensure each backend has the required number of connections
	// TODO: Ensure one faulty backend does not prevent the others to reach the wanted number of connections (give up after a few tries ?)
	for _, backend := range backends {
		for {
			mbcp.mutex.Lock()
			poolLen := len(mbcp.pools[backend.Address])
			mbcp.mutex.Unlock()

			if poolLen >= mbcp.proxy.backendConnectionPoolSize {
				break
			}

			mbc, err := NewMemcacheBackendConnection(mbcp, backend)
			if err != nil {
				mbcp.proxy.log.Warn().Err(err).Str("peer", backend.Address).Msg("Unable to connect to backend")
				backoff.Sleep(mbcp.ctx)
			} else {
				mbcp.mutex.Lock()
				mbcp.pools[backend.Address] = append(mbcp.pools[backend.Address], mbc)
				mbcp.mutex.Unlock()
				backoff.Reset()
			}

			select {
			case <-mbcp.ctx.Done():
				return
			default:
			}
		}
	}
}
