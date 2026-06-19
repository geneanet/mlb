package proxy

import (
	"context"
	"mlb/misc"
	"sync"
	"time"
)

// MemcacheBackendConnectionPool manages a pool of multiplexed connections to Memcache backends.
// It ensures that a minimum number of connections are maintained for each active backend.
type MemcacheBackendConnectionPool struct {
	pools               map[string]map[*MemcacheBackendConnection]struct{}
	mutex               sync.RWMutex
	updateMutex         sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
	chanFailure         chan *MemcacheBackendConnection
	proxy               *MemcacheProxy
	waitBackendsTimeout time.Duration
	waitBackends        map[string]chan struct{}
}

// NewMemcacheBackendConnectionPool creates a new MemcacheBackendConnectionPool.
func NewMemcacheBackendConnectionPool(proxy *MemcacheProxy) *MemcacheBackendConnectionPool {
	mbcp := &MemcacheBackendConnectionPool{
		pools:               make(map[string]map[*MemcacheBackendConnection]struct{}),
		proxy:               proxy,
		chanFailure:         make(chan *MemcacheBackendConnection),
		waitBackendsTimeout: proxy.connectTimeout,
		waitBackends:        make(map[string]chan struct{}),
	}
	mbcp.ctx, mbcp.cancel = context.WithCancel(proxy.ctx)

	// Background worker to handle backend connection failures
	go func() {
		for {
			select {
			case mbc := <-mbcp.chanFailure:
				proxy.log.Error().Str("peer", mbc.backend.Address).Msg("Backend connection failed")
				mbcp.Del(mbc)
				mbcp.Update()
			case <-mbcp.ctx.Done():
				return
			}
		}
	}()

	return mbcp
}

func (mbcp *MemcacheBackendConnectionPool) updateWaitState(address string) {
	if mbcp.pools[address] == nil {
		mbcp.pools[address] = make(map[*MemcacheBackendConnection]struct{})
	}
	needsWait := len(mbcp.pools[address]) == 0
	ch, exists := mbcp.waitBackends[address]

	if needsWait == exists {
		return
	}
	if needsWait {
		mbcp.waitBackends[address] = make(chan struct{})
	} else {
		close(ch)
		delete(mbcp.waitBackends, address)
	}
}

// Wait blocks until at least one connection is available for the given address or the context is cancelled.
func (mbcp *MemcacheBackendConnectionPool) Wait(ctx context.Context, address string) error {
	mbcp.mutex.RLock()
	ch := mbcp.waitBackends[address]
	mbcp.mutex.RUnlock()
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Get returns an available connection from the pool for the given address.
// If wait is true and no connection is available, it will block until one is created
// or waitBackendsTimeout is reached.
func (mbcp *MemcacheBackendConnectionPool) Get(address string, wait bool) *MemcacheBackendConnection {
	mbcp.mutex.RLock()
	defer mbcp.mutex.RUnlock()

	if mbcp.pools[address] == nil || len(mbcp.pools[address]) == 0 {
		if mbcp.waitBackendsTimeout > 0 && wait {
			mbcp.mutex.RUnlock()
			ctx, ctxCancel := context.WithDeadline(mbcp.ctx, time.Now().Add(mbcp.waitBackendsTimeout))
			defer ctxCancel()
			_ = mbcp.Wait(ctx, address)
			mbcp.mutex.RLock()
		}
	}

	for mbc := range mbcp.pools[address] {
		return mbc
	}
	return nil
}

// Del removes a connection from the pool.
func (mbcp *MemcacheBackendConnectionPool) Del(mbc *MemcacheBackendConnection) {
	mbcp.mutex.Lock()
	defer mbcp.mutex.Unlock()
	if mbcp.pools[mbc.backend.Address] != nil {
		delete(mbcp.pools[mbc.backend.Address], mbc)
		mbcp.updateWaitState(mbc.backend.Address)
	}
}

// NotifyFailure is called by a connection when it encounters a fatal error.
func (mbcp *MemcacheBackendConnectionPool) NotifyFailure(mbc *MemcacheBackendConnection) {
	mbcp.chanFailure <- mbc
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
			for conn := range pool {
				conn.cancel()
			}
			delete(mbcp.pools, addr)
			delete(mbcp.waitBackends, addr)
		}
	}

	for _, b := range backends {
		mbcp.updateWaitState(b.Address)
	}
	mbcp.mutex.Unlock()

	backoff := misc.NewExponentialBackoff(100*time.Millisecond, 1*time.Second, 1.5)

	// Ensure each backend has the required number of connections
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
				if mbcp.pools[backend.Address] == nil {
					mbcp.pools[backend.Address] = make(map[*MemcacheBackendConnection]struct{})
				}
				mbcp.pools[backend.Address][mbc] = struct{}{}
				mbcp.updateWaitState(backend.Address)
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
