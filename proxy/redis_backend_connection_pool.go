package proxy

import (
	"context"
	"mlb/backend"
	"mlb/misc"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

//----------------
// Connection Pool
//----------------

type RedisBackendConnectionPool struct {
	pool                    map[*RedisBackendConnection]struct{}
	mutex                   sync.RWMutex
	ctx                     context.Context
	cancel                  context.CancelFunc
	chan_failure            chan *RedisBackendConnection
	proxy                   *RedisProxy
	wait_backends_timeout   time.Duration
	wait_backends_semaphore *semaphore.Weighted
}

func NewRedisBackendConnectionPool(proxy *RedisProxy) *RedisBackendConnectionPool {
	rbcp := &RedisBackendConnectionPool{
		pool:                    make(map[*RedisBackendConnection]struct{}),
		proxy:                   proxy,
		chan_failure:            make(chan *RedisBackendConnection),
		wait_backends_timeout:   proxy.backend_wait_timeout,
		wait_backends_semaphore: semaphore.NewWeighted(1),
	}
	rbcp.ctx, rbcp.cancel = context.WithCancel(proxy.ctx)

	proxy.log.Debug().Msg("No connection in the pool, acquiring the lock")
	rbcp.wait_backends_semaphore.Acquire(rbcp.ctx, 1)

	// Remove failed connections
	go func() {
		for {
			select {
			case rbc := <-rbcp.chan_failure:
				proxy.log.Error().Str("peer", rbc.backend.Address).Msg("Backend connection failed")
				proxy.backendConnectionPool.Del(rbc)
				proxy.backendConnectionPool.Update()
			case <-rbcp.ctx.Done():
				return
			}
		}
	}()

	return rbcp
}

func (rbcp *RedisBackendConnectionPool) GetRandom(wait bool) *RedisBackendConnection {
	rbcp.mutex.RLock()
	defer rbcp.mutex.RUnlock()

	// Wait for a connection to be added to the pool or a timeout to occur
	if len(rbcp.pool) == 0 && rbcp.wait_backends_timeout > 0 && wait {
		rbcp.mutex.RUnlock()
		ctx, ctx_cancel := context.WithDeadline(rbcp.ctx, time.Now().Add(rbcp.wait_backends_timeout))
		defer ctx_cancel()
		if rbcp.wait_backends_semaphore.Acquire(ctx, 1) == nil {
			rbcp.wait_backends_semaphore.Release(1)
		}
		rbcp.mutex.RLock()
	}

	for rbc := range rbcp.pool { // Range over map is guaranteed to be random
		return rbc
	}
	return nil
}

func (rbcp *RedisBackendConnectionPool) Del(rbc *RedisBackendConnection) {
	rbcp.mutex.Lock()
	defer rbcp.mutex.Unlock()

	previous_len := len(rbcp.pool)
	delete(rbcp.pool, rbc)
	new_len := len(rbcp.pool)

	if previous_len > 0 && new_len == 0 {
		rbcp.proxy.log.Debug().Msg("There are no more connections in the pool, acquiring the lock")
		rbcp.wait_backends_semaphore.Acquire(rbcp.ctx, 1)
	}
}

func (rbcp *RedisBackendConnectionPool) NotifyFailure(rbc *RedisBackendConnection) {
	rbcp.chan_failure <- rbc
}

func (rbcp *RedisBackendConnectionPool) Update() {
	rbcp.mutex.Lock()
	defer rbcp.mutex.Unlock()

	previous_len := len(rbcp.pool)

	// Remove connections whose backend is not in the proxy backends list anymore
	for conn := range rbcp.pool {
		if !rbcp.proxy.backends.Has(conn.backend.Address) {
			conn.cancel()
			delete(rbcp.pool, conn)
		}
	}

	// Add new connections if needed
	backoff := misc.NewExponentialBackoff(rbcp.proxy.retryPeriod, rbcp.proxy.retryMaxPeriod, rbcp.proxy.retryBackoffFactor)
	for len(rbcp.pool) < rbcp.proxy.backendConnectionPoolSize {
		// Pick a backend
		var backend *backend.Backend
		for {
			backends := rbcp.proxy.backends.GetSortedList()
			if len(backends) > 0 {
				backend = backends[0]
				break
			}
			rbcp.proxy.log.Warn().Msg("Unable to find a new backend")
			backoff.Sleep(rbcp.ctx)

			// Exit if the context is cancelled
			select {
			case <-rbcp.ctx.Done():
				return
			default:
			}
		}
		backoff.Reset()

		// Add the backend
		rbc, err := NewRedisBackendConnection(rbcp, backend)
		if err != nil {
			rbcp.proxy.log.Warn().Err(err).Str("peer", backend.Address).Msg("Unable to connect to backend")
			backoff.Sleep(rbcp.ctx)
		} else {
			rbcp.pool[rbc] = struct{}{}
		}

		// Exit if the context is cancelled
		select {
		case <-rbcp.ctx.Done():
			return
		default:
		}
	}

	new_len := len(rbcp.pool)

	if previous_len == 0 && new_len > 0 {
		rbcp.proxy.log.Debug().Msg("At least one connection has been added to the pool, releasing the lock")
		rbcp.wait_backends_semaphore.Release(1)
	} else if previous_len > 0 && new_len == 0 {
		rbcp.proxy.log.Debug().Msg("There are no more connections in the pool, acquiring the lock")
		rbcp.wait_backends_semaphore.Acquire(rbcp.ctx, 1)
	}
}
