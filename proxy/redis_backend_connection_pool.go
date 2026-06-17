package proxy

import (
	"context"
	"mlb/backend"
	"mlb/misc"
	"sync"
	"time"
)

//----------------
// Connection Pool
//----------------

type RedisBackendConnectionPool struct {
	pool                map[*RedisBackendConnection]struct{}
	mutex               sync.RWMutex
	updateMutex         sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
	chanFailure         chan *RedisBackendConnection
	proxy               *RedisProxy
	waitBackendsTimeout time.Duration
	waitBackends        chan struct{}
}

func NewRedisBackendConnectionPool(proxy *RedisProxy) *RedisBackendConnectionPool {
	rbcp := &RedisBackendConnectionPool{
		pool:                make(map[*RedisBackendConnection]struct{}),
		proxy:               proxy,
		chanFailure:         make(chan *RedisBackendConnection),
		waitBackendsTimeout: proxy.backendWaitTimeout,
		waitBackends:        make(chan struct{}),
	}
	rbcp.ctx, rbcp.cancel = context.WithCancel(proxy.ctx)

	proxy.log.Debug().Msg("No connection in the pool, blocking GetRandom")

	// Remove failed connections
	go func() {
		for {
			select {
			case rbc := <-rbcp.chanFailure:
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

func (rbcp *RedisBackendConnectionPool) updateWaitState() {
	if (len(rbcp.pool) > 0) == (rbcp.waitBackends != nil) {
		if rbcp.waitBackends != nil {
			rbcp.proxy.log.Debug().Msg("At least one connection has been added to the pool, unblocking GetRandom")
			close(rbcp.waitBackends)
			rbcp.waitBackends = nil
		} else {
			rbcp.proxy.log.Debug().Msg("There are no more connections in the pool, blocking GetRandom")
			rbcp.waitBackends = make(chan struct{})
		}
	}
}

func (rbcp *RedisBackendConnectionPool) Wait(ctx context.Context) error {
	rbcp.mutex.RLock()
	ch := rbcp.waitBackends
	rbcp.mutex.RUnlock()
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

func (rbcp *RedisBackendConnectionPool) GetRandom(wait bool) *RedisBackendConnection {
	rbcp.mutex.RLock()
	defer rbcp.mutex.RUnlock()

	// Wait for a connection to be added to the pool or a timeout to occur
	if len(rbcp.pool) == 0 && rbcp.waitBackendsTimeout > 0 && wait {
		rbcp.mutex.RUnlock()
		ctx, ctxCancel := context.WithDeadline(rbcp.ctx, time.Now().Add(rbcp.waitBackendsTimeout))
		defer ctxCancel()
		_ = rbcp.Wait(ctx)
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
	delete(rbcp.pool, rbc)
	rbcp.updateWaitState()
}

func (rbcp *RedisBackendConnectionPool) NotifyFailure(rbc *RedisBackendConnection) {
	rbcp.chanFailure <- rbc
}

func (rbcp *RedisBackendConnectionPool) Update() {
	rbcp.updateMutex.Lock()
	defer rbcp.updateMutex.Unlock()

	rbcp.mutex.Lock()
	// Remove connections whose backend is not in the proxy backends list anymore
	for conn := range rbcp.pool {
		if !rbcp.proxy.backends.Has(conn.backend.Address) {
			conn.cancel()
			delete(rbcp.pool, conn)
		}
	}
	rbcp.updateWaitState()
	rbcp.mutex.Unlock()

	// Add new connections if needed
	backoff := misc.NewExponentialBackoff(rbcp.proxy.retryPeriod, rbcp.proxy.retryMaxPeriod, rbcp.proxy.retryBackoffFactor)

	for {
		rbcp.mutex.Lock()
		poolLen := len(rbcp.pool)
		rbcp.mutex.Unlock()

		if poolLen >= rbcp.proxy.backendConnectionPoolSize {
			break
		}

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

		// Add the backend (network call is done outside of rbcp.mutex lock)
		rbc, err := NewRedisBackendConnection(rbcp, backend)
		if err != nil {
			rbcp.proxy.log.Warn().Err(err).Str("peer", backend.Address).Msg("Unable to connect to backend")
			backoff.Sleep(rbcp.ctx)
		} else {
			rbcp.mutex.Lock()
			rbcp.pool[rbc] = struct{}{}
			rbcp.mutex.Unlock()
		}

		// Exit if the context is cancelled
		select {
		case <-rbcp.ctx.Done():
			return
		default:
		}
	}

	rbcp.mutex.Lock()
	rbcp.updateWaitState()
	rbcp.mutex.Unlock()
}
