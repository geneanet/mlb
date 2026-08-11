package redis

import (
	"context"
	"io"
	"mlb/backend"
	"mlb/misc"
	"sync"
	"time"
)

type RedisBackendConnectionFailure struct {
	rbc         *RedisBackendConnection
	err         error
	hadInFlight bool
}

// RedisBackendConnectionPool manages a pool of connections to Redis backends.
type RedisBackendConnectionPool struct {
	pool                map[*RedisBackendConnection]struct{}
	mutex               sync.RWMutex
	updateMutex         sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
	chanFailure         chan RedisBackendConnectionFailure
	proxy               *RedisProxy
	waitBackendsTimeout time.Duration
	waitBackends        chan struct{}
}

// NewRedisBackendConnectionPool creates a new RedisBackendConnectionPool.
func NewRedisBackendConnectionPool(proxy *RedisProxy) *RedisBackendConnectionPool {
	rbcp := &RedisBackendConnectionPool{
		pool:                make(map[*RedisBackendConnection]struct{}),
		proxy:               proxy,
		chanFailure:         make(chan RedisBackendConnectionFailure),
		waitBackendsTimeout: proxy.backendWaitTimeout,
		waitBackends:        make(chan struct{}),
	}
	rbcp.ctx, rbcp.cancel = context.WithCancel(proxy.ctx)

	proxy.log.Debug().Msg("No connection in the pool, blocking GetRandom")

	// Remove failed connections
	go func() {
		for {
			select {
			case failure := <-rbcp.chanFailure:
				if failure.err == io.EOF && !failure.hadInFlight {
					proxy.log.Debug().Str("peer", failure.rbc.backend.Address).Msg("Backend connection closed (idle)")
				} else {
					proxy.log.Error().Str("peer", failure.rbc.backend.Address).Err(failure.err).Msg("Backend connection failed")
				}
				proxy.backendConnectionPool.Del(failure.rbc)
				proxy.backendConnectionPool.Update()
			case <-rbcp.ctx.Done():
				return
			}
		}
	}()

	return rbcp
}

// updateWaitState updates the wait state based on pool size.
func (rbcp *RedisBackendConnectionPool) updateWaitState() {
	needsWait := len(rbcp.pool) == 0
	if needsWait == (rbcp.waitBackends != nil) {
		return
	}
	if needsWait {
		rbcp.proxy.log.Debug().Msg("There are no more connections in the pool, blocking GetRandom")
		rbcp.waitBackends = make(chan struct{})
	} else {
		rbcp.proxy.log.Debug().Msg("At least one connection has been added to the pool, unblocking GetRandom")
		close(rbcp.waitBackends)
		rbcp.waitBackends = nil
	}
}

// Wait blocks until at least one connection is available in the pool or the context is cancelled.
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

// GetRandom returns a random connection from the pool.
// If wait is true and the pool is empty, it will wait for a connection or timeout.
func (rbcp *RedisBackendConnectionPool) GetRandom(wait bool) *RedisBackendConnection {
	rbcp.mutex.RLock()

	healthyCount := 0
	for rbc := range rbcp.pool {
		if rbc.ctx.Err() == nil {
			healthyCount++
		}
	}

	// Wait for a connection to be added to the pool or a timeout to occur
	if healthyCount == 0 && rbcp.waitBackendsTimeout > 0 && wait {
		rbcp.mutex.RUnlock()
		ctx, ctxCancel := context.WithDeadline(rbcp.ctx, time.Now().Add(rbcp.waitBackendsTimeout))
		defer ctxCancel()
		_ = rbcp.Wait(ctx)
		rbcp.mutex.RLock()
	}

	// 1. Try to find a healthy, non-full connection
	for rbc := range rbcp.pool {
		if rbc.ctx.Err() == nil && !rbc.IsFull() {
			rbcp.mutex.RUnlock()
			return rbc
		}
	}

	// 2. All connections are full or unhealthy. Try to grow if below max.
	if len(rbcp.pool) < rbcp.proxy.backendMaxConnections {
		// Pick a backend from the pool's existing connections to stay on the same service
		// Or pick the first from the sorted list if we want to be more consistent
		var backend *backend.Backend
		for rbc := range rbcp.pool {
			backend = rbc.backend
			break
		}
		rbcp.mutex.RUnlock()

		if backend != nil {
			rbc, err := NewRedisBackendConnection(rbcp, backend)
			if err == nil {
				rbcp.mutex.Lock()
				rbcp.pool[rbc] = struct{}{}
				rbcp.updateWaitState()
				rbcp.mutex.Unlock()
				return rbc
			}
		}

		// Fallback to random if growth fails
		rbcp.mutex.RLock()
	}

	for rbc := range rbcp.pool {
		if rbc.ctx.Err() == nil {
			rbcp.mutex.RUnlock()
			return rbc
		}
	}
	rbcp.mutex.RUnlock()
	return nil
}

func (rbcp *RedisBackendConnectionPool) Del(rbc *RedisBackendConnection) {
	rbcp.mutex.Lock()
	defer rbcp.mutex.Unlock()
	delete(rbcp.pool, rbc)
	rbcp.updateWaitState()
}

func (rbcp *RedisBackendConnectionPool) NotifyFailure(rbc *RedisBackendConnection, err error, hadInFlight bool) {
	rbcp.chanFailure <- RedisBackendConnectionFailure{rbc: rbc, err: err, hadInFlight: hadInFlight}
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
	tries := 0

	for {
		rbcp.mutex.Lock()
		poolLen := len(rbcp.pool)
		rbcp.mutex.Unlock()

		if poolLen >= rbcp.proxy.backendMinConnections {
			break
		}

		// Pick a backend
		backends := rbcp.proxy.backends.GetSortedList()
		if len(backends) == 0 {
			rbcp.proxy.log.Warn().Msg("Unable to find a new backend")
			break // Don't loop infinitely, wait for the next update event
		}
		backend := backends[0]

		// Add the backend (network call is done outside of rbcp.mutex lock)
		rbc, err := NewRedisBackendConnection(rbcp, backend)
		if err != nil {
			rbcp.proxy.log.Warn().Err(err).Str("peer", backend.Address).Msg("Unable to connect to backend")
			tries++
			if tries >= 3 {
				break // Give up after 3 failures to avoid blocking other updates
			}
			backoff.Sleep(rbcp.ctx)
		} else {
			rbcp.mutex.Lock()
			rbcp.pool[rbc] = struct{}{}
			rbcp.updateWaitState()
			rbcp.mutex.Unlock()
			backoff.Reset()
			tries = 0
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
