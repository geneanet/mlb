package redis

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// RedisBackendConnectionPool manages a pool of connections to Redis backends.
// It uses a LIFO (Last-In-First-Out) strategy to reuse the most recently used connections,
// which helps in keeping a subset of connections "hot" and allows others to be idle-closed.
type RedisBackendConnectionPool struct {
	proxy       *RedisProxy
	ctx         context.Context
	mutex       sync.Mutex
	updateMutex sync.Mutex
	pool        []*RedisBackendConnection // The slice of idle connections
}

// NewRedisBackendConnectionPool creates a new RedisBackendConnectionPool and starts
// the background idle connection cleanup routine.
func NewRedisBackendConnectionPool(proxy *RedisProxy) *RedisBackendConnectionPool {
	rbcp := &RedisBackendConnectionPool{
		proxy: proxy,
		ctx:   proxy.ctx,
		pool:  make([]*RedisBackendConnection, 0),
	}

	// Start the idle connection cleanup routine.
	// It periodically scans the pool and closes connections that haven't been used
	// for more than the configured idleTimeout.
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				rbcp.cleanupIdle()
			case <-rbcp.ctx.Done():
				return
			}
		}
	}()

	return rbcp
}

// cleanupIdle removes and closes connections that have exceeded the idle timeout.
func (rbcp *RedisBackendConnectionPool) cleanupIdle() {
	rbcp.mutex.Lock()
	defer rbcp.mutex.Unlock()

	now := time.Now()
	closedCount := 0
	newPool := make([]*RedisBackendConnection, 0, len(rbcp.pool))
	for _, rbc := range rbcp.pool {
		if now.Sub(rbc.lastUsed) > rbcp.proxy.idleTimeout {
			closedCount++
			rbcp.proxy.log.Debug().
				Str("peer", rbc.backend.Address).
				Dur("idle_time", now.Sub(rbc.lastUsed)).
				Int("pool_size", len(rbcp.pool)-closedCount).
				Msg("Closing idle backend connection")
			if rbc.cancel != nil {
				rbc.cancel() // This triggers the cleanup routine in NewRedisBackendConnection
			}
		} else {
			newPool = append(newPool, rbc)
		}
	}
	rbcp.pool = newPool
}

// Get retrieves an available connection from the pool. If the pool is empty, it attempts
// to pick a backend and create a new connection. It implements LIFO to reuse "hot" connections.
func (rbcp *RedisBackendConnectionPool) Get(ctx context.Context) (*RedisBackendConnection, error) {
	for {
		rbcp.mutex.Lock()
		var rbc *RedisBackendConnection
		if len(rbcp.pool) > 0 {
			// LIFO: pop the last element to reuse the most recently returned connection.
			lastIdx := len(rbcp.pool) - 1
			rbc = rbcp.pool[lastIdx]
			rbcp.pool = rbcp.pool[:lastIdx]
			rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Int("pool_size", len(rbcp.pool)).Msg("Retrieved backend connection from pool")
		}
		rbcp.mutex.Unlock()

		if rbc == nil {
			// Pool is empty, attempt to find a backend and dial a new connection.
			backends := rbcp.proxy.backends.GetSortedList()

			// If no backends are available, we might wait a bit for the registry to be populated
			// (e.g., during startup or service discovery updates).
			if len(backends) == 0 && rbcp.proxy.backendWaitTimeout > 0 {
				rbcp.proxy.log.Debug().Dur("timeout", rbcp.proxy.backendWaitTimeout).Msg("Waiting for backends to become available")
				waitCtx, cancel := context.WithTimeout(ctx, rbcp.proxy.backendWaitTimeout)
				_ = rbcp.proxy.backends.Wait(waitCtx)
				cancel()
				backends = rbcp.proxy.backends.GetSortedList()
			}

			if len(backends) == 0 {
				return nil, fmt.Errorf("No backends available to create new connection")
			}

			// Pick a random backend from the available ones to balance new connections.
			backend := backends[rand.Intn(len(backends))]
			rbcp.proxy.log.Debug().Str("peer", backend.Address).Msg("Creating new backend connection (pool empty)")
			return NewRedisBackendConnection(rbcp, backend)
		}

		// Validation: check if the connection was cancelled while sitting in the pool.
		if rbc.ctx.Err() != nil {
			rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Discarding cancelled backend connection from pool")
			continue
		}

		// Optional deep healthcheck (e.g., sending PING) before handing off the connection.
		if rbcp.proxy.healthcheck {
			if err := rbc.Healthcheck(); err != nil {
				rbcp.proxy.log.Warn().Err(err).Str("peer", rbc.backend.Address).Msg("Healthcheck failed, discarding connection")
				rbc.cancel()
				continue
			}
		}

		return rbc, nil
	}
}

// Put adds a connection back to the pool for reuse. It ignores connections that are already cancelled.
func (rbcp *RedisBackendConnectionPool) Put(rbc *RedisBackendConnection) {
	if rbc.ctx.Err() != nil {
		return
	}
	rbcp.mutex.Lock()
	defer rbcp.mutex.Unlock()
	rbcp.pool = append(rbcp.pool, rbc)
	rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Int("pool_size", len(rbcp.pool)).Msg("Returned backend connection to pool")
}

// Update reconciles the current pool with changes in the backend registry.
// It removes connections to backends that are no longer present and triggers preconnect
// if the pool size is below the configured target.
func (rbcp *RedisBackendConnectionPool) Update() {
	rbcp.updateMutex.Lock()
	defer rbcp.updateMutex.Unlock()

	rbcp.mutex.Lock()
	rbcp.proxy.log.Debug().Int("pool_size", len(rbcp.pool)).Msg("Updating backend connection pool")

	// Remove connections that belong to backends no longer in the registry.
	newPool := make([]*RedisBackendConnection, 0, len(rbcp.pool))
	for _, rbc := range rbcp.pool {
		if rbcp.proxy.backends.Has(rbc.backend.Address) {
			newPool = append(newPool, rbc)
		} else {
			rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Closing connection to removed backend")
			if rbc.cancel != nil {
				rbc.cancel()
			}
		}
	}
	rbcp.pool = newPool
	currentCount := len(rbcp.pool)
	rbcp.mutex.Unlock()

	// Preconnect: open new connections if we are below the 'preconnect' threshold.
	if rbcp.proxy.preconnect > currentCount {
		rbcp.proxy.log.Debug().Int("current", currentCount).Int("target", rbcp.proxy.preconnect).Msg("Preconnecting to backends")
		backends := rbcp.proxy.backends.GetSortedList()
		if len(backends) == 0 {
			rbcp.proxy.log.Debug().Msg("No backends available for preconnect")
			return
		}

		for i := 0; i < rbcp.proxy.preconnect-currentCount; i++ {
			// Re-check count inside the loop to avoid over-connecting if other goroutines are active.
			rbcp.mutex.Lock()
			current := len(rbcp.pool)
			rbcp.mutex.Unlock()
			if current >= rbcp.proxy.preconnect {
				break
			}

			backend := backends[rand.Intn(len(backends))]
			rbcp.proxy.log.Debug().Str("peer", backend.Address).Msg("Preconnecting to backend")
			rbc, err := NewRedisBackendConnection(rbcp, backend)
			if err == nil {
				rbcp.Put(rbc)
			} else {
				rbcp.proxy.log.Warn().Err(err).Str("peer", backend.Address).Msg("Preconnect failed")
			}
		}
	}
}
