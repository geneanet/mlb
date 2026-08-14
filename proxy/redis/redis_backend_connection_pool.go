package redis

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"
	_ "unsafe" // for go:linkname
)

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

type idleConn struct {
	rbc    *RedisBackendConnection
	usedAt time.Time
}

type shard struct {
	sync.Mutex
	conns []idleConn
}

// RedisBackendConnectionPool manages a pool of connections to Redis backends.
// It uses a sharded LIFO (Last-In-First-Out) strategy with processor affinity
// and work-stealing to maximize throughput and minimize contention.
type RedisBackendConnectionPool struct {
	proxy   *RedisProxy
	ctx     context.Context
	shards  []*shard
	nshards int
}

// NewRedisBackendConnectionPool creates a new RedisBackendConnectionPool and starts
// the background idle connection cleanup routine.
func NewRedisBackendConnectionPool(proxy *RedisProxy) *RedisBackendConnectionPool {
	nshards := runtime.GOMAXPROCS(0)
	rbcp := &RedisBackendConnectionPool{
		proxy:   proxy,
		ctx:     proxy.ctx,
		nshards: nshards,
		shards:  make([]*shard, nshards),
	}

	for i := 0; i < nshards; i++ {
		rbcp.shards[i] = &shard{
			conns: make([]idleConn, 0),
		}
	}

	period := proxy.idleCleanupPeriod
	if period == 0 {
		period = 10 * time.Second
	}

	// Start the idle connection cleanup routine.
	go func() {
		ticker := time.NewTicker(period)
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
	timeout := rbcp.proxy.idleTimeout
	now := time.Now()

	for _, s := range rbcp.shards {
		var toClose []*RedisBackendConnection
		s.Lock()
		// Slice is ordered chronologically (oldest at the beginning).
		// Truncate the slice as soon as we cross the first valid (non-expired) connection.
		i := 0
		for i < len(s.conns) && now.Sub(s.conns[i].usedAt) > timeout {
			toClose = append(toClose, s.conns[i].rbc)
			i++
		}

		if i > 0 {
			// Shift remaining connections to the front.
			copy(s.conns, s.conns[i:])
			// Zero out the end to avoid GC leaks.
			for j := len(s.conns) - i; j < len(s.conns); j++ {
				s.conns[j] = idleConn{}
			}
			s.conns = s.conns[:len(s.conns)-i]
		}
		s.Unlock()

		// Close expired connections outside the lock.
		for _, rbc := range toClose {
			rbcp.proxy.log.Debug().
				Str("peer", rbc.backend.Address).
				Msg("Closing idle backend connection (TTL sweep)")
			if rbc.cancel != nil {
				rbc.cancel()
			}
		}
	}
}

// Get retrieves an available connection from the pool. It implements a sharded LIFO
// strategy with processor affinity and work-stealing.
func (rbcp *RedisBackendConnectionPool) Get(ctx context.Context) (*RedisBackendConnection, error) {
	for {
		var rbc *RedisBackendConnection
		var usedAt time.Time

		// Fast Path: try the shard associated with the current P.
		pid := procPin()
		shardIdx := pid % rbcp.nshards
		s := rbcp.shards[shardIdx]

		s.Lock()
		if n := len(s.conns); n > 0 {
			ic := s.conns[n-1]
			s.conns[n-1] = idleConn{} // Zero out to avoid GC leak
			s.conns = s.conns[:n-1]
			rbc = ic.rbc
			usedAt = ic.usedAt
		}
		s.Unlock()
		procUnpin()

		// Slow Path: Work-Stealing from other shards if the local one was empty.
		if rbc == nil {
			for i := 0; i < rbcp.nshards; i++ {
				if i == shardIdx {
					continue
				}
				os := rbcp.shards[i]
				os.Lock()
				if n := len(os.conns); n > 0 {
					ic := os.conns[n-1]
					os.conns[n-1] = idleConn{}
					os.conns = os.conns[:n-1]
					rbc = ic.rbc
					usedAt = ic.usedAt
					os.Unlock()
					rbcp.proxy.log.Debug().Int("from_shard", i).Int("to_shard", shardIdx).Msg("Work-stealing backend connection")
					break
				}
				os.Unlock()
			}
		}

		if rbc != nil {
			// Check if connection is expired.
			if time.Since(usedAt) > rbcp.proxy.idleTimeout {
				rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Discarding expired connection from pool")
				if rbc.cancel != nil {
					rbc.cancel()
				}
				continue
			}

			// Validation: check if the connection was cancelled while sitting in the pool.
			if rbc.ctx.Err() != nil {
				rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Discarding cancelled backend connection from pool")
				continue
			}

			// Optional deep healthcheck before handing off the connection.
			if rbcp.proxy.healthcheck {
				if err := rbc.Healthcheck(); err != nil {
					rbcp.proxy.log.Warn().Err(err).Str("peer", rbc.backend.Address).Msg("Healthcheck failed, discarding connection")
					rbc.cancel()
					continue
				}
			}

			rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Retrieved backend connection from pool")
			return rbc, nil
		}

		// Pool is empty, attempt to find a backend and dial a new connection.
		backends := rbcp.proxy.backends.GetSortedList()
		if len(backends) == 0 && rbcp.proxy.backendWaitTimeout > 0 {
			rbcp.proxy.log.Debug().Dur("timeout", rbcp.proxy.backendWaitTimeout).Msg("Waiting for backends to become available")
			waitCtx, cancel := context.WithTimeout(ctx, rbcp.proxy.backendWaitTimeout)
			_ = rbcp.proxy.backends.Wait(waitCtx)
			cancel()
			backends = rbcp.proxy.backends.GetSortedList()
		}

		if len(backends) == 0 {
			return nil, fmt.Errorf("no backends available to create new connection")
		}

		backend := backends[rand.Intn(len(backends))]
		rbcp.proxy.log.Debug().Str("peer", backend.Address).Msg("Creating new backend connection (pool empty)")
		return NewRedisBackendConnection(rbcp, backend)
	}
}

// Put adds a connection back to the pool for reuse.
func (rbcp *RedisBackendConnectionPool) Put(rbc *RedisBackendConnection) {
	if rbc.ctx.Err() != nil {
		return
	}
	if !rbcp.proxy.backends.Has(rbc.backend.Address) {
		rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Backend removed, discarding returned connection")
		if rbc.cancel != nil {
			rbc.cancel()
		}
		return
	}

	pid := procPin()
	shardIdx := pid % rbcp.nshards
	s := rbcp.shards[shardIdx]

	s.Lock()
	s.conns = append(s.conns, idleConn{rbc: rbc, usedAt: time.Now()})
	s.Unlock()
	procUnpin()

	rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Int("shard", shardIdx).Msg("Returned backend connection to pool")
}

// Update reconciles the current pool with changes in the backend registry.
func (rbcp *RedisBackendConnectionPool) Update() {
	var totalCurrent int
	for _, s := range rbcp.shards {
		var toClose []*RedisBackendConnection
		s.Lock()
		newConns := s.conns[:0]
		for _, ic := range s.conns {
			if rbcp.proxy.backends.Has(ic.rbc.backend.Address) {
				newConns = append(newConns, ic)
			} else {
				toClose = append(toClose, ic.rbc)
			}
		}
		// Zero out unused part to avoid GC leaks.
		for i := len(newConns); i < len(s.conns); i++ {
			s.conns[i] = idleConn{}
		}
		s.conns = newConns
		totalCurrent += len(s.conns)
		s.Unlock()

		for _, rbc := range toClose {
			rbcp.proxy.log.Debug().Str("peer", rbc.backend.Address).Msg("Closing connection to removed backend")
			if rbc.cancel != nil {
				rbc.cancel()
			}
		}
	}

	// Preconnect: open new connections if we are below the 'preconnect' threshold.
	if rbcp.proxy.preconnect > totalCurrent {
		backends := rbcp.proxy.backends.GetSortedList()
		if len(backends) == 0 {
			return
		}

		for i := 0; i < rbcp.proxy.preconnect-totalCurrent; i++ {
			backend := backends[rand.Intn(len(backends))]
			rbc, err := NewRedisBackendConnection(rbcp, backend)
			if err == nil {
				rbcp.Put(rbc)
			}
		}
	}
}

// Len returns the total number of idle connections across all shards.
func (rbcp *RedisBackendConnectionPool) Len() int {
	var n int
	for _, s := range rbcp.shards {
		if s == nil {
			continue
		}
		s.Lock()
		n += len(s.conns)
		s.Unlock()
	}
	return n
}
