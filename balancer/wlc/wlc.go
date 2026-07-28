package wlc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"

	"mlb/backend"
	"mlb/config"
	"mlb/module"
)

func init() {
	module.RegisterFactory("balancer", "wlc", newWLCBalancer, validateWLCBalancerConfig)
}

// backendStats holds the weight and active connection count for a backend.
type backendStats struct {
	backend    *backend.Backend
	weight     int64
	activeConn atomic.Int64
}

// WLCBalancer implements a Weighted Least Connections load balancing algorithm.
type WLCBalancer struct {
	id          string
	backends    *backend.Registry
	stats       map[string]*backendStats
	mu          sync.RWMutex
	log         zerolog.Logger
	updChan     chan backend.BackendUpdate
	updChanStop chan struct{}
	source      string
	evalCtx     *hcl.EvalContext
	ctx         context.Context
	ctxCancel   context.CancelFunc
	timeout     time.Duration
}

// WLCBalancerConfig defines the HCL configuration for the WLC balancer.
type WLCBalancerConfig struct {
	ID      string
	Source  string         `hcl:"source"`
	Weight  hcl.Expression `hcl:"weight"`
	Timeout string         `hcl:"timeout,optional"`
}

// validateWLCBalancerConfig validates the WLC balancer configuration.
func validateWLCBalancerConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &WLCBalancerConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Timeout, "timeout")

	return diags
}

// parseWLCBalancerConfig parses the WLC balancer configuration.
func parseWLCBalancerConfig(tc *module.Config) *WLCBalancerConfig {
	config := &WLCBalancerConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode WLC balancer config")
	}
	config.ID = tc.FullID()
	if config.Timeout == "" {
		config.Timeout = "0s"
	}
	return config
}

// newWLCBalancer creates a new instance of a WLC balancer.
func newWLCBalancer(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseWLCBalancerConfig(tc)

	b := &WLCBalancer{
		id:          config.ID,
		backends:    backend.NewRegistry(),
		stats:       make(map[string]*backendStats),
		log:         log.With().Str("id", config.ID).Logger(),
		updChan:     make(chan backend.BackendUpdate, 100),
		updChanStop: make(chan struct{}),
		source:      config.Source,
		evalCtx:     tc.Ctx,
	}

	var err error

	b.timeout, err = time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, err
	}

	b.ctx, b.ctxCancel = context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WLC Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WLC Balancer stopped")
		defer b.ctxCancel()
		defer close(b.updChanStop)

	mainloop:
		for {
			select {
			case upd := <-b.updChan: // Backend changed
				b.mu.Lock()

				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					var weight int64
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}
					if weight < 0 {
						weight = 0
					}

					if upd.Kind == backend.UpdBackendAdded {
						b.log.Info().Str("address", upd.Address).Int64("weight", weight).Msg("Adding backend to WLC balancer")
						clone := upd.Backend.Clone()
						clone.Ctx, clone.Cancel = context.WithCancel(b.ctx)
						b.backends.Add(clone)
						b.stats[upd.Address] = &backendStats{
							backend: clone,
							weight:  weight,
						}
					} else {
						b.log.Debug().Str("address", upd.Address).Int64("weight", weight).Msg("Updating backend in WLC balancer")
						b.backends.Update(upd.Backend.Clone())
						if s, ok := b.stats[upd.Address]; ok {
							s.weight = weight
						}
					}
					b.backends.Get(upd.Address).Meta.Set("wlc", "weight", cty.NumberIntVal(weight))

				case backend.UpdBackendRemoved:
					b.log.Info().Str("address", upd.Address).Msg("Removing backend from WLC balancer")
					if be := b.backends.Get(upd.Address); be != nil && be.Cancel != nil {
						be.Cancel()
					}
					b.backends.Remove(upd.Address)
					delete(b.stats, upd.Address)
				}

				b.mu.Unlock()

			case <-b.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return b, nil
}

// GetBackend returns the least-loaded backend according to the WLC algorithm.
// It returns the selected backend and a release function that MUST be called
// by the proxy when the connection is closed to decrement the active connection count.
func (b *WLCBalancer) GetBackend(wait bool) (*backend.Backend, func()) {
	b.mu.RLock()

	// Wait for the backend list to be populated or a timeout to occur
	if len(b.stats) == 0 && b.timeout > 0 && wait {
		b.mu.RUnlock()
		ctx, ctxCancel := context.WithDeadline(b.ctx, time.Now().Add(b.timeout))
		defer ctxCancel()
		_ = b.backends.Wait(ctx)
		b.mu.RLock()
	}

	var bestStats *backendStats

	for _, s := range b.stats {
		if s.weight <= 0 {
			continue
		}

		if bestStats == nil {
			bestStats = s
			continue
		}

		// Compare Ci / Wi < Cj / Wj  =>  Ci * Wj < Cj * Wi
		ci := s.activeConn.Load()
		wi := s.weight
		cj := bestStats.activeConn.Load()
		wj := bestStats.weight

		if ci*wj < cj*wi {
			bestStats = s
		}
	}

	if bestStats != nil {
		bestStats.activeConn.Add(1)
		be := bestStats.backend
		b.mu.RUnlock()

		return be, func() {
			bestStats.activeConn.Add(-1)
		}
	}

	b.mu.RUnlock()
	return nil, func() {}
}

// ReceiveUpdate implements the backend.BackendUpdateSubscriber interface.
func (b *WLCBalancer) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case b.updChan <- upd:
	case <-b.updChanStop:
	}
}

// GetBackendList returns the current list of backends in the balancer.
func (b *WLCBalancer) GetBackendList() []*backend.Backend {
	return b.backends.GetList()
}

// Bind cross-links the balancer with its source backend provider.
func (b *WLCBalancer) Bind(modules module.ModulesRegistry) error {
	m, err := module.Get[backend.BackendUpdateProvider](modules, b.source)
	if err != nil {
		return err
	}
	m.ProvideUpdates(b)
	return nil
}
