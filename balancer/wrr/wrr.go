package wrr

import (
	"context"
	"slices"
	"sync"
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
	module.RegisterFactory("balancer", "wrr", newWRRBalancer, validateWRRBalancerConfig)
}

type swrrPeer struct {
	backend       *backend.Backend
	weight        int64
	currentWeight int64
}

// WRRBalancer implements a Smooth Weighted Round-Robin load balancing algorithm.
// It updates each backend's current weight during selection under a mutex.
type WRRBalancer struct {
	id          string
	backends    *backend.Registry
	peers       []swrrPeer
	totalWeight int64
	mu          sync.Mutex
	log         zerolog.Logger
	updChan     chan backend.BackendUpdate
	updChanStop chan struct{}
	source      string
	evalCtx     *hcl.EvalContext
	ctx         context.Context
	ctxCancel   context.CancelFunc
	timeout     time.Duration
}

// WRRBalancerConfig defines the HCL configuration for the WRR balancer.
type WRRBalancerConfig struct {
	ID                string
	Source            string         `hcl:"source"`
	Weight            hcl.Expression `hcl:"weight"`
	Timeout           string         `hcl:"timeout,optional"`
	LogBackendUpdates bool           `hcl:"log_backend_updates,optional"`
}

// validateWRRBalancerConfig validates the WRR balancer configuration.
func validateWRRBalancerConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &WRRBalancerConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Timeout, "timeout")

	return diags
}

// parseWRRBalancerConfig parses the WRR balancer configuration.
func parseWRRBalancerConfig(tc *module.Config) *WRRBalancerConfig {
	config := &WRRBalancerConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode WRR balancer config")
	}
	config.ID = tc.FullID()
	if config.Timeout == "" {
		config.Timeout = "0s"
	}
	return config
}

// newWRRBalancer creates a new instance of a WRR balancer.
func newWRRBalancer(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseWRRBalancerConfig(tc)

	b := &WRRBalancer{
		id:          config.ID,
		log:         log.With().Str("id", config.ID).Logger(),
		updChan:     make(chan backend.BackendUpdate, 100),
		updChanStop: make(chan struct{}),
		source:      config.Source,
		evalCtx:     tc.Ctx,
		backends:    backend.NewRegistry(log.With().Str("id", config.ID).Logger(), config.LogBackendUpdates),
	}

	var err error

	b.timeout, err = time.ParseDuration(config.Timeout)
	if err != nil {
		return nil, err
	}

	b.ctx, b.ctxCancel = context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WRR Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WRR Balancer stopped")
		defer b.ctxCancel()
		defer close(b.updChanStop)

		weights := make(map[string]int)

	mainloop:
		for {
			select {
			case upd := <-b.updChan: // Backend changed
				b.mu.Lock()

				shouldRefresh := false
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}
					if weight < 0 {
						weight = 0
					}
					if !weightTotalFits(weights, upd.Address, weight) {
						b.log.Error().
							Str("address", upd.Address).
							Int("weight", weight).
							Msg("backend weight would overflow the total WRR weight; update ignored")
						b.mu.Unlock()
						continue mainloop
					}

					if upd.Kind == backend.UpdBackendAdded {
						clone := upd.Backend.Clone()
						b.backends.Add(clone)
						shouldRefresh = true
					} else {
						b.log.Debug().Str("address", upd.Address).Int("weight", weight).Msg("Updating backend in WRR balancer")
						b.backends.Update(upd.Backend.Clone(), "wrr")
						oldWeight, exists := weights[upd.Address]
						if !exists || oldWeight != weight {
							shouldRefresh = true
						}
					}

					b.backends.Get(upd.Address).Meta.Set("wrr", "weight", cty.NumberIntVal(int64(weight)))
					weights[upd.Address] = weight

				case backend.UpdBackendRemoved:
					b.backends.Remove(upd.Address)
					delete(weights, upd.Address)
					shouldRefresh = true
				case backend.UpdReady:
					b.backends.MarkReady()
				}

				// Rebuild the selection state only if weights or backend set changed.
				if shouldRefresh {
					b.refreshPeers(weights)
				}
				b.mu.Unlock()

			case <-b.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return b, nil
}

func weightTotalFits(weights map[string]int, address string, weight int) bool {
	const maxInt64 = int64(1<<63 - 1)

	var total int64
	for existingAddress, existingWeight := range weights {
		if existingAddress == address || existingWeight <= 0 {
			continue
		}
		existing := int64(existingWeight)
		if total > maxInt64-existing {
			return false
		}
		total += existing
	}

	if weight > 0 && total > maxInt64-int64(weight) {
		return false
	}
	return true
}

// refreshPeers rebuilds the weighted peer state in deterministic address order.
// The caller must hold b.mu.
func (b *WRRBalancer) refreshPeers(weights map[string]int) {
	addresses := make([]string, 0, len(weights))
	for addr, w := range weights {
		if w > 0 {
			addresses = append(addresses, addr)
		}
	}

	slices.Sort(addresses)

	peers := make([]swrrPeer, 0, len(addresses))
	var totalWeight int64
	for _, address := range addresses {
		be := b.backends.Get(address)
		if be == nil {
			b.log.Error().Str("address", address).Msg("backend missing while rebuilding WRR state")
			continue
		}
		weight := int64(weights[address])
		peers = append(peers, swrrPeer{backend: be, weight: weight})
		totalWeight += weight
	}

	b.peers = peers
	b.totalWeight = totalWeight
}

// getBackendLocked selects the next backend using smooth weighted round-robin.
// The caller must hold b.mu.
func (b *WRRBalancer) getBackendLocked() (*backend.Backend, func()) {
	if len(b.peers) == 0 {
		return nil, func() {}
	}

	best := 0
	for i := range b.peers {
		b.peers[i].currentWeight += b.peers[i].weight
		if b.peers[i].currentWeight > b.peers[best].currentWeight {
			best = i
		}
	}
	b.peers[best].currentWeight -= b.totalWeight
	return b.peers[best].backend, func() {}
}

// GetBackend returns a backend selected by smooth weighted round-robin.
// It returns the selected backend and a no-op release function.
func (b *WRRBalancer) GetBackend(wait bool) (*backend.Backend, func()) {
	b.mu.Lock()
	be, release := b.getBackendLocked()
	b.mu.Unlock()
	if be != nil {
		return be, release
	}

	// Wait for the backend list to be populated or a timeout to occur
	if b.timeout > 0 && wait {
		ctx, ctxCancel := context.WithDeadline(b.ctx, time.Now().Add(b.timeout))
		defer ctxCancel()
		_ = b.backends.Wait(ctx)

		b.mu.Lock()
		defer b.mu.Unlock()
		return b.getBackendLocked()
	}

	return nil, func() {}
}

// ReceiveUpdate implements the backend.BackendUpdateSubscriber interface.
func (b *WRRBalancer) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case b.updChan <- upd:
	case <-b.updChanStop:
	}
}

// Ready returns a channel that is closed when the balancer is ready.
func (b *WRRBalancer) Ready() <-chan struct{} {
	return b.backends.Ready()
}

// GetBackendList returns the current list of backends in the balancer.
func (b *WRRBalancer) GetBackendList() []*backend.Backend {
	return b.backends.GetList()
}

// Bind cross-links the balancer with its source backend provider.
func (b *WRRBalancer) Bind(modules module.ModulesRegistry) error {
	m, err := module.Get[backend.BackendUpdateProvider](modules, b.source)
	if err != nil {
		return err
	}
	m.ProvideUpdates(b)
	return nil
}
