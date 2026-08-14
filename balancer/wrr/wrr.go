package wrr

import (
	"context"
	"slices"
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
	module.RegisterFactory("balancer", "wrr", newWRRBalancer, validateWRRBalancerConfig)
}

// swrrState holds the pre-calculated sequence for Smooth Weighted Round-Robin.
type swrrState struct {
	sequence []string
	length   uint64
}

// WRRBalancer implements a Smooth Weighted Round-Robin load balancing algorithm.
// It uses an atomic pointer to a pre-calculated sequence to provide lock-free selection.
type WRRBalancer struct {
	id          string
	backends    *backend.Registry
	state       atomic.Pointer[swrrState]
	counter     atomic.Uint64
	mu          sync.Mutex // Protects weight map and registry during updates
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

	b.state.Store(&swrrState{sequence: []string{}, length: 0})

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

					if upd.Kind == backend.UpdBackendAdded {
						clone := upd.Backend.Clone()
						clone.Ctx, clone.Cancel = context.WithCancel(b.ctx)
						b.backends.Add(clone)
					} else {
						b.log.Debug().Str("address", upd.Address).Int("weight", weight).Msg("Updating backend in WRR balancer")
						b.backends.Update(upd.Backend.Clone())
					}

					b.backends.Get(upd.Address).Meta.Set("wrr", "weight", cty.NumberIntVal(int64(weight)))
					weights[upd.Address] = weight

				case backend.UpdBackendRemoved:
					if be := b.backends.Get(upd.Address); be != nil && be.Cancel != nil {
						be.Cancel()
					}
					b.backends.Remove(upd.Address)
					delete(weights, upd.Address)
				}

				// Re-calculate SWRR sequence
				b.refreshSequence(weights)
				b.mu.Unlock()

			case <-b.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return b, nil
}

// refreshSequence generates the Smooth Weighted Round-Robin sequence and updates the atomic state.
func (b *WRRBalancer) refreshSequence(weights map[string]int) {
	totalWeight := 0
	addresses := make([]string, 0, len(weights))
	for addr, w := range weights {
		if w > 0 {
			totalWeight += w
			addresses = append(addresses, addr)
		}
	}

	if totalWeight == 0 {
		b.state.Store(&swrrState{sequence: []string{}, length: 0})
		return
	}

	// Sort addresses for deterministic sequence generation
	slices.Sort(addresses)

	sequence := make([]string, 0, totalWeight)
	currentWeights := make([]int, len(addresses))

	for i := 0; i < totalWeight; i++ {
		bestIdx := -1
		for j, addr := range addresses {
			currentWeights[j] += weights[addr]
			if bestIdx == -1 || currentWeights[j] > currentWeights[bestIdx] {
				bestIdx = j
			}
		}

		sequence = append(sequence, addresses[bestIdx])
		currentWeights[bestIdx] -= totalWeight
	}

	b.state.Store(&swrrState{
		sequence: sequence,
		length:   uint64(len(sequence)),
	})
}

// GetBackend returns a backend from the pre-calculated SWRR sequence.
// Selection is lock-free using an atomic counter and pointer.
// It returns the selected backend and a no-op release function.
func (b *WRRBalancer) GetBackend(wait bool) (*backend.Backend, func()) {
	state := b.state.Load()

	// Wait for the backend list to be populated or a timeout to occur
	if state.length == 0 && b.timeout > 0 && wait {
		ctx, ctxCancel := context.WithDeadline(b.ctx, time.Now().Add(b.timeout))
		defer ctxCancel()
		_ = b.backends.Wait(ctx)
		state = b.state.Load()
	}

	if state.length > 0 {
		idx := b.counter.Add(1) - 1
		address := state.sequence[idx%state.length]
		return b.backends.Get(address), func() {}
	} else {
		return nil, func() {}
	}
}

// ReceiveUpdate implements the backend.BackendUpdateSubscriber interface.
func (b *WRRBalancer) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case b.updChan <- upd:
	case <-b.updChanStop:
	}
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
