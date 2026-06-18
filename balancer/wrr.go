package balancer

import (
	"context"
	"fmt"
	"math/rand"
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

// WRRBalancer implements a Weighted Round-Robin load balancing algorithm.
type WRRBalancer struct {
	id           string
	backends     *backend.Registry
	weightedList []string
	mu           sync.RWMutex
	log          zerolog.Logger
	updChan      chan backend.BackendUpdate
	updChanStop  chan struct{}
	source       string
	evalCtx      *hcl.EvalContext
	ctx          context.Context
	ctxCancel    context.CancelFunc
	timeout      time.Duration
}

// WRRBalancerConfig defines the HCL configuration for the WRR balancer.
type WRRBalancerConfig struct {
	ID      string
	Source  string         `hcl:"source"`
	Weight  hcl.Expression `hcl:"weight"`
	Timeout string         `hcl:"timeout,optional"`
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
	config.ID = fmt.Sprintf("balancer.%s.%s", tc.Type, tc.Name)
	if config.Timeout == "" {
		config.Timeout = "0s"
	}
	return config
}

// newWRRBalancer creates a new instance of a WRR balancer.
func newWRRBalancer(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) any {
	config := parseWRRBalancerConfig(tc)

	b := &WRRBalancer{
		id:           config.ID,
		backends:     backend.NewRegistry(),
		weightedList: make([]string, 0),
		log:          log.With().Str("id", config.ID).Logger(),
		updChan:      make(chan backend.BackendUpdate, 100),
		updChanStop:  make(chan struct{}),
		source:       config.Source,
		evalCtx:      tc.Ctx,
	}

	var err error

	b.timeout, err = time.ParseDuration(config.Timeout)
	if err != nil {
		panic(err)
	}

	b.ctx, b.ctxCancel = context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WRR Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WRR Balancer stopped")
		defer b.ctxCancel()
		defer close(b.updChanStop)

	mainloop:
		for {
			select {
			case upd := <-b.updChan: // Backend changed
				b.mu.Lock()

				switch upd.Kind {
				case backend.UpdBackendAdded:
					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}

					b.log.Info().Str("address", upd.Address).Int("weight", weight).Msg("Adding backend to WRR balancer")
					clone := upd.Backend.Clone()
					clone.Ctx, clone.Cancel = context.WithCancel(b.ctx)
					b.backends.Add(clone)
					b.backends.Get(upd.Address).Meta.Set("wrr", "weight", cty.NumberIntVal(int64(weight)))
					for i := 0; i < weight; i++ {
						b.weightedList = append(b.weightedList, upd.Address)
					}
				case backend.UpdBackendModified:
					var currentweight int = 0
					for _, addr := range b.weightedList {
						if addr == upd.Address {
							currentweight++
						}
					}

					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}

					b.log.Debug().Str("address", upd.Address).Msg("Updating backend in WRR balancer")
					b.backends.Update(upd.Backend.Clone())
					b.backends.Get(upd.Address).Meta.Set("wrr", "weight", cty.NumberIntVal(int64(weight)))

					if weight != currentweight {
						b.log.Info().Str("address", upd.Address).Int("oldWeight", currentweight).Int("newWeight", weight).Msg("Updating backend weight in WRR balancer")
						b.weightedList = slices.DeleteFunc(b.weightedList, func(a string) bool { return a == upd.Address })
						for i := 0; i < weight; i++ {
							b.weightedList = append(b.weightedList, upd.Address)
						}
					}
				case backend.UpdBackendRemoved:
					b.log.Info().Str("address", upd.Address).Msg("Removing backend from WRR balancer")
					b.weightedList = slices.DeleteFunc(b.weightedList, func(a string) bool { return a == upd.Address })
					if be := b.backends.Get(upd.Address); be != nil && be.Cancel != nil {
						be.Cancel()
					}
					b.backends.Remove(upd.Address)
				}

				b.mu.Unlock()

			case <-b.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return b
}

// GetBackend returns a random backend from the weighted list.
// If wait is true and the list is empty, it will wait for a backend or timeout.
func (b *WRRBalancer) GetBackend(wait bool) *backend.Backend {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Wait for the backend list to be populated or a timeout to occur
	if len(b.weightedList) == 0 && b.timeout > 0 && wait {
		b.mu.RUnlock()
		ctx, ctxCancel := context.WithDeadline(b.ctx, time.Now().Add(b.timeout))
		defer ctxCancel()
		_ = b.backends.Wait(ctx)
		b.mu.RLock()
	}

	if len(b.weightedList) > 0 {
		address := b.weightedList[rand.Intn(len(b.weightedList))]
		return b.backends.Get(address)
	} else {
		return nil
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
func (b *WRRBalancer) Bind(modules module.ModulesRegistry) {
	module.Get[backend.BackendUpdateProvider](modules, b.source).ProvideUpdates(b)
}
