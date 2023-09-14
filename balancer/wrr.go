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
	"golang.org/x/sync/semaphore"

	"mlb/backend"
	"mlb/misc"
	"mlb/module"
)

func init() {
	factories["wrr"] = &WRRBalancerFactory{}
}

type WRRBalancer struct {
	id            string
	backends      backend.BackendsMap
	weightedlist  []string
	mu            sync.RWMutex
	log           zerolog.Logger
	upd_chan      chan backend.BackendUpdate
	source        string
	evalCtx       *hcl.EvalContext
	ctx           context.Context
	ctx_cancel    context.CancelFunc
	wait_backends *semaphore.Weighted
	timeout       time.Duration
}

type WRRBalancerConfig struct {
	ID      string
	Source  string         `hcl:"source"`
	Weight  hcl.Expression `hcl:"weight"`
	Timeout string         `hcl:"timeout,optional"`
}

type WRRBalancerFactory struct{}

func (w WRRBalancerFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &WRRBalancerConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w WRRBalancerFactory) parseConfig(tc *Config) *WRRBalancerConfig {
	config := &WRRBalancerConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("balancer.%s.%s", tc.Type, tc.Name)
	if config.Timeout == "" {
		config.Timeout = "0s"
	}
	return config
}

func (w WRRBalancerFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	b := &WRRBalancer{
		id:            config.ID,
		backends:      make(backend.BackendsMap),
		weightedlist:  make([]string, 0),
		log:           log.With().Str("id", config.ID).Logger(),
		upd_chan:      make(chan backend.BackendUpdate),
		source:        config.Source,
		evalCtx:       tc.ctx,
		wait_backends: semaphore.NewWeighted(1),
	}

	var err error

	b.timeout, err = time.ParseDuration(config.Timeout)
	misc.PanicIfErr(err)

	b.ctx, b.ctx_cancel = context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WRR Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WRR Balancer stopped")
		defer b.ctx_cancel()
		defer close(b.upd_chan)

		b.log.Debug().Msg("No backends in the list, acquiring the lock")
		b.wait_backends.Acquire(b.ctx, 1)

	mainloop:
		for {
			select {
			case upd := <-b.upd_chan: // Backend changed
				b.mu.Lock()

				list_previous_size := len(b.weightedlist)

				switch upd.Kind {
				case backend.UpdBackendAdded:
					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}

					b.log.Info().Str("address", upd.Address).Int("weight", weight).Msg("Adding backend to WRR balancer")
					b.backends[upd.Address] = upd.Backend.Clone()
					b.backends[upd.Address].Meta.Set("wrr", "weight", cty.NumberIntVal(int64(weight)))
					for i := 0; i < weight; i++ {
						b.weightedlist = append(b.weightedlist, upd.Address)
					}
				case backend.UpdBackendModified:
					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}

					b.log.Info().Str("address", upd.Address).Int("weight", weight).Msg("Updating backend in WRR balancer")
					b.backends[upd.Address] = upd.Backend.Clone()
					b.backends[upd.Address].Meta.Set("wrr", "weight", cty.NumberIntVal(int64(weight)))
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == upd.Address })
					for i := 0; i < weight; i++ {
						b.weightedlist = append(b.weightedlist, upd.Address)
					}
				case backend.UpdBackendRemoved:
					b.log.Info().Str("address", upd.Address).Msg("Removing backend from WRR balancer")
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == upd.Address })
					delete(b.backends, upd.Address)
				}

				list_new_size := len(b.weightedlist)

				if list_previous_size == 0 && list_new_size > 0 {
					b.log.Debug().Msg("At least one backend has been added to the list, releasing the lock")
					b.wait_backends.Release(1)
				} else if list_previous_size > 0 && list_new_size == 0 {
					b.log.Debug().Msg("There are no more backends in the list, acquiring the lock")
					b.wait_backends.Acquire(b.ctx, 1)
				}

				b.mu.Unlock()

			case <-b.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return b
}

func (b *WRRBalancer) GetBackend(wait bool) *backend.Backend {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Wait for the backend list to be populated or a timeout to occur
	if len(b.weightedlist) == 0 && b.timeout > 0 && wait {
		b.mu.RUnlock()
		ctx, ctx_cancel := context.WithDeadline(b.ctx, time.Now().Add(b.timeout))
		defer ctx_cancel()
		if b.wait_backends.Acquire(ctx, 1) == nil {
			b.wait_backends.Release(1)
		}
		b.mu.RLock()
	}

	if len(b.weightedlist) > 0 {
		address := b.weightedlist[rand.Intn(len(b.weightedlist))]
		return b.backends[address]
	} else {
		return nil
	}
}

func (b *WRRBalancer) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(b.upd_chan)
}

func (b *WRRBalancer) GetUpdateSource() string {
	return b.source
}

func (b *WRRBalancer) GetID() string {
	return b.id
}

func (b *WRRBalancer) GetBackendList() []*backend.Backend {
	return misc.MapValues(b.backends)
}

func (b *WRRBalancer) Bind(modules module.ModulesList) {
	b.SubscribeTo(modules.GetBackendUpdateProvider(b.source))
}
