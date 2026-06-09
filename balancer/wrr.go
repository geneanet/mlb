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
	"mlb/misc"
	"mlb/module"
)

func init() {
	factories["wrr"] = &WRRBalancerFactory{}
}

type WRRBalancer struct {
	id           string
	backends     *backend.BackendsMap
	weightedList []string
	mu           sync.RWMutex
	log          zerolog.Logger
	updChan      chan backend.BackendUpdate
	updChanStop  chan struct{}
	source       string
	evalCtx      *hcl.EvalContext
	ctx          context.Context
	ctxCancel    context.CancelFunc
	waitBackends chan struct{}
	timeout      time.Duration
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
		id:           config.ID,
		backends:     backend.NewBackendsMap(),
		weightedList: make([]string, 0),
		log:          log.With().Str("id", config.ID).Logger(),
		updChan:      make(chan backend.BackendUpdate, 100),
		updChanStop:  make(chan struct{}),
		source:       config.Source,
		evalCtx:      tc.ctx,
		waitBackends: make(chan struct{}),
	}

	var err error

	b.timeout, err = time.ParseDuration(config.Timeout)
	misc.PanicIfErr(err)

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

				listPreviousSize := len(b.weightedList)

				switch upd.Kind {
				case backend.UpdBackendAdded:
					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}

					b.log.Info().Str("address", upd.Address).Int("weight", weight).Msg("Adding backend to WRR balancer")
					b.backends.Add(upd.Backend.Clone())
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
					b.backends.Remove(upd.Address)
				}

				listNewSize := len(b.weightedList)

				if listPreviousSize == 0 && listNewSize > 0 {
					b.log.Debug().Msg("At least one backend has been added to the list, unblocking GetBackend")
					close(b.waitBackends)
				} else if listPreviousSize > 0 && listNewSize == 0 {
					b.log.Debug().Msg("There are no more backends in the list, blocking GetBackend")
					b.waitBackends = make(chan struct{})
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
	if len(b.weightedList) == 0 && b.timeout > 0 && wait {
		b.mu.RUnlock()
		ctx, ctxCancel := context.WithDeadline(b.ctx, time.Now().Add(b.timeout))
		defer ctxCancel()
		select {
		case <-b.waitBackends: // Channel closed = backends available
		case <-ctx.Done():
		}
		b.mu.RLock()
	}

	if len(b.weightedList) > 0 {
		address := b.weightedList[rand.Intn(len(b.weightedList))]
		return b.backends.Get(address)
	} else {
		return nil
	}
}

func (b *WRRBalancer) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case b.updChan <- upd:
	case <-b.updChanStop:
	}
}

func (b *WRRBalancer) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(b)
}

func (b *WRRBalancer) GetUpdateSource() string {
	return b.source
}

func (b *WRRBalancer) GetID() string {
	return b.id
}

func (b *WRRBalancer) GetBackendList() []*backend.Backend {
	return b.backends.GetList()
}

func (b *WRRBalancer) Bind(modules module.ModulesList) {
	b.SubscribeTo(modules.GetBackendUpdateProvider(b.source))
}
