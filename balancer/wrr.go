package balancer

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"mlb/backend"
	"mlb/misc"
)

func init() {
	factories["wrr"] = &WRRBalancerFactory{}
}

type WRRBalancer struct {
	id           string
	backends     backend.BackendsMap
	weightedlist []string
	mu           sync.Mutex
	iterator     int
	log          zerolog.Logger
	upd_chan     chan backend.BackendUpdate
	source       string
	evalCtx      *hcl.EvalContext
}

type WRRBalancerConfig struct {
	ID     string
	Source string         `hcl:"source"`
	Weight hcl.Expression `hcl:"weight"`
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
	return config
}

func (w WRRBalancerFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) backend.BackendProvider {
	config := w.parseConfig(tc)

	b := &WRRBalancer{
		id:           config.ID,
		backends:     make(backend.BackendsMap),
		weightedlist: make([]string, 0),
		iterator:     0,
		log:          log.With().Str("id", config.ID).Logger(),
		upd_chan:     make(chan backend.BackendUpdate),
		source:       config.Source,
		evalCtx:      tc.ctx,
	}

	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WRR Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WRR Balancer stopped")
		defer cancel()

	mainloop:
		for {
			select {
			case upd := <-b.upd_chan: // Backend changed
				b.mu.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded:
					var weight int
					_, diags := upd.Backend.ResolveExpression(config.Weight, b.evalCtx, &weight)
					if diags.HasErrors() {
						b.log.Error().Msg(diags.Error())
					}

					b.log.Info().Str("address", upd.Address).Int("weight", weight).Msg("Adding backend to WRR balancer")
					b.backends[upd.Address] = upd.Backend.Clone()
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
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == upd.Address })
					for i := 0; i < weight; i++ {
						b.weightedlist = append(b.weightedlist, upd.Address)
					}
				case backend.UpdBackendRemoved:
					b.log.Info().Str("address", upd.Address).Msg("Removing backend from WRR balancer")
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == upd.Address })
					delete(b.backends, upd.Address)
				}
				if b.iterator >= len(b.weightedlist) {
					b.iterator = 0
				}
				b.mu.Unlock()

			case <-ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return b
}

func (b *WRRBalancer) GetBackend() *backend.Backend {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.weightedlist) > 0 {
		address := b.weightedlist[b.iterator]

		b.iterator++
		if b.iterator >= len(b.weightedlist) {
			b.iterator = 0
		}
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
