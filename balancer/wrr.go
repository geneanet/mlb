package balancer

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"

	"mlb/backend"
)

func init() {
	factories["wrr"] = &WRRBalancerFactory{}
}

type WRRBalancer struct {
	fullname     string
	backends     backend.BackendsMap
	weightedlist []string
	mu           sync.Mutex
	iterator     int
	log          zerolog.Logger
}

type WRRBalancerConfig struct {
	FullName string `hcl:"name,label"`
	Source   string `hcl:"source"`
}

type WRRBalancerFactory struct{}

func (w WRRBalancerFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &WRRBalancerConfig{}
	return gohcl.DecodeBody(tc.Config, nil, config)
}

func (w WRRBalancerFactory) parseConfig(tc *Config) *WRRBalancerConfig {
	config := &WRRBalancerConfig{}
	gohcl.DecodeBody(tc.Config, nil, config)
	config.FullName = fmt.Sprintf("balancer.%s.%s", tc.Type, tc.Name)
	return config
}

func (w WRRBalancerFactory) New(tc *Config, sources map[string]backend.BackendUpdateProvider, wg *sync.WaitGroup, ctx context.Context) backend.BackendProvider {
	config := w.parseConfig(tc)

	b := &WRRBalancer{
		fullname:     config.FullName,
		backends:     make(backend.BackendsMap),
		weightedlist: make([]string, 0),
		iterator:     0,
		log:          log.With().Str("id", config.FullName).Logger(),
	}

	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WRR Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WRR Balancer stopped")
		defer cancel()

		upd_chan := sources[config.Source].Subscribe()

	mainloop:
		for {
			select {
			case upd := <-upd_chan: // Backend changed
				b.mu.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded:
					b.log.Info().Str("address", upd.Address).Int("weight", upd.Backend.Weight).Msg("Adding backend to WRR balancer")
					b.backends[upd.Address] = upd.Backend.Clone()
					for i := 0; i < upd.Backend.Weight; i++ {
						b.weightedlist = append(b.weightedlist, upd.Address)
					}
				case backend.UpdBackendModified:
					b.log.Info().Str("address", upd.Address).Int("weight", upd.Backend.Weight).Msg("Updating backend in WRR balancer")
					b.backends[upd.Address] = upd.Backend.Clone()
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == upd.Address })
					for i := 0; i < upd.Backend.Weight; i++ {
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
