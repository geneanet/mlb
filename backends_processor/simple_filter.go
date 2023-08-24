package backends_processor

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/misc"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	factories["simple_filter"] = &SimpleFilterFactory{}
}

type SimpleFilter struct {
	id             string
	subscribers    []chan backend.BackendUpdate
	backends       backend.BackendsMap
	backends_mutex sync.RWMutex
	log            zerolog.Logger
	upd_chan       chan backend.BackendUpdate
	source         string
	condition      hcl.Expression
	evalCtx        *hcl.EvalContext
}

type SimpleFilterConfig struct {
	ID        string         `hcl:"id,label"`
	Source    string         `hcl:"source"`
	Condition hcl.Expression `hcl:"condition"`
}

type SimpleFilterFactory struct{}

func (w SimpleFilterFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &SimpleFilterConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w SimpleFilterFactory) parseConfig(tc *Config) *SimpleFilterConfig {
	config := &SimpleFilterConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("backends_processor.%s.%s", tc.Type, tc.Name)
	return config
}

func (w SimpleFilterFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) backend.BackendUpdateProvider {
	config := w.parseConfig(tc)

	f := &SimpleFilter{
		id:          config.ID,
		subscribers: []chan backend.BackendUpdate{},
		backends:    make(backend.BackendsMap),
		log:         log.With().Str("id", config.ID).Logger(),
		upd_chan:    make(chan backend.BackendUpdate),
		source:      config.Source,
		condition:   config.Condition,
		evalCtx:     tc.ctx,
	}

	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	f.log.Info().Msg("Filter starting")

	go func() {
		defer wg.Done()
		defer f.log.Info().Msg("Filter stopped")
		defer cancel()

	mainloop:
		for {
			select {
			case upd := <-f.upd_chan: // Backend changed
				f.backends_mutex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if _, ok := f.backends[upd.Address]; ok { // Modified
						if f.matchFilter(upd.Backend) { // Still passes the filter
							f.backends[upd.Address] = upd.Backend.Clone()
							f.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendModified,
								Address: f.backends[upd.Address].Address,
								Backend: f.backends[upd.Address],
							})
						} else { // Do not pass the filter anymore
							delete(f.backends, upd.Address)
							f.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendRemoved,
								Address: upd.Address,
							})
						}
					} else { // Added
						if f.matchFilter(upd.Backend) {
							f.backends[upd.Address] = upd.Backend.Clone()
							f.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendAdded,
								Address: f.backends[upd.Address].Address,
								Backend: f.backends[upd.Address],
							})
						}
					}
				case backend.UpdBackendRemoved:
					// Removed
					if _, ok := f.backends[upd.Address]; ok {
						delete(f.backends, upd.Address)
						f.sendUpdate(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
						})
					}
				}
				f.backends_mutex.Unlock()
			case <-ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return f
}

func (f *SimpleFilter) ProvideUpdates(ch chan backend.BackendUpdate) {
	f.subscribers = append(f.subscribers, ch)

	go func() {
		f.backends_mutex.RLock()
		defer f.backends_mutex.RUnlock()

		for _, b := range f.backends {
			f.sendUpdate(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: b.Address,
				Backend: b,
			})
		}
	}()
}

func (f *SimpleFilter) sendUpdate(m backend.BackendUpdate) {
	for _, s := range f.subscribers {
		s <- m
	}
}

func (f *SimpleFilter) matchFilter(b *backend.Backend) bool {
	var condition bool
	known, diags := b.ResolveExpression(f.condition, f.evalCtx, &condition)
	if diags.HasErrors() {
		f.log.Error().Msg(diags.Error())
		return false
	}
	return known && condition
}

func (f *SimpleFilter) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(f.upd_chan)
}

func (f *SimpleFilter) GetUpdateSource() string {
	return f.source
}

func (f *SimpleFilter) GetID() string {
	return f.id
}

func (f *SimpleFilter) GetBackendList() []*backend.Backend {
	return misc.MapValues(f.backends)
}
