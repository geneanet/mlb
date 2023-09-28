package backends_processor

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/module"
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
	subscribers    []backend.BackendUpdateSubscriber
	backends       *backend.BackendsMap
	backends_mutex sync.RWMutex
	log            zerolog.Logger
	upd_chan       chan backend.BackendUpdate
	upd_chan_stop  chan struct{}
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

func (w SimpleFilterFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	f := &SimpleFilter{
		id:            config.ID,
		subscribers:   []backend.BackendUpdateSubscriber{},
		backends:      backend.NewBackendsMap(),
		log:           log.With().Str("id", config.ID).Logger(),
		upd_chan:      make(chan backend.BackendUpdate),
		upd_chan_stop: make(chan struct{}),
		source:        config.Source,
		condition:     config.Condition,
		evalCtx:       tc.ctx,
	}

	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	f.log.Info().Msg("Filter starting")

	go func() {
		defer wg.Done()
		defer f.log.Info().Msg("Filter stopped")
		defer cancel()
		defer close(f.upd_chan_stop)

	mainloop:
		for {
			select {
			case upd := <-f.upd_chan: // Backend changed
				f.backends_mutex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if f.backends.Has(upd.Address) { // Modified
						if f.matchFilter(upd.Backend) { // Still passes the filter
							f.backends.Update(upd.Backend.Clone())
							f.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendModified,
								Address: upd.Address,
								Backend: f.backends.Get(upd.Address),
							})
						} else { // Do not pass the filter anymore
							f.backends.Remove(upd.Address)
							f.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendRemoved,
								Address: upd.Address,
							})
						}
					} else { // Added
						if f.matchFilter(upd.Backend) {
							f.backends.Add(upd.Backend.Clone())
							f.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendAdded,
								Address: upd.Address,
								Backend: f.backends.Get(upd.Address),
							})
						}
					}
				case backend.UpdBackendRemoved:
					// Removed
					if f.backends.Has(upd.Address) {
						f.backends.Remove(upd.Address)
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

func (f *SimpleFilter) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	f.subscribers = append(f.subscribers, s)

	go func() {
		f.backends_mutex.RLock()
		defer f.backends_mutex.RUnlock()

		for _, b := range f.backends.GetList() {
			f.sendUpdate(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: b.Address,
				Backend: b,
			})
		}
	}()
}

func (f *SimpleFilter) sendUpdate(u backend.BackendUpdate) {
	for _, s := range f.subscribers {
		s.ReceiveUpdate(u)
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

func (f *SimpleFilter) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case f.upd_chan <- upd:
	case <-f.upd_chan_stop:
	}
}

func (f *SimpleFilter) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(f)
}

func (f *SimpleFilter) GetUpdateSource() string {
	return f.source
}

func (f *SimpleFilter) GetID() string {
	return f.id
}

func (f *SimpleFilter) GetBackendList() []*backend.Backend {
	return f.backends.GetList()
}

func (f *SimpleFilter) Bind(modules module.ModulesList) {
	f.SubscribeTo(modules.GetBackendUpdateProvider(f.source))
}
