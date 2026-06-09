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
	id            string
	subscribers   []backend.BackendUpdateSubscriber
	subscribersMutex sync.RWMutex
	backends      *backend.BackendsMap
	backendsMutex sync.RWMutex
	log           zerolog.Logger
	updChan       chan backend.BackendUpdate
	updChanStop   chan struct{}
	source        string
	condition     hcl.Expression
	evalCtx       *hcl.EvalContext
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
		id:          config.ID,
		subscribers: []backend.BackendUpdateSubscriber{},
		backends:    backend.NewBackendsMap(),
		log:         log.With().Str("id", config.ID).Logger(),
		updChan:     make(chan backend.BackendUpdate, 100),
		updChanStop: make(chan struct{}),
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
		defer close(f.updChanStop)

	mainloop:
		for {
			select {
			case upd := <-f.updChan: // Backend changed
				f.backendsMutex.Lock()
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
				f.backendsMutex.Unlock()
			case <-ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return f
}

func (f *SimpleFilter) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	f.subscribersMutex.Lock()
	f.subscribers = append(f.subscribers, s)
	f.subscribersMutex.Unlock()

	f.backendsMutex.RLock()
	backends := f.backends.GetList()
	f.backendsMutex.RUnlock()

	for _, b := range backends {
		s.ReceiveUpdate(backend.BackendUpdate{
			Kind:    backend.UpdBackendAdded,
			Address: b.Address,
			Backend: b,
		})
	}
}

func (f *SimpleFilter) sendUpdate(u backend.BackendUpdate) {
	f.subscribersMutex.RLock()
	subscribers := make([]backend.BackendUpdateSubscriber, len(f.subscribers))
	copy(subscribers, f.subscribers)
	f.subscribersMutex.RUnlock()

	for _, s := range subscribers {
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
	case f.updChan <- upd:
	case <-f.updChanStop:
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
