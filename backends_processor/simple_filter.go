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
	id          string
	backends    *backend.Registry
	log         zerolog.Logger
	updChan     chan backend.BackendUpdate
	updChanStop chan struct{}
	source      string
	condition   hcl.Expression
	evalCtx     *hcl.EvalContext
}

type SimpleFilterConfig struct {
	ID        string         `hcl:"id,label"`
	Source    string         `hcl:"source"`
	Condition hcl.Expression `hcl:"condition"`
}

type SimpleFilterFactory struct{}

func (w SimpleFilterFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &SimpleFilterConfig{}
	return gohcl.DecodeBody(tc.Config, tc.Ctx, config)
}

func (w SimpleFilterFactory) parseConfig(tc *Config) *SimpleFilterConfig {
	config := &SimpleFilterConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode simple filter backend processor config")
	}
	config.ID = fmt.Sprintf("backends_processor.%s.%s", tc.Type, tc.Name)
	return config
}

func (w SimpleFilterFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	f := &SimpleFilter{
		id:          config.ID,
		backends:    backend.NewRegistry(),
		log:         log.With().Str("id", config.ID).Logger(),
		updChan:     make(chan backend.BackendUpdate, 100),
		updChanStop: make(chan struct{}),
		source:      config.Source,
		condition:   config.Condition,
		evalCtx:     tc.Ctx,
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
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if f.backends.Has(upd.Address) { // Modified
						if f.matchFilter(upd.Backend) { // Still passes the filter
							f.backends.Update(upd.Backend.Clone())
							f.backends.Publish(backend.BackendUpdate{
								Kind:    backend.UpdBackendModified,
								Address: upd.Address,
								Backend: f.backends.Get(upd.Address),
							})
						} else { // Do not pass the filter anymore
							f.backends.Remove(upd.Address)
							f.backends.Publish(backend.BackendUpdate{
								Kind:    backend.UpdBackendRemoved,
								Address: upd.Address,
							})
						}
					} else { // Added
						if f.matchFilter(upd.Backend) {
							f.backends.Add(upd.Backend.Clone())
							f.backends.Publish(backend.BackendUpdate{
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
						f.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
						})
					}
				}
			case <-ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return f
}

func (f *SimpleFilter) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	f.backends.ProvideUpdates(s)
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
	f.SubscribeTo(module.Get[backend.BackendUpdateProvider](modules, f.source))
}
