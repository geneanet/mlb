package filter

import (
	"context"
	"fmt"
	"mlb/backend"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	factories["simple"] = &SimpleFilterFactory{}
}

type SimpleFilter struct {
	fullname       string
	subscribers    []chan backend.BackendUpdate
	include_tags   []string
	exclude_tags   []string
	status         string
	meta           []MetaConditionConfig
	backends       backend.BackendsMap
	backends_mutex sync.RWMutex
	log            zerolog.Logger
	upd_chan       chan backend.BackendUpdate
	source         string
}

type SimpleFilterConfig struct {
	FullName    string                `hcl:"name,label"`
	Source      string                `hcl:"source"`
	IncludeTags []string              `hcl:"include_tags,optional"`
	ExcludeTags []string              `hcl:"exclude_tags,optional"`
	Status      string                `hcl:"status,optional"`
	Meta        []MetaConditionConfig `hcl:"meta,block"`
}

type MetaConditionConfig struct {
	Key   string `hcl:"key"`
	Value string `hcl:"value"`
}

type SimpleFilterFactory struct{}

func (w SimpleFilterFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &SimpleFilterConfig{}
	return gohcl.DecodeBody(tc.Config, nil, config)
}

func (w SimpleFilterFactory) parseConfig(tc *Config) *SimpleFilterConfig {
	config := &SimpleFilterConfig{}
	gohcl.DecodeBody(tc.Config, nil, config)
	config.FullName = fmt.Sprintf("filter.%s.%s", tc.Type, tc.Name)
	return config
}

func (w SimpleFilterFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) backend.BackendUpdateProvider {
	config := w.parseConfig(tc)

	f := &SimpleFilter{
		fullname:     config.FullName,
		subscribers:  []chan backend.BackendUpdate{},
		include_tags: config.IncludeTags,
		exclude_tags: config.ExcludeTags,
		status:       config.Status,
		meta:         config.Meta,
		backends:     make(backend.BackendsMap),
		log:          log.With().Str("id", config.FullName).Logger(),
		upd_chan:     make(chan backend.BackendUpdate),
		source:       config.Source,
	}

	if f.status == "" {
		f.status = "ok"
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
	if !(b.Status == f.status || f.status == "*") {
		return false
	}

	for _, t := range f.include_tags {
		if !b.Tags.Has(t) {
			return false
		}
	}

	for _, t := range f.exclude_tags {
		if b.Tags.Has(t) {
			return false
		}
	}

	for _, m := range f.meta { // Check each requested metadata
		v, ok := b.Meta[m.Key]
		if !ok { // If the metadata is not available
			return false
		}
		sv, err := v.ToString()
		if sv != m.Value || err != nil { // If the metadata do not match
			return false
		}
	}

	return true
}

func (f *SimpleFilter) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(f.upd_chan)
}

func (f *SimpleFilter) GetUpdateSource() string {
	return f.source
}
