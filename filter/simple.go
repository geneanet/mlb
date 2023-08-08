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
	"golang.org/x/exp/slices"
)

func init() {
	factories["simple"] = &SimpleFilterFactory{}
}

type SimpleFilter struct {
	fullname       string
	source         backend.Subscribable
	subscribers    []chan backend.BackendMessage
	include_tags   []string
	exclude_tags   []string
	status         string
	meta           []MetaConditionConfig
	backends       map[string]*backend.Backend
	backends_mutex sync.RWMutex
	log            zerolog.Logger
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

func (w SimpleFilterFactory) New(tc *Config, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable {
	config := w.parseConfig(tc)

	f := &SimpleFilter{
		fullname:     config.FullName,
		source:       sources[config.Source],
		subscribers:  []chan backend.BackendMessage{},
		include_tags: config.IncludeTags,
		exclude_tags: config.ExcludeTags,
		status:       config.Status,
		meta:         config.Meta,
		backends:     make(map[string]*backend.Backend),
		log:          log.With().Str("id", config.FullName).Logger(),
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

		msg_chan := f.source.Subscribe()

	mainloop:
		for {
			select {
			case msg := <-msg_chan: // Backend changed
				f.backends_mutex.Lock()
				switch msg.Kind {
				case backend.MsgBackendAdded, backend.MsgBackendModified:
					if _, ok := f.backends[msg.Address]; ok { // Modified
						if f.matchFilter(msg.Backend) { // Still passes the filter
							f.backends[msg.Address] = msg.Backend.Copy()
							f.sendMessage(backend.BackendMessage{
								Kind:    backend.MsgBackendModified,
								Address: f.backends[msg.Address].Address,
								Backend: f.backends[msg.Address],
							})
						} else { // Do not pass the filter anymore
							delete(f.backends, msg.Address)
							f.sendMessage(backend.BackendMessage{
								Kind:    backend.MsgBackendRemoved,
								Address: msg.Address,
							})
						}
					} else { // Added
						if f.matchFilter(msg.Backend) {
							f.backends[msg.Address] = msg.Backend.Copy()
							f.sendMessage(backend.BackendMessage{
								Kind:    backend.MsgBackendAdded,
								Address: f.backends[msg.Address].Address,
								Backend: f.backends[msg.Address],
							})
						}
					}
				case backend.MsgBackendRemoved:
					// Removed
					if _, ok := f.backends[msg.Address]; ok {
						delete(f.backends, msg.Address)
						f.sendMessage(backend.BackendMessage{
							Kind:    backend.MsgBackendRemoved,
							Address: msg.Address,
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

func (f *SimpleFilter) Subscribe() chan backend.BackendMessage {
	ch := make(chan backend.BackendMessage)
	f.subscribers = append(f.subscribers, ch)

	go func() {
		f.backends_mutex.RLock()
		defer f.backends_mutex.RUnlock()

		for _, b := range f.backends {
			f.sendMessage(backend.BackendMessage{
				Kind:    backend.MsgBackendAdded,
				Address: b.Address,
				Backend: b,
			})
		}
	}()

	return ch
}

func (f *SimpleFilter) sendMessage(m backend.BackendMessage) {
	for _, s := range f.subscribers {
		s <- m
	}
}

func (f *SimpleFilter) matchFilter(b *backend.Backend) bool {
	if !(b.Status == f.status || f.status == "*") {
		return false
	}

	for _, t := range f.include_tags {
		if !slices.Contains(b.Tags, t) {
			return false
		}
	}

	for _, t := range f.exclude_tags {
		if slices.Contains(b.Tags, t) {
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
