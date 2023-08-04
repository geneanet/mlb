package filter

import (
	"context"
	"mlb/backend"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

type SimpleFilter struct {
	fullname       string
	source         backend.Subscribable
	subscribers    []chan backend.BackendMessage
	tag            string
	status         string
	backends       map[string]*backend.Backend
	backends_mutex sync.RWMutex
	log            zerolog.Logger
}

type SimpleFilterConfig struct {
	FullName string `hcl:"name,label"`
	Source   string `hcl:"source"`
	Tag      string `hcl:"tag"`
	Status   string `hcl:"status"`
}

func NewSimpleFilter(config *SimpleFilterConfig, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) *SimpleFilter {
	f := &SimpleFilter{
		fullname:    config.FullName,
		source:      sources[config.Source],
		subscribers: []chan backend.BackendMessage{},
		tag:         config.Tag,
		status:      config.Status,
		backends:    make(map[string]*backend.Backend),
		log:         log.With().Str("id", config.FullName).Logger(),
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
						if (slices.Contains(msg.Backend.Tags, f.tag) || f.tag == "*") && (msg.Backend.Status == f.status || f.status == "*") { // Still passes the filter
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
						if (slices.Contains(msg.Backend.Tags, f.tag) || f.tag == "*") && (msg.Backend.Status == f.status || f.status == "*") {
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
