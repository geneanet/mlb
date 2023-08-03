package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

type Filter struct {
	id             string
	source         Subscribable
	subscribers    []chan BackendMessage
	tag            string
	status         string
	backends       map[string]*Backend
	backends_mutex sync.RWMutex
	log            zerolog.Logger
}

func NewFilter(id string, tag string, status string, source Subscribable, wg *sync.WaitGroup, ctx context.Context) *Filter {
	f := &Filter{
		id:          id,
		source:      source,
		subscribers: []chan BackendMessage{},
		tag:         tag,
		status:      status,
		backends:    make(map[string]*Backend),
		log:         log.With().Str("id", id).Logger(),
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
				switch msg.kind {
				case MsgBackendAdded, MsgBackendModified:
					if _, ok := f.backends[msg.address]; ok { // Modified
						if (slices.Contains(msg.backend.tags, f.tag) || f.tag == "*") && (msg.backend.status == f.status || f.status == "*") { // Still passes the filter
							f.backends[msg.address] = msg.backend.Copy()
							f.sendMessage(BackendMessage{
								kind:    MsgBackendModified,
								address: f.backends[msg.address].address,
								backend: f.backends[msg.address],
							})
						} else { // Do not pass the filter anymore
							delete(f.backends, msg.address)
							f.sendMessage(BackendMessage{
								kind:    MsgBackendRemoved,
								address: msg.address,
							})
						}
					} else { // Added
						if (slices.Contains(msg.backend.tags, f.tag) || f.tag == "*") && (msg.backend.status == f.status || f.status == "*") {
							f.backends[msg.address] = msg.backend.Copy()
							f.sendMessage(BackendMessage{
								kind:    MsgBackendAdded,
								address: f.backends[msg.address].address,
								backend: f.backends[msg.address],
							})
						}
					}
				case MsgBackendRemoved:
					// Removed
					if _, ok := f.backends[msg.address]; ok {
						delete(f.backends, msg.address)
						f.sendMessage(BackendMessage{
							kind:    MsgBackendRemoved,
							address: msg.address,
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

func (f *Filter) Subscribe() chan BackendMessage {
	ch := make(chan BackendMessage)
	f.subscribers = append(f.subscribers, ch)

	go func() {
		f.backends_mutex.RLock()
		defer f.backends_mutex.RUnlock()

		for _, backend := range f.backends {
			f.sendMessage(BackendMessage{
				kind:    MsgBackendAdded,
				address: backend.address,
				backend: backend,
			})
		}
	}()

	return ch
}

func (f *Filter) sendMessage(m BackendMessage) {
	for _, s := range f.subscribers {
		s <- m
	}
}
