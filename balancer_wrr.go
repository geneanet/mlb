package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

type BalancerWRR struct {
	id           string
	backends     map[string]*Backend
	weightedlist []string
	mu           sync.Mutex
	iterator     int
	log          zerolog.Logger
}

func NewBalancerWRR(id string, source Subscribable, wg *sync.WaitGroup, ctx context.Context) *BalancerWRR {
	b := &BalancerWRR{
		id:           id,
		backends:     make(map[string]*Backend),
		weightedlist: make([]string, 0),
		iterator:     0,
		log:          log.With().Str("id", id).Logger(),
	}

	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	b.log.Info().Msg("WRR Balancer starting")

	go func() {
		defer wg.Done()
		defer b.log.Info().Msg("WRR Balancer stopped")
		defer cancel()

		msg_chan := source.Subscribe()

	mainloop:
		for {
			select {
			case msg := <-msg_chan: // Backend changed
				b.mu.Lock()
				switch msg.kind {
				case MsgBackendAdded:
					b.log.Info().Str("address", msg.address).Int("weight", msg.backend.weight).Msg("Adding backend to WRR balancer")
					b.backends[msg.address] = msg.backend.Copy()
					for i := 0; i < msg.backend.weight; i++ {
						b.weightedlist = append(b.weightedlist, msg.address)
					}
				case MsgBackendModified:
					b.log.Info().Str("address", msg.address).Int("weight", msg.backend.weight).Msg("Updating backend in WRR balancer")
					b.backends[msg.address] = msg.backend.Copy()
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == msg.address })
					for i := 0; i < msg.backend.weight; i++ {
						b.weightedlist = append(b.weightedlist, msg.address)
					}
				case MsgBackendRemoved:
					b.log.Info().Str("address", msg.address).Msg("Removing backend from WRR balancer")
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == msg.address })
					delete(b.backends, msg.address)
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

func (b *BalancerWRR) GetBackend() *Backend {
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
