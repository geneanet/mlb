package balancer

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"

	"mlb/backend"
)

type BalancerWRR struct {
	fullname     string
	backends     map[string]*backend.Backend
	weightedlist []string
	mu           sync.Mutex
	iterator     int
	log          zerolog.Logger
}

type WRRBalancerConfig struct {
	FullName string `hcl:"name,label"`
	Source   string `hcl:"source"`
}

func NewBalancerWRR(config *WRRBalancerConfig, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) *BalancerWRR {
	b := &BalancerWRR{
		fullname:     config.FullName,
		backends:     make(map[string]*backend.Backend),
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

		msg_chan := sources[config.Source].Subscribe()

	mainloop:
		for {
			select {
			case msg := <-msg_chan: // Backend changed
				b.mu.Lock()
				switch msg.Kind {
				case backend.MsgBackendAdded:
					b.log.Info().Str("address", msg.Address).Int("weight", msg.Backend.Weight).Msg("Adding backend to WRR balancer")
					b.backends[msg.Address] = msg.Backend.Copy()
					for i := 0; i < msg.Backend.Weight; i++ {
						b.weightedlist = append(b.weightedlist, msg.Address)
					}
				case backend.MsgBackendModified:
					b.log.Info().Str("address", msg.Address).Int("weight", msg.Backend.Weight).Msg("Updating backend in WRR balancer")
					b.backends[msg.Address] = msg.Backend.Copy()
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == msg.Address })
					for i := 0; i < msg.Backend.Weight; i++ {
						b.weightedlist = append(b.weightedlist, msg.Address)
					}
				case backend.MsgBackendRemoved:
					b.log.Info().Str("address", msg.Address).Msg("Removing backend from WRR balancer")
					b.weightedlist = slices.DeleteFunc(b.weightedlist, func(a string) bool { return a == msg.Address })
					delete(b.backends, msg.Address)
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

func (b *BalancerWRR) GetBackend() *backend.Backend {
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
