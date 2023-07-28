package main

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
)

// MySQL Backends
type BackendDirectory struct {
	backends       map[string]*Backend
	user           string
	password       string
	default_period float64
	max_period     float64
	backoff_factor float64
	running        bool
	msg_chan       chan consulMessage
	mu             sync.Mutex
}

func newBackendDirectory(user string, password string, default_period float64, max_period float64, backoff_factor float64, msg_chan chan consulMessage) BackendDirectory {
	return BackendDirectory{
		backends:       make(map[string]*Backend),
		user:           user,
		password:       password,
		default_period: default_period,
		max_period:     max_period,
		backoff_factor: backoff_factor,
		msg_chan:       msg_chan,
		running:        false,
	}
}

func (bd *BackendDirectory) start(wg *sync.WaitGroup) {
	if bd.running {
		return
	}

	bd.running = true
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer func() { bd.running = false }()

		for {
			msg := <-bd.msg_chan

			bd.mu.Lock()
			switch msg.kind {
			case MsgServiceAdded, MsgServiceModified:
				if backend, ok := bd.backends[msg.id]; ok { // Modified

					log.Info().Str("id", msg.id).Msg("Updating backend")
					backend.weight = msg.service.Service.Weights.Passing
					backend.tags = msg.service.Service.Tags
				} else { // Added
					log.Info().Str("id", msg.id).Msg("Adding backend")
					backend := newBackend(
						msg.service.Service.Address,
						msg.service.Service.Port,
						msg.service.Service.Weights.Passing,
						msg.service.Service.Tags,
						bd.user,
						bd.password,
						bd.default_period,
						bd.max_period,
						bd.backoff_factor,
					)
					err := backend.startPolling()
					if err != nil {
						log.Error().Str("id", msg.id).Err(err).Msg("Error while adding backend")
					} else {
						bd.backends[msg.id] = backend
					}
				}
			case MsgServiceRemoved:
				// Removed
				if backend, ok := bd.backends[msg.id]; ok {
					log.Info().Str("id", msg.id).Msg("Removing backend")
					backend.stopPolling()
					delete(bd.backends, msg.id)
				}
			}
			bd.mu.Unlock()
		}
	}()
}

func (bd *BackendDirectory) getBackend(tag string, status string) (string, error) {
	bd.mu.Lock()
	defer bd.mu.Unlock()

	// TODO: More efficient WRR !
	weighted_ids := make([]string, 0, 1024)
	for id, backend := range bd.backends {
		if slices.Contains(backend.tags, tag) && (backend.status == status || status == "all") {
			for i := 0; i < backend.weight; i++ {
				weighted_ids = append(weighted_ids, id)
			}
		}
	}

	if len(weighted_ids) > 0 {
		return weighted_ids[rand.Intn(len(weighted_ids))], nil
	} else {
		return "", fmt.Errorf("no backend found for tag %s + status %s", tag, status)
	}
}
