package main

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type CheckerMySQL struct {
	id             string
	checks         map[string]*CheckerMySQLCheck
	checks_mutex   sync.RWMutex
	user           string
	password       string
	default_period float64
	max_period     float64
	backoff_factor float64
	source         Subscribable
	subscribers    []chan BackendMessage
	ctx            context.Context
	cancel         context.CancelFunc
	log            zerolog.Logger
}

func NewCheckerMySQL(id string, user string, password string, default_period float64, max_period float64, backoff_factor float64, source Subscribable, wg *sync.WaitGroup, ctx context.Context) *CheckerMySQL {
	c := &CheckerMySQL{
		id:             id,
		checks:         make(map[string]*CheckerMySQLCheck),
		user:           user,
		password:       password,
		default_period: default_period,
		max_period:     max_period,
		backoff_factor: backoff_factor,
		source:         source,
		log:            log.With().Str("id", id).Logger(),
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("MySQL checker starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("MySQL checker stopped")
		defer c.cancel()

		msg_chan := c.source.Subscribe()
		status_chan := make(chan *Backend)

	mainloop:
		for {
			select {
			case backend := <-status_chan: // Backend status changed
				c.checks_mutex.Lock()
				c.sendMessage(BackendMessage{
					kind:    MsgBackendModified,
					address: backend.address,
					backend: backend,
				})
				c.checks_mutex.Unlock()

			case msg := <-msg_chan: // Backend changed
				c.checks_mutex.Lock()
				switch msg.kind {
				case MsgBackendAdded, MsgBackendModified:
					if check, ok := c.checks[msg.address]; ok { // Modified
						check.UpdateBackend(msg.backend)
						c.sendMessage(BackendMessage{
							kind:    MsgBackendModified,
							address: check.backend.address,
							backend: check.backend,
						})
					} else { // Added
						c.log.Info().Str("address", msg.address).Msg("Adding MySQL check")
						check := NewCheckerMySQLCheck(
							msg.backend.Copy(),
							c.user,
							c.password,
							c.default_period,
							c.max_period,
							c.backoff_factor,
							status_chan,
						)
						err := check.StartPolling()
						if err != nil {
							c.log.Error().Str("address", msg.address).Err(err).Msg("Error while adding MySQL check")
						} else {
							c.checks[msg.address] = check
							c.sendMessage(BackendMessage{
								kind:    MsgBackendAdded,
								address: check.backend.address,
								backend: check.backend,
							})
						}
					}
				case MsgBackendRemoved:
					// Removed
					if check, ok := c.checks[msg.address]; ok {
						c.log.Info().Str("address", msg.address).Msg("Removing MySQL check")
						check.StopPolling()
						delete(c.checks, msg.address)
						c.sendMessage(BackendMessage{
							kind:    MsgBackendRemoved,
							address: msg.address,
						})
					}
				}
				c.checks_mutex.Unlock()

			case <-c.ctx.Done(): // Context cancelled
				// Stop backends
				for _, backend := range c.checks {
					backend.StopPolling()
				}
				break mainloop
			}

		}
	}()

	return c
}

func (c *CheckerMySQL) Subscribe() chan BackendMessage {
	ch := make(chan BackendMessage)
	c.subscribers = append(c.subscribers, ch)

	go func() {
		c.checks_mutex.RLock()
		defer c.checks_mutex.RUnlock()

		for _, check := range c.checks {
			c.sendMessage(BackendMessage{
				kind:    MsgBackendAdded,
				address: check.backend.address,
				backend: check.backend,
			})
		}
	}()

	return ch
}

func (c *CheckerMySQL) sendMessage(m BackendMessage) {
	for _, s := range c.subscribers {
		s <- m
	}
}
