package checker

import (
	"context"
	"mlb/backend"
	"mlb/misc"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type CheckerMySQL struct {
	fullname       string
	checks         map[string]*CheckerMySQLCheck
	checks_mutex   sync.RWMutex
	user           string
	password       string
	default_period time.Duration
	max_period     time.Duration
	backoff_factor float64
	source         backend.Subscribable
	subscribers    []chan backend.BackendMessage
	ctx            context.Context
	cancel         context.CancelFunc
	log            zerolog.Logger
}

type MySQLCheckerConfig struct {
	FullName      string  `hcl:"name,label"`
	Source        string  `hcl:"source"`
	User          string  `hcl:"user"`
	Password      string  `hcl:"password"`
	Period        string  `hcl:"period"`
	MaxPeriod     string  `hcl:"max_period"`
	BackoffFactor float64 `hcl:"backoff_factor"`
}

func NewCheckerMySQL(config *MySQLCheckerConfig, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) *CheckerMySQL {
	c := &CheckerMySQL{
		fullname:       config.FullName,
		checks:         make(map[string]*CheckerMySQLCheck),
		user:           config.User,
		password:       config.Password,
		backoff_factor: config.BackoffFactor,
		source:         sources[config.Source],
		log:            log.With().Str("id", config.FullName).Logger(),
	}

	var err error

	c.default_period, err = time.ParseDuration(config.Period)
	misc.PanicIfErr(err)
	c.max_period, err = time.ParseDuration(config.MaxPeriod)
	misc.PanicIfErr(err)

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("MySQL checker starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("MySQL checker stopped")
		defer c.cancel()

		msg_chan := c.source.Subscribe()
		status_chan := make(chan *backend.Backend)

	mainloop:
		for {
			select {
			case b := <-status_chan: // Backend status changed
				c.checks_mutex.Lock()
				c.sendMessage(backend.BackendMessage{
					Kind:    backend.MsgBackendModified,
					Address: b.Address,
					Backend: b,
				})
				c.checks_mutex.Unlock()

			case msg := <-msg_chan: // Backend changed
				c.checks_mutex.Lock()
				switch msg.Kind {
				case backend.MsgBackendAdded, backend.MsgBackendModified:
					if check, ok := c.checks[msg.Address]; ok { // Modified
						check.UpdateBackend(msg.Backend)
						c.sendMessage(backend.BackendMessage{
							Kind:    backend.MsgBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else { // Added
						c.log.Info().Str("address", msg.Address).Msg("Adding MySQL check")
						check := NewCheckerMySQLCheck(
							msg.Backend.Copy(),
							c.user,
							c.password,
							c.default_period,
							c.max_period,
							c.backoff_factor,
							status_chan,
						)
						err := check.StartPolling()
						if err != nil {
							c.log.Error().Str("address", msg.Address).Err(err).Msg("Error while adding MySQL check")
						} else {
							c.checks[msg.Address] = check
							c.sendMessage(backend.BackendMessage{
								Kind:    backend.MsgBackendAdded,
								Address: check.backend.Address,
								Backend: check.backend,
							})
						}
					}
				case backend.MsgBackendRemoved:
					// Removed
					if check, ok := c.checks[msg.Address]; ok {
						c.log.Info().Str("address", msg.Address).Msg("Removing MySQL check")
						check.StopPolling()
						delete(c.checks, msg.Address)
						c.sendMessage(backend.BackendMessage{
							Kind:    backend.MsgBackendRemoved,
							Address: msg.Address,
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

func (c *CheckerMySQL) Subscribe() chan backend.BackendMessage {
	ch := make(chan backend.BackendMessage)
	c.subscribers = append(c.subscribers, ch)

	go func() {
		c.checks_mutex.RLock()
		defer c.checks_mutex.RUnlock()

		for _, check := range c.checks {
			c.sendMessage(backend.BackendMessage{
				Kind:    backend.MsgBackendAdded,
				Address: check.backend.Address,
				Backend: check.backend,
			})
		}
	}()

	return ch
}

func (c *CheckerMySQL) sendMessage(m backend.BackendMessage) {
	for _, s := range c.subscribers {
		s <- m
	}
}
