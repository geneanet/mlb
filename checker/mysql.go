package checker

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/misc"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	factories["mysql"] = &MySQLCheckerFactory{}
}

type MySQLChecker struct {
	fullname       string
	checks         map[string]*CheckerMySQLCheck
	checks_mutex   sync.RWMutex
	user           string
	password       string
	default_period time.Duration
	max_period     time.Duration
	backoff_factor float64
	subscribers    []chan backend.BackendUpdate
	ctx            context.Context
	cancel         context.CancelFunc
	log            zerolog.Logger
	upd_chan       chan backend.BackendUpdate
	source         string
}

type MySQLCheckerConfig struct {
	FullName      string  `hcl:"name,label"`
	Source        string  `hcl:"source"`
	User          string  `hcl:"user,optional"`
	Password      string  `hcl:"password,optional"`
	Period        string  `hcl:"period,optional"`
	MaxPeriod     string  `hcl:"max_period,optional"`
	BackoffFactor float64 `hcl:"backoff_factor,optional"`
}

type MySQLCheckerFactory struct{}

func (w MySQLCheckerFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &MySQLCheckerConfig{}
	return gohcl.DecodeBody(tc.Config, nil, config)
}

func (w MySQLCheckerFactory) parseConfig(tc *Config) *MySQLCheckerConfig {
	config := &MySQLCheckerConfig{}
	gohcl.DecodeBody(tc.Config, nil, config)
	config.FullName = fmt.Sprintf("checker.%s.%s", tc.Type, tc.Name)
	if config.Period == "" {
		config.Period = "500ms"
	}
	if config.MaxPeriod == "" {
		config.MaxPeriod = "2s"
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = 1.5
	}
	return config
}

func (w MySQLCheckerFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) backend.BackendUpdateProvider {
	config := w.parseConfig(tc)

	c := &MySQLChecker{
		fullname:       config.FullName,
		checks:         make(map[string]*CheckerMySQLCheck),
		user:           config.User,
		password:       config.Password,
		backoff_factor: config.BackoffFactor,
		log:            log.With().Str("id", config.FullName).Logger(),
		upd_chan:       make(chan backend.BackendUpdate),
		source:         config.Source,
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

		status_chan := make(chan *backend.Backend)

	mainloop:
		for {
			select {
			case b := <-status_chan: // Backend status changed
				c.checks_mutex.Lock()
				c.sendUpdate(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: b.Address,
					Backend: b,
				})
				c.checks_mutex.Unlock()

			case upd := <-c.upd_chan: // Backend changed
				c.checks_mutex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if check, ok := c.checks[upd.Address]; ok { // Modified
						check.UpdateBackend(upd.Backend)
						c.sendUpdate(backend.BackendUpdate{
							Kind:    backend.UpdBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else { // Added
						c.log.Info().Str("address", upd.Address).Msg("Adding MySQL check")
						check := NewCheckerMySQLCheck(
							upd.Backend.Clone(),
							c.user,
							c.password,
							c.default_period,
							c.max_period,
							c.backoff_factor,
							status_chan,
						)
						err := check.StartPolling()
						if err != nil {
							c.log.Error().Str("address", upd.Address).Err(err).Msg("Error while adding MySQL check")
						} else {
							c.checks[upd.Address] = check
							c.sendUpdate(backend.BackendUpdate{
								Kind:    backend.UpdBackendAdded,
								Address: check.backend.Address,
								Backend: check.backend,
							})
						}
					}
				case backend.UpdBackendRemoved:
					// Removed
					if check, ok := c.checks[upd.Address]; ok {
						c.log.Info().Str("address", upd.Address).Msg("Removing MySQL check")
						check.StopPolling()
						delete(c.checks, upd.Address)
						c.sendUpdate(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
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

func (c *MySQLChecker) ProvideUpdates(ch chan backend.BackendUpdate) {
	c.subscribers = append(c.subscribers, ch)

	go func() {
		c.checks_mutex.RLock()
		defer c.checks_mutex.RUnlock()

		for _, check := range c.checks {
			c.sendUpdate(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: check.backend.Address,
				Backend: check.backend,
			})
		}
	}()
}

func (c *MySQLChecker) sendUpdate(m backend.BackendUpdate) {
	for _, s := range c.subscribers {
		s <- m
	}
}

func (c *MySQLChecker) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(c.upd_chan)
}

func (c *MySQLChecker) GetUpdateSource() string {
	return c.source
}
