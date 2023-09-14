package backends_processor

import (
	"context"
	"database/sql"
	"fmt"
	"mlb/backend"
	"mlb/misc"
	"mlb/module"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	factories["mysql"] = &MySQLCheckerFactory{}
}

type MySQLChecker struct {
	id              string
	checks          map[string]*MySQLCheck
	checks_mutex    sync.RWMutex
	user            string
	password        string
	default_period  time.Duration
	max_period      time.Duration
	backoff_factor  float64
	subscribers     []chan backend.BackendUpdate
	ctx             context.Context
	cancel          context.CancelFunc
	log             zerolog.Logger
	upd_chan        chan backend.BackendUpdate
	source          string
	connect_timeout time.Duration
	read_timeout    time.Duration
	write_timeout   time.Duration
}

type MySQLCheckerConfig struct {
	ID             string  `hcl:"id,label"`
	Source         string  `hcl:"source"`
	User           string  `hcl:"user,optional"`
	Password       string  `hcl:"password,optional"`
	Period         string  `hcl:"period,optional"`
	MaxPeriod      string  `hcl:"max_period,optional"`
	BackoffFactor  float64 `hcl:"backoff_factor,optional"`
	ConnectTimeout string  `hcl:"connect_timeout,optional"`
	ReadTimeout    string  `hcl:"read_timeout,optional"`
	WriteTimeout   string  `hcl:"write_timeout,optional"`
}

type MySQLCheckerFactory struct{}

func (w MySQLCheckerFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &MySQLCheckerConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w MySQLCheckerFactory) parseConfig(tc *Config) *MySQLCheckerConfig {
	config := &MySQLCheckerConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("backends_processor.%s.%s", tc.Type, tc.Name)
	if config.Period == "" {
		config.Period = "1s"
	}
	if config.MaxPeriod == "" {
		config.MaxPeriod = "5s"
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = 1.5
	}
	if config.ConnectTimeout == "" {
		config.ConnectTimeout = "0s"
	}
	if config.ReadTimeout == "" {
		config.ReadTimeout = "0s"
	}
	if config.WriteTimeout == "" {
		config.WriteTimeout = "0s"
	}
	return config
}

func (w MySQLCheckerFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	c := &MySQLChecker{
		id:             config.ID,
		checks:         make(map[string]*MySQLCheck),
		user:           config.User,
		password:       config.Password,
		backoff_factor: config.BackoffFactor,
		log:            log.With().Str("id", config.ID).Logger(),
		upd_chan:       make(chan backend.BackendUpdate),
		source:         config.Source,
	}

	var err error

	c.default_period, err = time.ParseDuration(config.Period)
	misc.PanicIfErr(err)
	c.max_period, err = time.ParseDuration(config.MaxPeriod)
	misc.PanicIfErr(err)
	c.connect_timeout, err = time.ParseDuration(config.ConnectTimeout)
	misc.PanicIfErr(err)
	c.read_timeout, err = time.ParseDuration(config.ReadTimeout)
	misc.PanicIfErr(err)
	c.write_timeout, err = time.ParseDuration(config.WriteTimeout)
	misc.PanicIfErr(err)

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("MySQL checker starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("MySQL checker stopped")
		defer c.cancel()
		defer close(c.upd_chan)

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
						check.backend.Meta.Update(upd.Backend.Meta, "mysql")
						c.sendUpdate(backend.BackendUpdate{
							Kind:    backend.UpdBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else { // Added
						c.log.Info().Str("address", upd.Address).Msg("Adding MySQL check")
						check := NewMySQLCheck(
							upd.Backend.Clone(),
							c.user+":"+c.password+"@tcp("+upd.Address+")/?readTimeout="+c.read_timeout.String()+"&writeTimeout="+c.write_timeout.String()+"&timeout="+c.connect_timeout.String(),
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

func (c *MySQLChecker) GetID() string {
	return c.id
}

func (c *MySQLChecker) GetBackendList() []*backend.Backend {
	c.checks_mutex.RLock()
	defer c.checks_mutex.RUnlock()

	backends := []*backend.Backend{}

	for _, check := range c.checks {
		backends = append(backends, check.backend)
	}

	return backends
}

func (c *MySQLChecker) Bind(modules module.ModulesList) {
	c.SubscribeTo(modules.GetBackendUpdateProvider(c.source))
}

type MySQLCheck struct {
	backend        *backend.Backend
	dsn            string
	period         time.Duration
	default_period time.Duration
	max_period     time.Duration
	backoff_factor float64
	status_chan    chan *backend.Backend
	ticker         *time.Ticker
	stop_chan      chan bool
	running        bool
	db             *sql.DB
}

func NewMySQLCheck(backend *backend.Backend, dsn string, default_period time.Duration, max_period time.Duration, backoff_factor float64, status_chan chan *backend.Backend) *MySQLCheck {
	c := &MySQLCheck{
		backend:        backend,
		dsn:            dsn,
		period:         default_period,
		default_period: default_period,
		max_period:     max_period,
		backoff_factor: backoff_factor,
		status_chan:    status_chan,
		stop_chan:      make(chan bool),
		running:        false,
	}
	backend.Meta.Set("mysql", "status", cty.UnknownVal(cty.String))
	backend.Meta.Set("mysql", "readonly", cty.UnknownVal(cty.Bool))
	return c
}

func (c *MySQLCheck) fetchStatus() (ret_status cty.Value, ret_readonly cty.Value, ret_err error) {
	defer func() {
		if r := recover(); r != nil {
			ret_status = cty.StringVal("err")
			ret_readonly = cty.BoolVal(false)
			ret_err = misc.EnsureError(r)

			c.applyBackoff()
		}
	}()

	log.Trace().Str("address", c.backend.Address).Msg("Probing Backend")

	result, err := c.db.Query("SELECT @@read_only")
	misc.PanicIfErr(err)
	defer result.Close()

	var read_only bool

	result.Next()
	err = result.Scan(&read_only)
	misc.PanicIfErr(err)

	c.resetPeriod()

	return cty.StringVal("ok"), cty.BoolVal(read_only), nil
}

func (c *MySQLCheck) updateStatus() {
	new_status, new_readonly, err := c.fetchStatus()

	if err != nil {
		log.Error().Str("address", c.backend.Address).Err(err).Msg("Error while fetching status from backend")
	}

	old_status, ok := c.backend.Meta.Get("mysql", "status")
	if !ok || !old_status.IsKnown() || old_status.Equals(new_status).False() {
		c.backend.Meta.Set("mysql", "status", new_status)

		if !old_status.IsKnown() {
			log.Info().Str("address", c.backend.Address).Str("new_status", new_status.AsString()).Msg("Backend status changed")
		} else {
			log.Info().Str("address", c.backend.Address).Str("old_status", old_status.AsString()).Str("new_status", new_status.AsString()).Msg("Backend status changed")
		}

		c.status_chan <- c.backend
	}

	old_readonly, ok := c.backend.Meta.Get("mysql", "readonly")
	if !ok || !old_readonly.IsKnown() || old_readonly.Equals(new_readonly).False() {
		c.backend.Meta.Set("mysql", "readonly", new_readonly)

		if !old_readonly.IsKnown() {
			log.Info().Str("address", c.backend.Address).Bool("new_readonly", new_readonly.True()).Msg("Backend readonly changed")
		} else {
			log.Info().Str("address", c.backend.Address).Bool("old_readonly", old_readonly.True()).Bool("new_readonly", new_readonly.True()).Msg("Backend readonly changed")
		}

		c.status_chan <- c.backend
	}

}

func (c *MySQLCheck) StartPolling() error {
	if c.running {
		return nil
	}
	c.running = true

	db, err := sql.Open("mysql", c.dsn)
	if err != nil {
		return err
	}
	c.db = db

	c.period = c.default_period
	c.ticker = time.NewTicker(c.period)

	go func() {
		defer func() { c.running = false }()

		for {
			c.updateStatus()

			// Wait next iteration
			select {
			case <-c.stop_chan:
				return
			case <-c.ticker.C:
			}
		}
	}()

	return nil
}

func (c *MySQLCheck) StopPolling() {
	if !c.running {
		return
	}

	c.db.Close()
	c.ticker.Stop()
	c.stop_chan <- true
}

func (c *MySQLCheck) updatePeriod(period time.Duration) {
	if c.running && (c.period != period) {
		c.period = period
		c.ticker.Reset(c.period)

		log.Warn().Dur("period", c.period).Str("address", c.backend.Address).Msg("Updating Backend probing period")
	}
}

func (c *MySQLCheck) resetPeriod() {
	c.updatePeriod(c.default_period)
}

func (c *MySQLCheck) applyBackoff() {
	new_period := time.Duration(float64(c.period) * c.backoff_factor)
	if new_period > c.max_period {
		new_period = c.max_period
	}
	c.updatePeriod(new_period)
}
