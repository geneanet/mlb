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
	"github.com/zclconf/go-cty/cty/gocty"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	factories["mysql"] = &MySQLCheckerFactory{}
}

type MySQLChecker struct {
	id             string
	checks         map[string]*MySQLCheck
	checksMtex     sync.RWMutex
	user           string
	password       string
	defaultPeriod  time.Duration
	maxPeriod      time.Duration
	backoffFactor  float64
	subscribers    []backend.BackendUpdateSubscriber
	ctx            context.Context
	cancel         context.CancelFunc
	log            zerolog.Logger
	updChan        chan backend.BackendUpdate
	updChanStop    chan struct{}
	source         string
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
	checkReplica   bool
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
	CheckReplica   bool    `hcl:"check_replica,optional"`
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
		id:            config.ID,
		checks:        make(map[string]*MySQLCheck),
		user:          config.User,
		password:      config.Password,
		backoffFactor: config.BackoffFactor,
		subscribers:   []backend.BackendUpdateSubscriber{},
		log:           log.With().Str("id", config.ID).Logger(),
		updChan:       make(chan backend.BackendUpdate),
		updChanStop:   make(chan struct{}),
		source:        config.Source,
		checkReplica:  config.CheckReplica,
	}

	var err error

	c.defaultPeriod, err = time.ParseDuration(config.Period)
	misc.PanicIfErr(err)
	c.maxPeriod, err = time.ParseDuration(config.MaxPeriod)
	misc.PanicIfErr(err)
	c.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	misc.PanicIfErr(err)
	c.readTimeout, err = time.ParseDuration(config.ReadTimeout)
	misc.PanicIfErr(err)
	c.writeTimeout, err = time.ParseDuration(config.WriteTimeout)
	misc.PanicIfErr(err)

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("MySQL checker starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("MySQL checker stopped")
		defer c.cancel()
		defer close(c.updChanStop)
		defer c.stopChecks()

		statusChan := make(chan *backend.Backend)

		for {
			select {
			case b := <-statusChan: // Backend status changed
				c.checksMtex.Lock()
				c.sendUpdate(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: b.Address,
					Backend: b,
				})
				c.checksMtex.Unlock()

			case upd := <-c.updChan: // Backend changed
				c.checksMtex.Lock()
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
							c.user+":"+c.password+"@tcp("+upd.Address+")/?readTimeout="+c.readTimeout.String()+"&writeTimeout="+c.writeTimeout.String()+"&timeout="+c.connectTimeout.String(),
							c.defaultPeriod,
							c.maxPeriod,
							c.backoffFactor,
							statusChan,
							c.checkReplica,
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
				c.checksMtex.Unlock()

			case <-c.ctx.Done(): // Context cancelled
				return
			}
		}
	}()

	return c
}

func (c *MySQLChecker) stopChecks() {
	// Stop backend checks
	for _, backend := range c.checks {
		backend.StopPolling()
	}
}

func (c *MySQLChecker) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.subscribers = append(c.subscribers, s)

	go func() {
		c.checksMtex.RLock()
		defer c.checksMtex.RUnlock()

		for _, check := range c.checks {
			c.sendUpdate(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: check.backend.Address,
				Backend: check.backend,
			})
		}
	}()
}

func (c *MySQLChecker) sendUpdate(u backend.BackendUpdate) {
	for _, s := range c.subscribers {
		s.ReceiveUpdate(u)
	}
}

func (c *MySQLChecker) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case c.updChan <- upd:
	case <-c.updChanStop:
	}
}

func (c *MySQLChecker) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(c)
}

func (c *MySQLChecker) GetUpdateSource() string {
	return c.source
}

func (c *MySQLChecker) GetID() string {
	return c.id
}

func (c *MySQLChecker) GetBackendList() []*backend.Backend {
	c.checksMtex.RLock()
	defer c.checksMtex.RUnlock()

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
	backend       *backend.Backend
	dsn           string
	period        time.Duration
	defaultPeriod time.Duration
	maxPeriod     time.Duration
	backoffFactor float64
	statusChan    chan *backend.Backend
	ticker        *misc.ExponentialBackoffTicker
	stopChan      chan struct{}
	running       bool
	db            *sql.DB
	checkReplica  bool
}

func NewMySQLCheck(backend *backend.Backend, dsn string, defaultPeriod time.Duration, maxPeriod time.Duration, backoffFactor float64, statusChan chan *backend.Backend, checkReplica bool) *MySQLCheck {
	c := &MySQLCheck{
		backend:       backend,
		dsn:           dsn,
		period:        defaultPeriod,
		defaultPeriod: defaultPeriod,
		maxPeriod:     maxPeriod,
		backoffFactor: backoffFactor,
		statusChan:    statusChan,
		stopChan:      make(chan struct{}),
		running:       false,
		checkReplica:  checkReplica,
	}
	backend.Meta.Set("mysql", "status", cty.UnknownVal(cty.String))
	backend.Meta.Set("mysql", "readonly", cty.UnknownVal(cty.Bool))
	if c.checkReplica {
		backend.Meta.Set("mysql", "replica_latency", cty.UnknownVal(cty.Number))
		backend.Meta.Set("mysql", "replica_running", cty.UnknownVal(cty.Bool))
	}
	return c
}

func (c *MySQLCheck) fetchReadOnly() (retReadonly cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retReadonly = cty.BoolVal(false)
			retErr = misc.EnsureError(r)
		}
	}()

	var readOnly bool

	// Execute query with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.defaultPeriod)
	defer cancel()
	result, err := c.db.QueryContext(ctx, "SELECT @@read_only")
	misc.PanicIfErr(err)
	defer result.Close()

	// Fetch row
	result.Next()
	err = result.Scan(&readOnly)
	misc.PanicIfErr(err)

	return cty.BoolVal(readOnly), nil
}

func (c *MySQLCheck) fetchReplicaLatency() (retReplicaLatency cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retReplicaLatency = cty.NumberIntVal(-1)
			retErr = misc.EnsureError(r)
		}
	}()

	// Default value -1 if replication is not running
	var replicationLatency int64 = -1

	// Execute query with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.defaultPeriod)
	defer cancel()
	result, err := c.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	misc.PanicIfErr(err)
	defer result.Close()

	// If we have a row
	if result.Next() {
		// Find the column index for Seconds_Behind_Source
		columns, err := result.Columns()
		misc.PanicIfErr(err)
		sbsColumn := -1
		for i := range columns {
			if columns[i] == "Seconds_Behind_Source" {
				sbsColumn = i
				break
			}
		}
		if sbsColumn == -1 {
			panic("Column Seconds_Behind_Source not found in SHOW REPLICA STATUS")
		}

		// Create the buffer and scan the row
		var sbsValue sql.NullInt64
		values := make([]interface{}, len(columns))
		for i := range columns {
			if i == sbsColumn {
				values[i] = &sbsValue
			} else {
				values[i] = new(sql.RawBytes)
			}
		}
		err = result.Scan(values...)
		misc.PanicIfErr(err)

		// Get the value if not null
		if sbsValue.Valid {
			replicationLatency = int64(sbsValue.Int64)
		}
	}

	return cty.NumberIntVal(replicationLatency), nil
}

func (c *MySQLCheck) fetchStatus() (retStatus cty.Value, retReadonly cty.Value, retReplicaLatency cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retStatus = cty.StringVal("err")
			retReadonly = cty.BoolVal(false)
			retReplicaLatency = cty.NumberIntVal(-1)
			retErr = misc.EnsureError(r)

			// Close and reopen MySQL connection to ensure we start on a good base next time
			log.Info().Str("address", c.backend.Address).Msg("Reopening MySQL connection")
			db, err := sql.Open("mysql", c.dsn)
			if err != nil {
				log.Warn().Str("address", c.backend.Address).Err(err).Msg("Error while reopening MySQL connection")
			}
			c.db = db

			// Increase fetch period
			if period, updated := c.ticker.ApplyBackoff(); updated {
				log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
			}
		}
	}()

	log.Trace().Str("address", c.backend.Address).Msg("Probing Backend")

	// Read Only
	readOnly, err := c.fetchReadOnly()
	misc.PanicIfErr(err)

	// Replica Latency
	var replicaLatency cty.Value = cty.UnknownVal(cty.Bool)
	if c.checkReplica {
		replicaLatency, err = c.fetchReplicaLatency()
		misc.PanicIfErr(err)
	}

	// If everything went OK, reset the fetch period if needed
	if period, updated := c.ticker.Reset(); updated {
		log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
	}

	return cty.StringVal("ok"), readOnly, replicaLatency, nil
}

func (c *MySQLCheck) updateStatus() {
	newStatus, newReadonly, newReplicaLatency, err := c.fetchStatus()

	if err != nil {
		log.Error().Str("address", c.backend.Address).Err(err).Msg("Error while fetching status from backend")
	}

	oldStatus, ok := c.backend.Meta.Get("mysql", "status")
	if !ok || !oldStatus.IsKnown() || oldStatus.Equals(newStatus).False() {
		c.backend.Meta.Set("mysql", "status", newStatus)

		if !oldStatus.IsKnown() {
			log.Info().Str("address", c.backend.Address).Str("newStatus", newStatus.AsString()).Msg("Backend status changed")
		} else {
			log.Info().Str("address", c.backend.Address).Str("oldStatus", oldStatus.AsString()).Str("newStatus", newStatus.AsString()).Msg("Backend status changed")
		}

		c.statusChan <- c.backend
	}

	oldReadonly, ok := c.backend.Meta.Get("mysql", "readonly")
	if !ok || !oldReadonly.IsKnown() || oldReadonly.Equals(newReadonly).False() {
		c.backend.Meta.Set("mysql", "readonly", newReadonly)

		if !oldReadonly.IsKnown() {
			log.Info().Str("address", c.backend.Address).Bool("newReadonly", newReadonly.True()).Msg("Backend readonly changed")
		} else {
			log.Info().Str("address", c.backend.Address).Bool("oldReadonly", oldReadonly.True()).Bool("newReadonly", newReadonly.True()).Msg("Backend readonly changed")
		}

		c.statusChan <- c.backend
	}

	if c.checkReplica {
		oldReplicaLatency, ok := c.backend.Meta.Get("mysql", "replica_latency")
		if !ok || !oldReplicaLatency.IsKnown() || oldReplicaLatency.Equals(newReplicaLatency).False() {
			c.backend.Meta.Set("mysql", "replica_latency", newReplicaLatency)
			c.backend.Meta.Set("mysql", "replica_running", newReplicaLatency.GreaterThanOrEqualTo(cty.NumberUIntVal(0)))

			var newReplicaLatencyValue int64
			err := gocty.FromCtyValue(newReplicaLatency, &newReplicaLatencyValue)
			misc.PanicIfErr(err)

			if !oldReplicaLatency.IsKnown() {
				log.Debug().Str("address", c.backend.Address).Int64("newReplicaLatency", newReplicaLatencyValue).Msg("Backend replica_latency changed")
			} else {
				var oldReplicaLatencyValue int64
				err := gocty.FromCtyValue(oldReplicaLatency, &oldReplicaLatencyValue)
				misc.PanicIfErr(err)

				log.Debug().Str("address", c.backend.Address).Int64("oldReplicaLatency", oldReplicaLatencyValue).Int64("newReplicaLatency", newReplicaLatencyValue).Msg("Backend replica_latency changed")
			}

			c.statusChan <- c.backend
		}
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

	c.ticker = misc.NewExponentialBackoffTicker(c.defaultPeriod, c.maxPeriod, c.backoffFactor)

	go func() {
		defer func() { c.running = false }()

		for {
			c.updateStatus()

			// Wait next iteration
			select {
			case <-c.stopChan:
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
	close(c.stopChan)
}
