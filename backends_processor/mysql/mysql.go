package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"mlb/backend"
	"mlb/config"
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

	"github.com/go-sql-driver/mysql"
)

var (
	mysqlDriverName   = "mysql"
	mysqlDriverNameMu sync.RWMutex
)

func getMySQLDriverName() string {
	mysqlDriverNameMu.RLock()
	defer mysqlDriverNameMu.RUnlock()
	return mysqlDriverName
}

func setMySQLDriverName(name string) {
	mysqlDriverNameMu.Lock()
	defer mysqlDriverNameMu.Unlock()
	mysqlDriverName = name
}

func init() {
	module.RegisterFactory("backends_processor", "mysql", newMySQLChecker, validateMySQLCheckerConfig)
}

type MySQLChecker struct {
	id              string
	checks          map[string]*MySQLCheck
	checksMtex      sync.RWMutex
	user            string
	password        string
	defaultPeriod   time.Duration
	maxPeriod       time.Duration
	backoffFactor   float64
	backends        *backend.Registry
	ctx             context.Context
	cancel          context.CancelFunc
	log             zerolog.Logger
	updChan         chan backend.BackendUpdate
	updChanStop     chan struct{}
	source          string
	connectTimeout  time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
	connMaxLifetime time.Duration
	checkReplica    bool
}

// MySQLCheckerConfig defines the HCL configuration for the MySQL backend processor.
type MySQLCheckerConfig struct {
	ID              string  `hcl:"id,label"`
	Source          string  `hcl:"source"`
	User            string  `hcl:"user,optional"`
	Password        string  `hcl:"password,optional"`
	Period          string  `hcl:"period,optional"`
	MaxPeriod       string  `hcl:"max_period,optional"`
	BackoffFactor   float64 `hcl:"backoff_factor,optional"`
	ConnectTimeout  string  `hcl:"connect_timeout,optional"`
	ReadTimeout     string  `hcl:"read_timeout,optional"`
	WriteTimeout    string  `hcl:"write_timeout,optional"`
	ConnMaxLifetime string  `hcl:"conn_max_lifetime,optional"`
	CheckReplica    bool    `hcl:"check_replica,optional"`
}

// validateMySQLCheckerConfig validates the MySQL checker configuration.
func validateMySQLCheckerConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &MySQLCheckerConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Period, "period")
	config.CheckDuration(&diags, configBody.MaxPeriod, "max_period")
	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.ReadTimeout, "read_timeout")
	config.CheckDuration(&diags, configBody.WriteTimeout, "write_timeout")
	config.CheckDuration(&diags, configBody.ConnMaxLifetime, "conn_max_lifetime")

	return diags
}

// parseMySQLCheckerConfig parses the MySQL checker configuration.
func parseMySQLCheckerConfig(tc *module.Config) *MySQLCheckerConfig {
	config := &MySQLCheckerConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode mysql backend processor config")
	}
	config.ID = tc.FullID()
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
	if config.ConnMaxLifetime == "" {
		config.ConnMaxLifetime = "5m"
	}
	return config
}

func newMySQLChecker(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) any {
	config := parseMySQLCheckerConfig(tc)

	c := &MySQLChecker{
		id:            config.ID,
		checks:        make(map[string]*MySQLCheck),
		user:          config.User,
		password:      config.Password,
		backoffFactor: config.BackoffFactor,
		log:           log.With().Str("id", config.ID).Logger(),
		updChan:       make(chan backend.BackendUpdate, 100),
		updChanStop:   make(chan struct{}),
		source:        config.Source,
		backends:      backend.NewRegistry(),
		checkReplica:  config.CheckReplica,
	}

	var err error

	c.defaultPeriod, err = time.ParseDuration(config.Period)
	if err != nil {
		panic(err)
	}
	c.maxPeriod, err = time.ParseDuration(config.MaxPeriod)
	if err != nil {
		panic(err)
	}
	c.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	if err != nil {
		panic(err)
	}
	c.readTimeout, err = time.ParseDuration(config.ReadTimeout)
	if err != nil {
		panic(err)
	}
	c.writeTimeout, err = time.ParseDuration(config.WriteTimeout)
	if err != nil {
		panic(err)
	}
	c.connMaxLifetime, err = time.ParseDuration(config.ConnMaxLifetime)
	if err != nil {
		panic(err)
	}

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
				c.backends.Publish(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: b.Address,
					Backend: b,
				})

			case upd := <-c.updChan: // Backend changed
				c.checksMtex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if check, ok := c.checks[upd.Address]; ok { // Modified
						// Update the existing backend in the registry with new data from inventory, while preserving the mysql bucket
						c.backends.Update(upd.Backend, "mysql")
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else { // Added
						c.log.Info().Str("address", upd.Address).Msg("Adding MySQL check")

						cfg := mysql.NewConfig()
						cfg.User = c.user
						cfg.Passwd = c.password
						cfg.Net = "tcp"
						cfg.Addr = upd.Address
						cfg.Timeout = c.connectTimeout
						cfg.ReadTimeout = c.readTimeout
						cfg.WriteTimeout = c.writeTimeout

						check := NewMySQLCheck(
							upd.Backend.Clone(),
							cfg.FormatDSN(),
							c.defaultPeriod,
							c.maxPeriod,
							c.backoffFactor,
							c.connMaxLifetime,
							statusChan,
							c.checkReplica,
						)
						err := check.StartPolling()
						if err != nil {
							c.log.Error().Str("address", upd.Address).Err(err).Msg("Error while adding MySQL check")
						} else {
							c.checks[upd.Address] = check
							c.backends.Add(check.backend)
							c.backends.Publish(backend.BackendUpdate{
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
						c.backends.Remove(upd.Address)
						c.backends.Publish(backend.BackendUpdate{
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

// stopChecks stops all active MySQL health checks.
func (c *MySQLChecker) stopChecks() {
	c.checksMtex.RLock()
	defer c.checksMtex.RUnlock()

	// Stop backend checks
	for _, backend := range c.checks {
		backend.StopPolling()
	}
}

// ProvideUpdates registers a subscriber and sends initial updates for all currently matched backends.
func (c *MySQLChecker) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

// ReceiveUpdate implements the backend.BackendUpdateSubscriber interface.
func (c *MySQLChecker) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case c.updChan <- upd:
	case <-c.updChanStop:
	}
}

// GetBackendList returns the current list of backends with their MySQL health metadata.
func (c *MySQLChecker) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

// Bind cross-links the processor with its source backend provider.
func (c *MySQLChecker) Bind(modules module.ModulesRegistry) {
	module.Get[backend.BackendUpdateProvider](modules, c.source).ProvideUpdates(c)
}

// MySQLCheck manages the health check lifecycle for a single MySQL backend.
type MySQLCheck struct {
	backend         *backend.Backend
	dsn             string
	period          time.Duration
	defaultPeriod   time.Duration
	maxPeriod       time.Duration
	backoffFactor   float64
	statusChan      chan *backend.Backend
	ticker          *misc.ExponentialBackoffTicker
	stopChan        chan struct{}
	ctx             context.Context
	cancel          context.CancelFunc
	running         bool
	runningMu       sync.Mutex
	db              *sql.DB
	connMaxLifetime time.Duration
	checkReplica    bool
}

// NewMySQLCheck creates a new MySQLCheck instance.
func NewMySQLCheck(backend *backend.Backend, dsn string, defaultPeriod time.Duration, maxPeriod time.Duration, backoffFactor float64, connMaxLifetime time.Duration, statusChan chan *backend.Backend, checkReplica bool) *MySQLCheck {
	c := &MySQLCheck{
		backend:         backend,
		dsn:             dsn,
		period:          defaultPeriod,
		defaultPeriod:   defaultPeriod,
		maxPeriod:       maxPeriod,
		backoffFactor:   backoffFactor,
		statusChan:      statusChan,
		stopChan:        make(chan struct{}),
		running:         false,
		connMaxLifetime: connMaxLifetime,
		checkReplica:    checkReplica,
	}
	backend.Meta.Set("mysql", "status", cty.UnknownVal(cty.String))
	backend.Meta.Set("mysql", "readonly", cty.UnknownVal(cty.Bool))
	if c.checkReplica {
		backend.Meta.Set("mysql", "replica_latency", cty.UnknownVal(cty.Number))
		backend.Meta.Set("mysql", "replica_running", cty.UnknownVal(cty.Bool))
	}
	return c
}

// fetchReadOnly checks if the MySQL instance is in read-only mode.
func (c *MySQLCheck) fetchReadOnly() (retReadonly cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retReadonly = cty.BoolVal(false)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}
		}
	}()

	var readOnly bool

	// Execute query with timeout
	ctx, cancel := context.WithTimeout(c.ctx, c.defaultPeriod)
	defer cancel()
	err := c.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
	if err != nil {
		panic(err)
	}

	return cty.BoolVal(readOnly), nil
}

// fetchReplicaLatency checks the replication lag of the MySQL instance.
func (c *MySQLCheck) fetchReplicaLatency() (retReplicaLatency cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retReplicaLatency = cty.NumberIntVal(-1)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}
		}
	}()

	// Default value -1 if replication is not running
	var replicationLatency int64 = -1

	// Execute query with timeout
	ctx, cancel := context.WithTimeout(c.ctx, c.defaultPeriod)
	defer cancel()
	result, err := c.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		panic(err)
	}
	defer result.Close()

	// If we have a row
	if result.Next() {
		// Find the column index for Seconds_Behind_Source
		columns, err := result.Columns()
		if err != nil {
			panic(err)
		}
		sbsColumn := -1
		for i := range columns {
			if columns[i] == "Seconds_Behind_Source" {
				sbsColumn = i
				break
			}
		}
		if sbsColumn == -1 {
			return cty.NumberIntVal(-1), fmt.Errorf("column Seconds_Behind_Source not found in SHOW REPLICA STATUS")
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
		if err != nil {
			panic(err)
		}

		// Get the value if not null
		if sbsValue.Valid {
			replicationLatency = int64(sbsValue.Int64)
		}
	}

	return cty.NumberIntVal(replicationLatency), nil
}

// fetchStatus performs all health checks for the MySQL instance.
func (c *MySQLCheck) fetchStatus() (retStatus cty.Value, retReadonly cty.Value, retReplicaLatency cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retStatus = cty.StringVal("err")
			retReadonly = cty.BoolVal(false)
			retReplicaLatency = cty.NumberIntVal(-1)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}

			// Close and reopen MySQL connection to ensure we start on a good base next time
			log.Info().Str("address", c.backend.Address).Msg("Reopening MySQL connection")
			if c.db != nil {
				c.db.Close()
			}
			db, err := sql.Open(getMySQLDriverName(), c.dsn)
			if err != nil {
				log.Warn().Str("address", c.backend.Address).Err(err).Msg("Error while reopening MySQL connection")
			} else {
				db.SetMaxOpenConns(1)
				db.SetMaxIdleConns(1)
				db.SetConnMaxLifetime(c.connMaxLifetime)
				c.db = db
			}

			// Increase fetch period
			if period, updated := c.ticker.ApplyBackoff(); updated {
				log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
			}
		}
	}()

	log.Trace().Str("address", c.backend.Address).Msg("Probing Backend")

	// Read Only
	readOnly, err := c.fetchReadOnly()
	if err != nil {
		panic(err)
	}

	// Replica Latency
	var replicaLatency cty.Value = cty.UnknownVal(cty.Number)
	if c.checkReplica {
		replicaLatency, err = c.fetchReplicaLatency()
		if err != nil {
			panic(err)
		}
	}

	// If everything went OK, reset the fetch period if needed
	if period, updated := c.ticker.Reset(); updated {
		log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
	}

	return cty.StringVal("ok"), readOnly, replicaLatency, nil
}

// updateStatus fetches the current status and updates the backend metadata if it has changed.
func (c *MySQLCheck) updateStatus() {
	newStatus, newReadonly, newReplicaLatency, err := c.fetchStatus()

	if err != nil {
		log.Error().Str("address", c.backend.Address).Err(err).Msg("Error while fetching status from backend")
	}

	changed := false

	oldStatus, ok := c.backend.Meta.Get("mysql", "status")
	if !ok || !oldStatus.IsKnown() || oldStatus.Equals(newStatus).False() {
		c.backend.Meta.Set("mysql", "status", newStatus)

		if !oldStatus.IsKnown() {
			log.Info().Str("address", c.backend.Address).Str("newStatus", newStatus.AsString()).Msg("Backend status changed")
		} else {
			log.Info().Str("address", c.backend.Address).Str("oldStatus", oldStatus.AsString()).Str("newStatus", newStatus.AsString()).Msg("Backend status changed")
		}
		changed = true
	}

	oldReadonly, ok := c.backend.Meta.Get("mysql", "readonly")
	if !ok || !oldReadonly.IsKnown() || oldReadonly.Equals(newReadonly).False() {
		c.backend.Meta.Set("mysql", "readonly", newReadonly)

		if !oldReadonly.IsKnown() {
			log.Info().Str("address", c.backend.Address).Bool("newReadonly", newReadonly.True()).Msg("Backend readonly changed")
		} else {
			log.Info().Str("address", c.backend.Address).Bool("oldReadonly", oldReadonly.True()).Bool("newReadonly", newReadonly.True()).Msg("Backend readonly changed")
		}
		changed = true
	}

	if c.checkReplica {
		oldReplicaLatency, ok := c.backend.Meta.Get("mysql", "replica_latency")
		if !ok || !oldReplicaLatency.IsKnown() || oldReplicaLatency.Equals(newReplicaLatency).False() {
			c.backend.Meta.Set("mysql", "replica_latency", newReplicaLatency)
			c.backend.Meta.Set("mysql", "replica_running", newReplicaLatency.GreaterThanOrEqualTo(cty.NumberUIntVal(0)))

			var newReplicaLatencyValue int64
			err := gocty.FromCtyValue(newReplicaLatency, &newReplicaLatencyValue)
			if err != nil {
				panic(err)
			}

			if !oldReplicaLatency.IsKnown() {
				log.Debug().Str("address", c.backend.Address).Int64("newReplicaLatency", newReplicaLatencyValue).Msg("Backend replica_latency changed")
			} else {
				var oldReplicaLatencyValue int64
				err := gocty.FromCtyValue(oldReplicaLatency, &oldReplicaLatencyValue)
				if err != nil {
					panic(err)
				}

				log.Debug().Str("address", c.backend.Address).Int64("oldReplicaLatency", oldReplicaLatencyValue).Int64("newReplicaLatency", newReplicaLatencyValue).Msg("Backend replica_latency changed")
			}
			changed = true
		}
	}

	if changed {
		select {
		case c.statusChan <- c.backend:
		case <-c.stopChan:
		}
	}
}

// StartPolling starts the health check polling loop for the MySQL instance.
func (c *MySQLCheck) StartPolling() error {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return nil
	}

	c.stopChan = make(chan struct{})
	c.ctx, c.cancel = context.WithCancel(context.Background())

	db, err := sql.Open(getMySQLDriverName(), c.dsn)
	if err != nil {
		c.runningMu.Unlock()
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(c.connMaxLifetime)
	c.db = db

	c.ticker = misc.NewExponentialBackoffTicker(c.defaultPeriod, c.maxPeriod, c.backoffFactor)

	c.running = true
	c.runningMu.Unlock()

	go func() {
		defer func() {
			c.runningMu.Lock()
			db := c.db
			ticker := c.ticker
			c.running = false
			c.runningMu.Unlock()

			if db != nil {
				db.Close()
			}
			if ticker != nil {
				ticker.Stop()
			}
		}()

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

// StopPolling stops the health check polling loop for the MySQL instance.
func (c *MySQLCheck) StopPolling() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	if !c.running {
		return
	}

	if c.cancel != nil {
		c.cancel()
	}

	select {
	case <-c.stopChan:
	default:
		close(c.stopChan)
	}
}
