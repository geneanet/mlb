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
	id                 string
	checks             map[string]*MySQLCheck
	user               string
	password           string
	defaultPeriod      time.Duration
	maxPeriod          time.Duration
	backoffFactor      float64
	retryPeriod        time.Duration
	retryMaxPeriod     time.Duration
	retryBackoffFactor float64
	retryMaxAttempts   int
	failureThreshold   int
	backends           *backend.Registry
	ctx                context.Context
	cancel             context.CancelFunc
	log                zerolog.Logger
	updChan            chan backend.BackendUpdate
	updChanStop        chan struct{}
	source             string
	connectTimeout     time.Duration
	readTimeout        time.Duration
	writeTimeout       time.Duration
	connMaxLifetime    time.Duration
	checkReplica       bool
	initialBackends    map[string]bool
	initialChecked     map[string]bool
	upstreamReady      bool
}

// MySQLCheckerConfig defines the HCL configuration for the MySQL backend processor.
type MySQLCheckerConfig struct {
	ID                 string  `hcl:"id,label"`
	Source             string  `hcl:"source"`
	User               string  `hcl:"user,optional"`
	Password           string  `hcl:"password,optional"`
	Period             string  `hcl:"period,optional"`
	MaxPeriod          string  `hcl:"max_period,optional"`
	BackoffFactor      float64 `hcl:"backoff_factor,optional"`
	ConnectTimeout     string  `hcl:"connect_timeout,optional"`
	ReadTimeout        string  `hcl:"read_timeout,optional"`
	WriteTimeout       string  `hcl:"write_timeout,optional"`
	RetryPeriod        string  `hcl:"retry_period,optional"`
	RetryMaxPeriod     string  `hcl:"retry_max_period,optional"`
	RetryBackoffFactor float64 `hcl:"retry_backoff_factor,optional"`
	RetryMaxAttempts   int     `hcl:"retry_max_attempts,optional"`
	FailureThreshold   *int    `hcl:"failure_threshold,optional"`
	ConnMaxLifetime    string  `hcl:"conn_max_lifetime,optional"`
	CheckReplica       bool    `hcl:"check_replica,optional"`
	LogBackendUpdates  bool    `hcl:"log_backend_updates,optional"`
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
	config.CheckDuration(&diags, configBody.RetryPeriod, "retry_period")
	config.CheckDuration(&diags, configBody.RetryMaxPeriod, "retry_max_period")
	if configBody.FailureThreshold != nil && *configBody.FailureThreshold < 1 {
		diags = append(diags, &hcl.Diagnostic{
			Severity: hcl.DiagError,
			Summary:  "Invalid failure_threshold",
			Detail:   "failure_threshold must be at least 1.",
		})
	}

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
	if config.RetryPeriod == "" {
		config.RetryPeriod = "100ms"
	}
	if config.RetryMaxPeriod == "" {
		config.RetryMaxPeriod = "1s"
	}
	if config.RetryBackoffFactor == 0 {
		config.RetryBackoffFactor = 1.5
	}
	if config.RetryMaxAttempts == 0 {
		config.RetryMaxAttempts = 3 // default to 3 attempts
	}
	if config.FailureThreshold == nil {
		threshold := 1
		config.FailureThreshold = &threshold
	}
	if config.ConnMaxLifetime == "" {
		config.ConnMaxLifetime = "5m"
	}
	return config
}

func newMySQLChecker(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseMySQLCheckerConfig(tc)
	if *config.FailureThreshold < 1 {
		return nil, fmt.Errorf("failure_threshold must be at least 1")
	}

	c := &MySQLChecker{
		id:                 config.ID,
		checks:             make(map[string]*MySQLCheck),
		user:               config.User,
		password:           config.Password,
		backoffFactor:      config.BackoffFactor,
		retryBackoffFactor: config.RetryBackoffFactor,
		retryMaxAttempts:   config.RetryMaxAttempts,
		failureThreshold:   *config.FailureThreshold,
		log:                log.With().Str("id", config.ID).Logger(),
		updChan:            make(chan backend.BackendUpdate, 100),
		updChanStop:        make(chan struct{}),
		source:             config.Source,
		backends:           backend.NewRegistry(log.With().Str("id", config.ID).Logger(), config.LogBackendUpdates),
		checkReplica:       config.CheckReplica,
		initialBackends:    make(map[string]bool),
		initialChecked:     make(map[string]bool),
	}

	var err error

	c.defaultPeriod, err = time.ParseDuration(config.Period)
	if err != nil {
		return nil, err
	}
	c.maxPeriod, err = time.ParseDuration(config.MaxPeriod)
	if err != nil {
		return nil, err
	}
	c.retryPeriod, err = time.ParseDuration(config.RetryPeriod)
	if err != nil {
		return nil, err
	}
	c.retryMaxPeriod, err = time.ParseDuration(config.RetryMaxPeriod)
	if err != nil {
		return nil, err
	}
	c.connectTimeout, err = time.ParseDuration(config.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	c.readTimeout, err = time.ParseDuration(config.ReadTimeout)
	if err != nil {
		return nil, err
	}
	c.writeTimeout, err = time.ParseDuration(config.WriteTimeout)
	if err != nil {
		return nil, err
	}
	c.connMaxLifetime, err = time.ParseDuration(config.ConnMaxLifetime)
	if err != nil {
		return nil, err
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

		// checkReadiness checks if the processor can signal its readiness.
		// It requires the upstream source to be ready AND all backends initially
		// received to have completed at least one health check.
		checkReadiness := func() {
			if !c.upstreamReady {
				return
			}
			for addr := range c.initialBackends {
				if !c.initialChecked[addr] {
					return
				}
			}
			c.backends.MarkReady()
		}

		for {
			select {
			case b := <-statusChan: // Backend status changed
				// Mark this backend as having its first check completed
				c.initialChecked[b.Address] = true
				c.backends.Publish(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: b.Address,
					Backend: b,
				})
				checkReadiness()

			case upd := <-c.updChan: // Backend changed
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					// Track backends added before the source is ready as "initial"
					if !c.upstreamReady && upd.Kind == backend.UpdBackendAdded {
						c.initialBackends[upd.Address] = true
					}
					if check, ok := c.checks[upd.Address]; ok { // Modified
						// Update the existing backend in the registry with new data from inventory, while preserving the mysql bucket
						c.backends.Update(upd.Backend, "mysql")
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else { // Added
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
							c.retryPeriod,
							c.retryMaxPeriod,
							c.retryBackoffFactor,
							c.retryMaxAttempts,
							c.connMaxLifetime,
							statusChan,
							c.checkReplica,
						)
						check.failureThreshold = c.failureThreshold
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
					delete(c.initialBackends, upd.Address)
					delete(c.initialChecked, upd.Address)
					// Removed
					if check, ok := c.checks[upd.Address]; ok {
						check.StopPolling()
						delete(c.checks, upd.Address)
						c.backends.Remove(upd.Address)
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
						})
					}
					checkReadiness()
				case backend.UpdReady:
					c.upstreamReady = true
					checkReadiness()
				}

			case <-c.ctx.Done(): // Context cancelled
				return
			}
		}
	}()

	return c, nil
}

// stopChecks stops all active MySQL health checks.
func (c *MySQLChecker) stopChecks() {
	// Stop backend checks
	for _, backend := range c.checks {
		backend.StopPolling()
	}
}

// ProvideUpdates registers a subscriber and sends initial updates for all currently matched backends.
func (c *MySQLChecker) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

// Ready returns a channel that is closed when the checker is ready.
func (c *MySQLChecker) Ready() <-chan struct{} {
	return c.backends.Ready()
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
func (c *MySQLChecker) Bind(modules module.ModulesRegistry) error {
	m, err := module.Get[backend.BackendUpdateProvider](modules, c.source)
	if err != nil {
		return err
	}
	m.ProvideUpdates(c)
	return nil
}

// MySQLCheck manages the health check lifecycle for a single MySQL backend.
type MySQLCheck struct {
	backend             *backend.Backend
	dsn                 string
	period              time.Duration
	defaultPeriod       time.Duration
	maxPeriod           time.Duration
	retryPeriod         time.Duration
	retryMaxPeriod      time.Duration
	retryBackoffFactor  float64
	retryMaxAttempts    int
	failureThreshold    int
	consecutiveFailures int
	statusChan          chan *backend.Backend
	backoff             *misc.ExponentialBackoff
	stopChan            chan struct{}
	ctx                 context.Context
	cancel              context.CancelFunc
	running             bool
	runningMu           sync.Mutex
	db                  *sql.DB
	connMaxLifetime     time.Duration
	checkReplica        bool
}

// NewMySQLCheck creates a new MySQLCheck instance.
func NewMySQLCheck(backend *backend.Backend, dsn string, defaultPeriod time.Duration, maxPeriod time.Duration, backoffFactor float64, retryPeriod, retryMaxPeriod time.Duration, retryBackoffFactor float64, retryMaxAttempts int, connMaxLifetime time.Duration, statusChan chan *backend.Backend, checkReplica bool) *MySQLCheck {
	c := &MySQLCheck{
		backend:            backend,
		dsn:                dsn,
		defaultPeriod:      defaultPeriod,
		maxPeriod:          maxPeriod,
		retryPeriod:        retryPeriod,
		retryMaxPeriod:     retryMaxPeriod,
		retryBackoffFactor: retryBackoffFactor,
		retryMaxAttempts:   retryMaxAttempts,
		failureThreshold:   1,
		backoff:            misc.NewExponentialBackoff(defaultPeriod, maxPeriod, backoffFactor),
		ctx:                context.Background(),
		statusChan:         statusChan,
		stopChan:           make(chan struct{}),
		running:            false,
		connMaxLifetime:    connMaxLifetime,
		checkReplica:       checkReplica,
	}
	c.period = c.backoff.Get()
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
			retReadonly = cty.UnknownVal(cty.Bool)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}
		}
	}()

	var readOnly bool
	var err error

	retryBackoff := misc.NewExponentialBackoff(c.retryPeriod, c.retryMaxPeriod, c.retryBackoffFactor)

	for attempt := 0; attempt < c.retryMaxAttempts; attempt++ {
		if attempt > 0 {
			log.Warn().Str("address", c.backend.Address).Int("attempt", attempt).Msg("Retrying MySQL read_only check")
			retryBackoff.Sleep(c.ctx)
		}
		if err := c.ctx.Err(); err != nil {
			return cty.UnknownVal(cty.Bool), err
		}

		ctx, cancel := context.WithTimeout(c.ctx, c.defaultPeriod)
		err = c.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
		cancel()

		if err == nil {
			break
		}
	}

	if err != nil {
		return cty.UnknownVal(cty.Bool), err
	}

	return cty.BoolVal(readOnly), nil
}

// fetchReplicaLatency checks the replication lag of the MySQL instance.
func (c *MySQLCheck) fetchReplicaLatency() (retReplicaLatency cty.Value, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retReplicaLatency = cty.UnknownVal(cty.Number)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}
		}
	}()

	retryBackoff := misc.NewExponentialBackoff(c.retryPeriod, c.retryMaxPeriod, c.retryBackoffFactor)
	var replicationLatency int64 = -1
	var err error

	for attempt := 0; attempt < c.retryMaxAttempts; attempt++ {
		if attempt > 0 {
			log.Warn().Str("address", c.backend.Address).Int("attempt", attempt).Msg("Retrying MySQL replica_latency check")
			retryBackoff.Sleep(c.ctx)
		}
		if err := c.ctx.Err(); err != nil {
			return cty.UnknownVal(cty.Number), err
		}

		err = func() error {
			ctx, cancel := context.WithTimeout(c.ctx, c.defaultPeriod)
			defer cancel()

			result, err := c.db.QueryContext(ctx, "SHOW REPLICA STATUS")
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Fallback to SHOW SLAVE STATUS for MySQL < 8.0.22 and MariaDB
				result, err = c.db.QueryContext(ctx, "SHOW SLAVE STATUS")
				if err != nil {
					return err
				}
			}
			defer result.Close()

			// If we have a row
			if result.Next() {
				// Find the column index for Seconds_Behind_Source or Seconds_Behind_Master
				columns, err := result.Columns()
				if err != nil {
					return err
				}
				sbsColumn := -1
				for i := range columns {
					if columns[i] == "Seconds_Behind_Source" || columns[i] == "Seconds_Behind_Master" {
						sbsColumn = i
						break
					}
				}
				if sbsColumn == -1 {
					return fmt.Errorf("column Seconds_Behind_Source or Seconds_Behind_Master not found in replica status")
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
					return err
				}

				// Get the value if not null
				if sbsValue.Valid {
					replicationLatency = int64(sbsValue.Int64)
				}
			}
			return result.Err()
		}()

		if err == nil {
			break
		}
	}

	if err != nil {
		return cty.UnknownVal(cty.Number), err
	}

	return cty.NumberIntVal(replicationLatency), nil
}

// fetchStatus performs all health checks for the MySQL instance.
func (c *MySQLCheck) fetchStatus() (retStatus cty.Value, retReadonly cty.Value, retReplicaLatency cty.Value, retErr error) {
	log.Trace().Str("address", c.backend.Address).Msg("Probing Backend")

	// Read Only
	readOnly, err := c.fetchReadOnly()
	if err != nil {
		return cty.StringVal("err"), cty.UnknownVal(cty.Bool), cty.UnknownVal(cty.Number), err
	}

	// Replica Latency
	var replicaLatency = cty.UnknownVal(cty.Number)
	if c.checkReplica {
		replicaLatency, err = c.fetchReplicaLatency()
		if err != nil {
			return cty.StringVal("err"), readOnly, cty.UnknownVal(cty.Number), err
		}
	}

	return cty.StringVal("ok"), readOnly, replicaLatency, nil
}

// updateStatus fetches the current status and updates the backend metadata if it has changed.
func (c *MySQLCheck) updateStatus() {
	newStatus, newReadonly, newReplicaLatency, err := c.fetchStatus()
	if c.ctx.Err() != nil {
		return
	}

	oldStatus, ok := c.backend.Meta.Get("mysql", "status")
	oldPeriod := c.period
	if err != nil {
		c.consecutiveFailures++
		c.period = c.backoff.Get()
		if ok && oldStatus.RawEquals(cty.StringVal("ok")) && c.consecutiveFailures < c.failureThreshold {
			newStatus = oldStatus
		}
		log.Error().Str("address", c.backend.Address).Err(err).
			Int("consecutive_failures", c.consecutiveFailures).
			Int("failure_threshold", c.failureThreshold).
			Msg("Error while fetching status from backend")
	} else {
		c.consecutiveFailures = 0
		c.backoff.Reset()
		c.period = c.backoff.Get()
	}
	if oldPeriod != c.period {
		log.Warn().Str("address", c.backend.Address).Dur("period", c.period).Msg("Updating fetch period")
	}

	changed := false

	if !ok || !oldStatus.RawEquals(newStatus) {
		c.backend.Meta.Set("mysql", "status", newStatus)

		evt := log.Info().Str("address", c.backend.Address)
		if oldStatus.IsKnown() && !oldStatus.IsNull() {
			evt.Str("oldStatus", oldStatus.AsString())
		} else {
			evt.Str("oldStatus", "unknown")
		}
		if newStatus.IsKnown() && !newStatus.IsNull() {
			evt.Str("newStatus", newStatus.AsString())
		} else {
			evt.Str("newStatus", "unknown")
		}
		evt.Msg("Backend status changed")
		changed = true
	}

	oldReadonly, ok := c.backend.Meta.Get("mysql", "readonly")
	if newReadonly.IsKnown() && !newReadonly.IsNull() && (!ok || !oldReadonly.RawEquals(newReadonly)) {
		c.backend.Meta.Set("mysql", "readonly", newReadonly)

		if !oldReadonly.IsKnown() {
			log.Info().Str("address", c.backend.Address).Bool("newReadonly", newReadonly.IsKnown() && !newReadonly.IsNull() && newReadonly.True()).Msg("Backend readonly changed")
		} else {
			log.Info().Str("address", c.backend.Address).Bool("oldReadonly", oldReadonly.IsKnown() && !oldReadonly.IsNull() && oldReadonly.True()).Bool("newReadonly", newReadonly.IsKnown() && !newReadonly.IsNull() && newReadonly.True()).Msg("Backend readonly changed")
		}
		changed = true
	}

	if c.checkReplica {
		oldReplicaLatency, ok := c.backend.Meta.Get("mysql", "replica_latency")
		if newReplicaLatency.IsKnown() && !newReplicaLatency.IsNull() && (!ok || !oldReplicaLatency.RawEquals(newReplicaLatency)) {
			c.backend.Meta.Set("mysql", "replica_latency", newReplicaLatency)
			c.backend.Meta.Set("mysql", "replica_running", cty.BoolVal(newReplicaLatency.IsKnown() && !newReplicaLatency.IsNull() && newReplicaLatency.GreaterThanOrEqualTo(cty.NumberUIntVal(0)).True()))

			evt := log.Debug().Str("address", c.backend.Address)

			if oldReplicaLatency.IsKnown() && !oldReplicaLatency.IsNull() {
				var oldReplicaLatencyValue int64
				if err := gocty.FromCtyValue(oldReplicaLatency, &oldReplicaLatencyValue); err == nil {
					evt.Int64("oldReplicaLatency", oldReplicaLatencyValue)
				}
			}

			if newReplicaLatency.IsKnown() && !newReplicaLatency.IsNull() {
				var newReplicaLatencyValue int64
				if err := gocty.FromCtyValue(newReplicaLatency, &newReplicaLatencyValue); err == nil {
					evt.Int64("newReplicaLatency", newReplicaLatencyValue)
				}
			}

			evt.Msg("Backend replica_latency changed")
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

	c.backoff.Reset()
	c.period = c.backoff.Get()
	c.consecutiveFailures = 0

	c.running = true
	c.runningMu.Unlock()

	go func() {
		defer func() {
			c.runningMu.Lock()
			db := c.db
			if db != nil {
				_ = db.Close()
			}
			c.running = false
			c.runningMu.Unlock()
		}()

		for {
			if c.ctx.Err() != nil {
				return
			}
			c.updateStatus()

			timer := time.NewTimer(c.period)
			select {
			case <-c.stopChan:
				timer.Stop()
				return
			case <-timer.C:
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
