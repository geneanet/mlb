package redis

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"mlb/misc"
	"mlb/module"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"
)

func init() {
	module.RegisterFactory("backends_processor", "redis", newRedisChecker, validateRedisCheckerConfig)
}

// RedisChecker manages multiple health checks for Redis backends.
// It subscribes to a backend source and maintains a registry of monitored Redis instances.
type RedisChecker struct {
	id                 string
	checks             map[string]*RedisCheck
	checksMtex         sync.RWMutex
	password           string
	defaultPeriod      time.Duration
	maxPeriod          time.Duration
	backoffFactor      float64
	retryPeriod        time.Duration
	retryMaxPeriod     time.Duration
	retryBackoffFactor float64
	retryMaxAttempts   int
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
	initialBackends    map[string]bool
	initialChecked     map[string]bool
	upstreamReady      bool
}

// RedisCheckerConfig defines the HCL configuration schema for the Redis backend processor.
type RedisCheckerConfig struct {
	ID                 string  `hcl:"id,label"`
	Source             string  `hcl:"source"`
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
	LogBackendUpdates  bool    `hcl:"log_backend_updates,optional"`
}

// validateRedisCheckerConfig validates the Redis checker configuration.
func validateRedisCheckerConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &RedisCheckerConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Period, "period")
	config.CheckDuration(&diags, configBody.MaxPeriod, "max_period")
	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.ReadTimeout, "read_timeout")
	config.CheckDuration(&diags, configBody.WriteTimeout, "write_timeout")
	config.CheckDuration(&diags, configBody.RetryPeriod, "retry_period")
	config.CheckDuration(&diags, configBody.RetryMaxPeriod, "retry_max_period")

	return diags
}

// parseRedisCheckerConfig parses the Redis checker configuration.
func parseRedisCheckerConfig(tc *module.Config) *RedisCheckerConfig {
	config := &RedisCheckerConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode redis backend processor config")
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
		config.ConnectTimeout = "1s"
	}
	if config.ReadTimeout == "" {
		config.ReadTimeout = "1s"
	}
	if config.WriteTimeout == "" {
		config.WriteTimeout = "1s"
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
		config.RetryMaxAttempts = 3 // default to 3 attempts (or whatever logic handles time limit)
	}
	return config
}

// New creates a new instance of the RedisChecker module.
func newRedisChecker(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseRedisCheckerConfig(tc)

	c := &RedisChecker{
		id:                 config.ID,
		checks:             make(map[string]*RedisCheck),
		password:           config.Password,
		backoffFactor:      config.BackoffFactor,
		retryBackoffFactor: config.RetryBackoffFactor,
		retryMaxAttempts:   config.RetryMaxAttempts,
		log:                log.With().Str("id", config.ID).Logger(),
		updChan:            make(chan backend.BackendUpdate, 100),
		updChanStop:        make(chan struct{}),
		source:             config.Source,
		backends:           backend.NewRegistry(log.With().Str("id", config.ID).Logger(), config.LogBackendUpdates),
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

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("Redis checker starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("Redis checker stopped")
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
			case b := <-statusChan:
				// Mark this backend as having its first check completed
				c.initialChecked[b.Address] = true
				c.backends.Publish(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: b.Address,
					Backend: b,
				})
				checkReadiness()

			case upd := <-c.updChan:
				c.checksMtex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					// Track backends added before the source is ready as "initial"
					if !c.upstreamReady && upd.Kind == backend.UpdBackendAdded {
						c.initialBackends[upd.Address] = true
					}
					if check, ok := c.checks[upd.Address]; ok {
						c.backends.Update(upd.Backend, "redis")
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else {
						check := NewRedisCheck(
							upd.Backend.Clone(),
							c.password,
							c.defaultPeriod,
							c.maxPeriod,
							c.backoffFactor,
							c.retryPeriod,
							c.retryMaxPeriod,
							c.retryBackoffFactor,
							c.retryMaxAttempts,
							c.connectTimeout,
							c.readTimeout,
							c.writeTimeout,
							statusChan,
						)
						err := check.StartPolling()
						if err != nil {
							c.log.Error().Str("address", upd.Address).Err(err).Msg("Error while adding Redis check")
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
					if check, ok := c.checks[upd.Address]; ok {
						check.StopPolling()
						delete(c.checks, upd.Address)
						c.backends.Remove(upd.Address)
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
						})
					}
				case backend.UpdReady:
					c.upstreamReady = true
					checkReadiness()
				}
				c.checksMtex.Unlock()

			case <-c.ctx.Done():
				return
			}
		}
	}()

	return c, nil
}

func (c *RedisChecker) stopChecks() {
	c.checksMtex.RLock()
	defer c.checksMtex.RUnlock()

	for _, backend := range c.checks {
		backend.StopPolling()
	}
}

// ProvideUpdates implements backend.BackendUpdateProvider.
func (c *RedisChecker) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

// Ready returns a channel that is closed when the checker is ready.
func (c *RedisChecker) Ready() <-chan struct{} {
	return c.backends.Ready()
}

// ReceiveUpdate implements backend.BackendUpdateSubscriber.
func (c *RedisChecker) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case c.updChan <- upd:
	case <-c.updChanStop:
	}
}

func (c *RedisChecker) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

// Bind initializes the module by subscribing to its configured source.
func (c *RedisChecker) Bind(modules module.ModulesRegistry) error {
	m, err := module.Get[backend.BackendUpdateProvider](modules, c.source)
	if err != nil {
		return err
	}
	m.ProvideUpdates(c)
	return nil
}

type redisClient interface {
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
	Info(ctx context.Context, section ...string) *redis.StringCmd
	Close() error
}

// RedisCheck represents a background health checker for a single Redis instance.
type RedisCheck struct {
	backend            *backend.Backend
	password           string
	period             time.Duration
	defaultPeriod      time.Duration
	maxPeriod          time.Duration
	backoffFactor      float64
	retryPeriod        time.Duration
	retryMaxPeriod     time.Duration
	retryBackoffFactor float64
	retryMaxAttempts   int
	statusChan         chan *backend.Backend
	ticker             *misc.ExponentialBackoffTicker
	stopChan           chan struct{}
	ctx                context.Context
	cancel             context.CancelFunc
	running            bool
	runningMu          sync.Mutex
	client             redisClient
	connectTimeout     time.Duration
	readTimeout        time.Duration
	writeTimeout       time.Duration
}

// NewRedisCheck initializes a new health checker for a specific Redis backend.
func NewRedisCheck(backend *backend.Backend, password string, defaultPeriod time.Duration, maxPeriod time.Duration, backoffFactor float64, retryPeriod, retryMaxPeriod time.Duration, retryBackoffFactor float64, retryMaxAttempts int, connectTimeout, readTimeout, writeTimeout time.Duration, statusChan chan *backend.Backend) *RedisCheck {
	c := &RedisCheck{
		backend:            backend,
		password:           password,
		period:             defaultPeriod,
		defaultPeriod:      defaultPeriod,
		maxPeriod:          maxPeriod,
		backoffFactor:      backoffFactor,
		retryPeriod:        retryPeriod,
		retryMaxPeriod:     retryMaxPeriod,
		retryBackoffFactor: retryBackoffFactor,
		retryMaxAttempts:   retryMaxAttempts,
		statusChan:         statusChan,
		stopChan:           make(chan struct{}),
		running:            false,
		connectTimeout:     connectTimeout,
		readTimeout:        readTimeout,
		writeTimeout:       writeTimeout,
	}
	// Pre-initialize metadata with unknown values
	backend.Meta.Set("redis", "status", cty.UnknownVal(cty.String))
	backend.Meta.Set("redis", "role", cty.UnknownVal(cty.String))
	backend.Meta.Set("redis", "readonly", cty.UnknownVal(cty.Bool))
	backend.Meta.Set("redis", "connected_slaves", cty.UnknownVal(cty.Number))
	backend.Meta.Set("redis", "master_link_status", cty.UnknownVal(cty.String))
	backend.Meta.Set("redis", "master_sync_in_progress", cty.UnknownVal(cty.Bool))
	return c
}

func parseRoleResponse(roleResult interface{}) (retRole cty.Value, retReadonly cty.Value, retSlaves cty.Value, err error) {
	// ROLE returns an array: [role, ...]
	if roles, ok := roleResult.([]interface{}); ok && len(roles) > 0 {
		if role, ok := roles[0].(string); ok {
			slaves := int64(0)
			if role == "master" && len(roles) > 2 {
				if slaveList, ok := roles[2].([]interface{}); ok {
					slaves = int64(len(slaveList))
				}
			}
			return cty.StringVal(role), cty.BoolVal(role != "master"), cty.NumberIntVal(slaves), nil
		}
	}
	return cty.NilVal, cty.NilVal, cty.NilVal, fmt.Errorf("unexpected ROLE result format")
}

func parseInfoResponse(infoResult string) (retRole cty.Value, retReadonly cty.Value, retSlaves cty.Value, retMasterLinkStatus cty.Value, retMasterSyncInProgress cty.Value) {
	role := "unknown"
	slaves := int64(0)
	masterLinkStatus := "unknown"
	masterSyncInProgress := false

	lines := strings.Split(infoResult, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "role:") {
			role = strings.TrimPrefix(line, "role:")
		} else if strings.HasPrefix(line, "connected_slaves:") {
			s := strings.TrimPrefix(line, "connected_slaves:")
			if val, err := strconv.ParseInt(s, 10, 64); err == nil {
				slaves = val
			}
		} else if strings.HasPrefix(line, "master_link_status:") {
			masterLinkStatus = strings.TrimPrefix(line, "master_link_status:")
		} else if strings.HasPrefix(line, "master_sync_in_progress:") {
			s := strings.TrimPrefix(line, "master_sync_in_progress:")
			masterSyncInProgress = s == "1"
		}
	}

	return cty.StringVal(role), cty.BoolVal(role != "master"), cty.NumberIntVal(slaves), cty.StringVal(masterLinkStatus), cty.BoolVal(masterSyncInProgress)
}

// fetchStatus probes the Redis instance to determine its current status and role.
// It prioritizes the ROLE command and falls back to INFO replication for older versions.
func (c *RedisCheck) fetchStatus() (retStatus cty.Value, retRole cty.Value, retReadonly cty.Value, retSlaves cty.Value, retMasterLinkStatus cty.Value, retMasterSyncInProgress cty.Value, retErr error) {
	defer func() {
		// Recovery block to handle network errors or Redis restarts.
		// If an error occurs, we attempt to recreate the client and apply an exponential backoff.
		if r := recover(); r != nil {
			retStatus = cty.StringVal("err")
			retRole = cty.UnknownVal(cty.String)
			retReadonly = cty.UnknownVal(cty.Bool)
			retSlaves = cty.UnknownVal(cty.Number)
			retMasterLinkStatus = cty.UnknownVal(cty.String)
			retMasterSyncInProgress = cty.UnknownVal(cty.Bool)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}

			// Reconnect on error to ensure we start from a clean state next iteration.
			log.Info().Str("address", c.backend.Address).Msg("Reconnecting Redis")
			if c.client != nil {
				_ = c.client.Close()
			}
			c.client = redis.NewClient(&redis.Options{
				Addr:         c.backend.Address,
				Password:     c.password,
				DialTimeout:  c.connectTimeout,
				ReadTimeout:  c.readTimeout,
				WriteTimeout: c.writeTimeout,
			})

			// Slow down polling frequency if errors persist.
			if period, updated := c.ticker.ApplyBackoff(); updated {
				log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
			}
		}
	}()

	log.Trace().Str("address", c.backend.Address).Msg("Probing Redis Backend")

	readTimeout := c.readTimeout
	if readTimeout <= 0 {
		readTimeout = 1 * time.Second // ponytail: safeguard against immediate timeout if misconfigured to 0s
	}

	retryBackoff := misc.NewExponentialBackoff(c.retryPeriod, c.retryMaxPeriod, c.retryBackoffFactor)

	var infoResult string
	var err error

	for attempt := 0; attempt < c.retryMaxAttempts; attempt++ {
		if attempt > 0 {
			log.Warn().Str("address", c.backend.Address).Int("attempt", attempt).Msg("Retrying Redis check")
			retryBackoff.Sleep(c.ctx)
		}

		ctx, cancel := context.WithTimeout(c.ctx, readTimeout)
		infoResult, err = c.client.Info(ctx, "replication").Result()
		cancel()

		if err == nil {
			break
		}
	}

	if err != nil {
		panic(err)
	}

	retRole, retReadonly, retSlaves, retMasterLinkStatus, retMasterSyncInProgress = parseInfoResponse(infoResult)

	// Use ROLE command (available since Redis 2.8.12) which returns a structured array if possible for role and slaves.
	ctxRole, cancelRole := context.WithTimeout(c.ctx, readTimeout)
	roleResult, errRole := c.client.Do(ctxRole, "ROLE").Result()
	cancelRole()

	if errRole == nil {
		if rRole, rReadonly, rSlaves, errParse := parseRoleResponse(roleResult); errParse == nil {
			retRole = rRole
			retReadonly = rReadonly
			retSlaves = rSlaves
		}
	}

	// Reset polling frequency on success.
	if period, updated := c.ticker.Reset(); updated {
		log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
	}

	return cty.StringVal("ok"), retRole, retReadonly, retSlaves, retMasterLinkStatus, retMasterSyncInProgress, nil
}

// updateStatus executes a single health check and updates the backend's metadata.
func (c *RedisCheck) updateStatus() {
	newStatus, newRole, newReadonly, newSlaves, newMasterLinkStatus, newMasterSyncInProgress, err := c.fetchStatus()

	if err != nil {
		log.Error().Str("address", c.backend.Address).Err(err).Msg("Error while fetching status from Redis backend")
	}

	changed := false

	updateMeta := func(key string, newValue cty.Value, logKnown func(e *zerolog.Event)) {
		oldValue, ok := c.backend.Meta.Get("redis", key)
		if !ok || !oldValue.RawEquals(newValue) {
			c.backend.Meta.Set("redis", key, newValue)
			evt := log.Info().Str("address", c.backend.Address)
			if newValue.IsKnown() && !newValue.IsNull() {
				logKnown(evt)
			} else {
				evt.Str(key, "unknown")
			}
			evt.Msgf("Backend %s changed", key)
			changed = true
		}
	}

	updateMeta("status", newStatus, func(e *zerolog.Event) { e.Str("status", newStatus.AsString()) })
	updateMeta("role", newRole, func(e *zerolog.Event) { e.Str("role", newRole.AsString()) })
	updateMeta("readonly", newReadonly, func(e *zerolog.Event) {
		e.Bool("readonly", newReadonly.Type() == cty.Bool && !newReadonly.IsNull() && newReadonly.True())
	})
	updateMeta("connected_slaves", newSlaves, func(e *zerolog.Event) {
		s, _ := newSlaves.AsBigFloat().Int64()
		e.Int64("connected_slaves", s)
	})
	updateMeta("master_link_status", newMasterLinkStatus, func(e *zerolog.Event) { e.Str("master_link_status", newMasterLinkStatus.AsString()) })
	updateMeta("master_sync_in_progress", newMasterSyncInProgress, func(e *zerolog.Event) {
		e.Bool("master_sync_in_progress", newMasterSyncInProgress.Type() == cty.Bool && !newMasterSyncInProgress.IsNull() && newMasterSyncInProgress.True())
	})

	// Notify parent checker if any metadata changed
	if changed {
		select {
		case c.statusChan <- c.backend:
		case <-c.stopChan:
		}
	}
}

// StartPolling begins the background health check loop.
func (c *RedisCheck) StartPolling() error {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return nil
	}

	c.stopChan = make(chan struct{})
	c.ctx, c.cancel = context.WithCancel(context.Background())

	// Initialize the Redis client
	c.client = redis.NewClient(&redis.Options{
		Addr:         c.backend.Address,
		Password:     c.password,
		DialTimeout:  c.connectTimeout,
		ReadTimeout:  c.readTimeout,
		WriteTimeout: c.writeTimeout,
	})

	c.ticker = misc.NewExponentialBackoffTicker(c.defaultPeriod, c.maxPeriod, c.backoffFactor)

	c.running = true
	c.runningMu.Unlock()

	go func() {
		defer func() {
			c.runningMu.Lock()
			client := c.client
			ticker := c.ticker
			c.running = false
			c.runningMu.Unlock()

			if client != nil {
				_ = client.Close()
			}
			if ticker != nil {
				ticker.Stop()
			}
		}()

		for {
			c.updateStatus()

			select {
			case <-c.stopChan:
				return
			case <-c.ticker.C:
			}
		}
	}()

	return nil
}

// StopPolling halts the background health check loop and closes the Redis connection.
func (c *RedisCheck) StopPolling() {
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
