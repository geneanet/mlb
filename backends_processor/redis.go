package backends_processor

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"mlb/misc"
	"mlb/module"
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
	module.RegisterFactory("backends_processor", "redis", &RedisCheckerFactory{})
}

// RedisChecker manages multiple health checks for Redis backends.
// It subscribes to a backend source and maintains a registry of monitored Redis instances.
type RedisChecker struct {
	id             string
	checks         map[string]*RedisCheck
	checksMtex     sync.RWMutex
	password       string
	defaultPeriod  time.Duration
	maxPeriod      time.Duration
	backoffFactor  float64
	backends       *backend.Registry
	ctx            context.Context
	cancel         context.CancelFunc
	log            zerolog.Logger
	updChan        chan backend.BackendUpdate
	updChanStop    chan struct{}
	source         string
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

// RedisCheckerConfig defines the HCL configuration schema for the Redis backend processor.
type RedisCheckerConfig struct {
	ID             string  `hcl:"id,label"`
	Source         string  `hcl:"source"`
	Password       string  `hcl:"password,optional"`
	Period         string  `hcl:"period,optional"`
	MaxPeriod      string  `hcl:"max_period,optional"`
	BackoffFactor  float64 `hcl:"backoff_factor,optional"`
	ConnectTimeout string  `hcl:"connect_timeout,optional"`
	ReadTimeout    string  `hcl:"read_timeout,optional"`
	WriteTimeout   string  `hcl:"write_timeout,optional"`
}

type RedisCheckerFactory struct{}

func (w RedisCheckerFactory) ValidateConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &RedisCheckerConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Period, "period")
	config.CheckDuration(&diags, configBody.MaxPeriod, "max_period")
	config.CheckDuration(&diags, configBody.ConnectTimeout, "connect_timeout")
	config.CheckDuration(&diags, configBody.ReadTimeout, "read_timeout")
	config.CheckDuration(&diags, configBody.WriteTimeout, "write_timeout")

	return diags
}

func (w RedisCheckerFactory) parseConfig(tc *module.Config) *RedisCheckerConfig {
	config := &RedisCheckerConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode redis backend processor config")
	}
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
		config.ConnectTimeout = "1s"
	}
	if config.ReadTimeout == "" {
		config.ReadTimeout = "1s"
	}
	if config.WriteTimeout == "" {
		config.WriteTimeout = "1s"
	}
	return config
}

// New creates a new instance of the RedisChecker module.
func (w RedisCheckerFactory) New(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	c := &RedisChecker{
		id:            config.ID,
		checks:        make(map[string]*RedisCheck),
		password:      config.Password,
		backoffFactor: config.BackoffFactor,
		log:           log.With().Str("id", config.ID).Logger(),
		updChan:       make(chan backend.BackendUpdate, 100),
		updChanStop:   make(chan struct{}),
		source:        config.Source,
		backends:      backend.NewRegistry(),
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

		for {
			select {
			case b := <-statusChan:
				c.backends.Publish(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: b.Address,
					Backend: b,
				})

			case upd := <-c.updChan:
				c.checksMtex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if check, ok := c.checks[upd.Address]; ok {
						c.backends.Update(upd.Backend, "redis")
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendModified,
							Address: check.backend.Address,
							Backend: check.backend,
						})
					} else {
						c.log.Info().Str("address", upd.Address).Msg("Adding Redis check")

						check := NewRedisCheck(
							upd.Backend.Clone(),
							c.password,
							c.defaultPeriod,
							c.maxPeriod,
							c.backoffFactor,
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
					if check, ok := c.checks[upd.Address]; ok {
						c.log.Info().Str("address", upd.Address).Msg("Removing Redis check")
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

			case <-c.ctx.Done():
				return
			}
		}
	}()

	return c
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

// ReceiveUpdate implements backend.BackendUpdateSubscriber.
func (c *RedisChecker) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case c.updChan <- upd:
	case <-c.updChanStop:
	}
}

// SubscribeTo implements backend.BackendUpdateSubscriber.
func (c *RedisChecker) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(c)
}

func (c *RedisChecker) GetUpdateSource() string {
	return c.source
}

func (c *RedisChecker) GetID() string {
	return c.id
}

func (c *RedisChecker) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

// Bind initializes the module by subscribing to its configured source.
func (c *RedisChecker) Bind(modules module.ModulesRegistry) {
	c.SubscribeTo(module.Get[backend.BackendUpdateProvider](modules, c.source))
}

// RedisCheck represents a background health checker for a single Redis instance.
type RedisCheck struct {
	backend        *backend.Backend
	password       string
	period         time.Duration
	defaultPeriod  time.Duration
	maxPeriod      time.Duration
	backoffFactor  float64
	statusChan     chan *backend.Backend
	ticker         *misc.ExponentialBackoffTicker
	stopChan       chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	running        bool
	runningMu      sync.Mutex
	client         *redis.Client
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

// NewRedisCheck initializes a new health checker for a specific Redis backend.
func NewRedisCheck(backend *backend.Backend, password string, defaultPeriod time.Duration, maxPeriod time.Duration, backoffFactor float64, connectTimeout, readTimeout, writeTimeout time.Duration, statusChan chan *backend.Backend) *RedisCheck {
	c := &RedisCheck{
		backend:        backend,
		password:       password,
		period:         defaultPeriod,
		defaultPeriod:  defaultPeriod,
		maxPeriod:      maxPeriod,
		backoffFactor:  backoffFactor,
		statusChan:     statusChan,
		stopChan:       make(chan struct{}),
		running:        false,
		connectTimeout: connectTimeout,
		readTimeout:    readTimeout,
		writeTimeout:   writeTimeout,
	}
	// Pre-initialize metadata with unknown values
	backend.Meta.Set("redis", "status", cty.UnknownVal(cty.String))
	backend.Meta.Set("redis", "role", cty.UnknownVal(cty.String))
	backend.Meta.Set("redis", "readonly", cty.UnknownVal(cty.Bool))
	return c
}

func parseRoleResponse(roleResult interface{}) (retRole cty.Value, retReadonly cty.Value, err error) {
	// ROLE returns an array: [role, ...]
	if roles, ok := roleResult.([]interface{}); ok && len(roles) > 0 {
		if role, ok := roles[0].(string); ok {
			return cty.StringVal(role), cty.BoolVal(role != "master"), nil
		}
	}
	return cty.NilVal, cty.NilVal, fmt.Errorf("unexpected ROLE result format")
}

func parseInfoResponse(infoResult string) (retRole cty.Value, retReadonly cty.Value) {
	if strings.Contains(infoResult, "role:master") || strings.Contains(infoResult, "role:primary") {
		return cty.StringVal("master"), cty.BoolVal(false)
	} else if strings.Contains(infoResult, "role:slave") || strings.Contains(infoResult, "role:replica") {
		return cty.StringVal("slave"), cty.BoolVal(true)
	}
	return cty.StringVal("unknown"), cty.BoolVal(false)
}

// fetchStatus probes the Redis instance to determine its current status and role.
// It prioritizes the ROLE command and falls back to INFO replication for older versions.
func (c *RedisCheck) fetchStatus() (retStatus cty.Value, retRole cty.Value, retReadonly cty.Value, retErr error) {
	defer func() {
		// Recovery block to handle network errors or Redis restarts.
		// If an error occurs, we attempt to recreate the client and apply an exponential backoff.
		if r := recover(); r != nil {
			retStatus = cty.StringVal("err")
			retRole = cty.UnknownVal(cty.String)
			retReadonly = cty.UnknownVal(cty.Bool)
			if e, ok := r.(error); ok {
				retErr = e
			} else {
				retErr = fmt.Errorf("%v", r)
			}

			// Reconnect on error to ensure we start from a clean state next iteration.
			log.Info().Str("address", c.backend.Address).Msg("Reconnecting Redis")
			if c.client != nil {
				c.client.Close()
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

	ctx, cancel := context.WithTimeout(c.ctx, c.readTimeout)
	defer cancel()

	// Use ROLE command (available since Redis 2.8.12) which returns a structured array.
	roleResult, err := c.client.Do(ctx, "ROLE").Result()
	if err != nil {
		// Fallback to INFO replication if ROLE is not available or fails.
		// This parses the raw "role:master" or "role:slave" string from INFO replication output.
		infoResult, err := c.client.Info(ctx, "replication").Result()
		if err != nil {
			panic(err)
		}
		retRole, retReadonly = parseInfoResponse(infoResult)
	} else {
		retRole, retReadonly, err = parseRoleResponse(roleResult)
		if err != nil {
			panic(err)
		}
	}

	// Reset polling frequency on success.
	if period, updated := c.ticker.Reset(); updated {
		log.Warn().Str("address", c.backend.Address).Dur("period", period).Msg("Updating fetch period")
	}

	return cty.StringVal("ok"), retRole, retReadonly, nil
}

// updateStatus executes a single health check and updates the backend's metadata.
func (c *RedisCheck) updateStatus() {
	newStatus, newRole, newReadonly, err := c.fetchStatus()

	if err != nil {
		log.Error().Str("address", c.backend.Address).Err(err).Msg("Error while fetching status from Redis backend")
	}

	changed := false

	// Update status metadata
	oldStatus, ok := c.backend.Meta.Get("redis", "status")
	if !ok || !oldStatus.IsKnown() || oldStatus.Equals(newStatus).False() {
		c.backend.Meta.Set("redis", "status", newStatus)
		if newStatus.IsKnown() {
			log.Info().Str("address", c.backend.Address).Str("status", newStatus.AsString()).Msg("Backend status changed")
		} else {
			log.Info().Str("address", c.backend.Address).Str("status", "unknown").Msg("Backend status changed")
		}
		changed = true
	}

	// Update role metadata
	oldRole, ok := c.backend.Meta.Get("redis", "role")
	if !ok || !oldRole.IsKnown() || oldRole.Equals(newRole).False() {
		c.backend.Meta.Set("redis", "role", newRole)
		if newRole.IsKnown() {
			log.Info().Str("address", c.backend.Address).Str("role", newRole.AsString()).Msg("Backend role changed")
		} else {
			log.Info().Str("address", c.backend.Address).Str("role", "unknown").Msg("Backend role changed")
		}
		changed = true
	}

	// Update readonly metadata
	oldReadonly, ok := c.backend.Meta.Get("redis", "readonly")
	if !ok || !oldReadonly.IsKnown() || oldReadonly.Equals(newReadonly).False() {
		c.backend.Meta.Set("redis", "readonly", newReadonly)
		if newReadonly.IsKnown() {
			log.Info().Str("address", c.backend.Address).Bool("readonly", newReadonly.True()).Msg("Backend readonly changed")
		} else {
			log.Info().Str("address", c.backend.Address).Str("readonly", "unknown").Msg("Backend readonly changed")
		}
		changed = true
	}

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
				client.Close()
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
