package consul_kv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/config"
	"mlb/misc"
	"mlb/module"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"
)

func init() {
	module.RegisterFactory("backends_processor", "consul_kv", newConsulKV, validateConsulKVConfig)
}

// ConsulKV implements a backend processor that fetches metadata from Consul KV.
type ConsulKV struct {
	id            string
	url           string
	defaultPeriod time.Duration
	maxPeriod     time.Duration
	backoffFactor float64
	backends      *backend.Registry
	defaultValues map[string]cty.Value
	ctx           context.Context
	cancel        context.CancelFunc
	log           zerolog.Logger
	updChan       chan backend.BackendUpdate
	updChanStop   chan struct{}
	source        string
	evalCtx       *hcl.EvalContext
	watchers      map[string][]*consulKVWatcher
}

// ConsulKVConfig defines the HCL configuration for the Consul KV processor.
type ConsulKVConfig struct {
	ID            string                `hcl:"id,label"`
	Source        string                `hcl:"source"`
	URL           string                `hcl:"url"`
	Period        string                `hcl:"period,optional"`
	MaxPeriod     string                `hcl:"max_period,optional"`
	BackoffFactor float64               `hcl:"backoff_factor,optional"`
	Values        []ConsulKVValueConfig `hcl:"value,block"`
}

// ConsulKVValueConfig defines a single Consul KV watch.
type ConsulKVValueConfig struct {
	ID        string         `hcl:"id,label"`
	ConsulKey hcl.Expression `hcl:"consul_key"`
	Default   string         `hcl:"default"`
}

// validateConsulKVConfig validates the Consul KV configuration.
func validateConsulKVConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &ConsulKVConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Period, "period")
	config.CheckDuration(&diags, configBody.MaxPeriod, "max_period")

	return diags
}

func parseConsulKVConfig(tc *module.Config) *ConsulKVConfig {
	config := &ConsulKVConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode consul kv backend processor config")
	}
	config.ID = tc.FullID()
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

func newConsulKV(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseConsulKVConfig(tc)

	c := &ConsulKV{
		id:            config.ID,
		url:           config.URL,
		backoffFactor: config.BackoffFactor,
		log:           log.With().Str("id", config.ID).Logger(),
		updChan:       make(chan backend.BackendUpdate, 100),
		updChanStop:   make(chan struct{}),
		source:        config.Source,
		backends:      backend.NewRegistry(),
		defaultValues: make(map[string]cty.Value),
		evalCtx:       tc.Ctx,
		watchers:      make(map[string][]*consulKVWatcher),
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

	// Default values
	for _, v := range config.Values {
		c.defaultValues[v.ID] = cty.StringVal(v.Default)
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("Consul KV watcher starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("Consul KV watcher stopped")
		defer c.cancel()
		defer close(c.updChanStop)

		watcherChan := make(chan *consulKVWatcherMessage)

	mainloop:
		for {
			select {
			case msg := <-watcherChan:
				// Update metadata
				msg.backend.Meta.Set("consul_kv", msg.id, cty.StringVal(msg.value))

				// Send the update
				c.backends.Publish(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: msg.backend.Address,
					Backend: msg.backend,
				})
			case upd := <-c.updChan: // Backends changed
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					// Add/Update the backend while preserving the consul_kv bucket
					c.backends.Update(upd.Backend, "consul_kv")

					// Set default values if they are missing
					b := c.backends.Get(upd.Address)
					for _, v := range config.Values {
						if _, ok := b.Meta.Get("consul_kv", v.ID); !ok {
							b.Meta.Set("consul_kv", v.ID, cty.StringVal(v.Default))
						}
					}

					// First, cancel every watcher we may have for the backend
					if _, ok := c.watchers[upd.Address]; ok {
						for _, w := range c.watchers[upd.Address] {
							w.cancel()
						}
						delete(c.watchers, upd.Address)
					}

					// Start a watcher for every requested value
					for _, v := range config.Values {
						var consulKey string
						known, diags := upd.Backend.ResolveExpression(v.ConsulKey, c.evalCtx, &consulKey)
						if diags.HasErrors() {
							c.log.Error().Msg(diags.Error())
						}
						if known {
							if _, ok := c.watchers[upd.Address]; !ok {
								c.watchers[upd.Address] = []*consulKVWatcher{}
							}
							w := newConsulKVWatcher(c.backends.Get(upd.Address), v.ID, c.url, consulKey, c.defaultPeriod, c.maxPeriod, c.backoffFactor, watcherChan, c.ctx, c.log)
							c.watchers[upd.Address] = append(c.watchers[upd.Address], w)
						}
					}

					// Send the update
					c.backends.Publish(backend.BackendUpdate{
						Kind:    upd.Kind,
						Address: upd.Address,
						Backend: c.backends.Get(upd.Address),
					})
				case backend.UpdBackendRemoved:
					// If we actually have the backend
					if c.backends.Has(upd.Address) {
						// Cancel every watcher we may have for the backend
						if _, ok := c.watchers[upd.Address]; ok {
							for _, w := range c.watchers[upd.Address] {
								w.cancel()
							}
							delete(c.watchers, upd.Address)
						}

						// Remove the backend
						c.backends.Remove(upd.Address)

						// Send the update
						c.backends.Publish(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
						})
					}
				}
			case <-c.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return c, nil
}

// ProvideUpdates registers a subscriber and sends initial updates for all currently matched backends.
func (c *ConsulKV) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

// ReceiveUpdate implements the backend.BackendUpdateSubscriber interface.
func (c *ConsulKV) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case c.updChan <- upd:
	case <-c.updChanStop:
	}
}

// GetBackendList returns the current list of backends with their Consul KV metadata.
func (c *ConsulKV) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

// Bind cross-links the processor with its source backend provider.
func (c *ConsulKV) Bind(modules module.ModulesRegistry) error {
	m, err := module.Get[backend.BackendUpdateProvider](modules, c.source)
	if err != nil {
		return err
	}
	m.ProvideUpdates(c)
	return nil
}

// Watcher

// consulKVWatcherMessage is used to communicate value changes from a watcher to the main loop.
type consulKVWatcherMessage struct {
	backend *backend.Backend
	id      string
	value   string
}

// consulKVWatcher polls a specific key in Consul for a backend and value ID.
type consulKVWatcher struct {
	backend *backend.Backend
	id      string
	url     string
	key     string
	channel chan *consulKVWatcherMessage
	ctx     context.Context
	cancel  context.CancelFunc
	ticker  *misc.ExponentialBackoffTicker
	log     zerolog.Logger
	index   string
}

// consulKVValue represents the structure of a Consul KV response.
type consulKVValue struct {
	Key   string
	Value string
}

// newConsulKVWatcher creates and starts a new consulKVWatcher.
func newConsulKVWatcher(backend *backend.Backend, id string, url string, key string, defaultPeriod time.Duration, maxPeriod time.Duration, backoffFactor float64, channel chan *consulKVWatcherMessage, ctx context.Context, log zerolog.Logger) *consulKVWatcher {
	w := &consulKVWatcher{
		backend: backend,
		id:      id,
		url:     url,
		key:     key,
		channel: channel,
		log:     log.With().Str("backend", backend.Address).Str("key", key).Logger(),
	}

	w.ctx, w.cancel = context.WithCancel(ctx)

	w.log.Info().Msg("Polling Consul")

	w.ticker = misc.NewExponentialBackoffTicker(defaultPeriod, maxPeriod, backoffFactor)

	go func() {
		defer w.log.Info().Msg("Consul polling stopped")
		defer w.cancel()
		defer w.ticker.Stop()

		oldValue := cty.UnknownVal(cty.String)

		for {
			value, err := w.fetch()

			if errors.Is(err, context.Canceled) {
				return
			} else if err != nil {
				w.log.Error().Err(err).Msg("Error while fetching data")
				if period, updated := w.ticker.ApplyBackoff(); updated {
					w.log.Warn().Dur("period", period).Msg("Updating fetch period")
				}
			} else {
				if period, updated := w.ticker.Reset(); updated {
					w.log.Warn().Dur("period", period).Msg("Updating fetch period")
				}

				// Value has changed
				if cty.UnknownAsNull(oldValue).Equals(cty.UnknownAsNull(value)).False() {
					var valStr string
					if value.IsKnown() && !value.IsNull() {
						valStr = value.AsString()
					}
					w.log.Info().Str("value", valStr).Msg("Value changed")

					w.channel <- &consulKVWatcherMessage{
						backend: w.backend,
						id:      w.id,
						value:   valStr,
					}
				}

				oldValue = value
			}

			select {
			case <-w.ticker.C: // Wait next iteration
			case <-w.ctx.Done(): // Context cancelled
				return
			}
		}

	}()

	return w
}

// fetch performs a long-polling request to Consul to get the current value of the key.
func (w *consulKVWatcher) fetch() (retValue cty.Value, retError error) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				retError = e
			} else {
				retError = fmt.Errorf("%v", r)
			}
		}
	}()

	w.log.Debug().Msg("Fetching key")

	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()

	rq, err := http.NewRequestWithContext(ctx, "GET", w.url+"/v1/kv/"+w.key+"?index="+w.index+"&timeout=60s", nil)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(rq)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	w.log.Debug().Int("status", resp.StatusCode).Msg("Key value fetched")

	if resp.StatusCode == 404 {
		return cty.UnknownVal(cty.String), nil
	} else if resp.StatusCode != 200 {
		panic(fmt.Errorf("unexpected status code %s", resp.Status))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	data := []consulKVValue{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		panic(err)
	}

	dataDecoded, err := base64.StdEncoding.DecodeString(data[0].Value)
	if err != nil {
		panic(err)
	}

	dataStr := string(dataDecoded)

	w.log.Debug().Str("value", dataStr).Msg("Key fetched")

	w.index = resp.Header.Get("X-Consul-Index")

	return cty.StringVal(dataStr), nil
}
