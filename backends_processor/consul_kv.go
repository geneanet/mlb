package backends_processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
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
	factories["consul_kv"] = &ConsulKVFactory{}
}

type ConsulKV struct {
	id             string
	url            string
	default_period time.Duration
	max_period     time.Duration
	backoff_factor float64
	backends       *backend.BackendsMap
	backends_mutex sync.RWMutex
	default_values map[string]cty.Value
	subscribers    []backend.BackendUpdateSubscriber
	ctx            context.Context
	cancel         context.CancelFunc
	log            zerolog.Logger
	upd_chan       chan backend.BackendUpdate
	upd_chan_stop  chan struct{}
	source         string
	evalCtx        *hcl.EvalContext
	watchers       map[string][]*consulKVWatcher
}

type ConsulKVConfig struct {
	ID            string                `hcl:"id,label"`
	Source        string                `hcl:"source"`
	URL           string                `hcl:"url"`
	Period        string                `hcl:"period,optional"`
	MaxPeriod     string                `hcl:"max_period,optional"`
	BackoffFactor float64               `hcl:"backoff_factor,optional"`
	Values        []ConsulKVValueConfig `hcl:"value,block"`
}

type ConsulKVValueConfig struct {
	ID        string         `hcl:"id,label"`
	ConsulKey hcl.Expression `hcl:"consul_key"`
	Default   string         `hcl:"default"`
}

type ConsulKVFactory struct{}

func (w ConsulKVFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &ConsulKVConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w ConsulKVFactory) parseConfig(tc *Config) *ConsulKVConfig {
	config := &ConsulKVConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("backends_processor.%s.%s", tc.Type, tc.Name)
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

func (w ConsulKVFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	c := &ConsulKV{
		id:             config.ID,
		url:            config.URL,
		backoff_factor: config.BackoffFactor,
		log:            log.With().Str("id", config.ID).Logger(),
		upd_chan:       make(chan backend.BackendUpdate),
		upd_chan_stop:  make(chan struct{}),
		source:         config.Source,
		backends:       backend.NewBackendsMap(),
		default_values: make(map[string]cty.Value),
		subscribers:    []backend.BackendUpdateSubscriber{},
		evalCtx:        tc.ctx,
		watchers:       make(map[string][]*consulKVWatcher),
	}

	var err error

	c.default_period, err = time.ParseDuration(config.Period)
	misc.PanicIfErr(err)
	c.max_period, err = time.ParseDuration(config.MaxPeriod)
	misc.PanicIfErr(err)

	// Default values
	for _, v := range config.Values {
		c.default_values[v.ID] = cty.StringVal(v.Default)
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Msg("Consul KV watcher starting")

	go func() {
		defer wg.Done()
		defer c.log.Info().Msg("Consul KV watcher stopped")
		defer c.cancel()
		defer close(c.upd_chan_stop)

		watcher_chan := make(chan *consulKVWatcherMessage)

	mainloop:
		for {
			select {
			case msg := <-watcher_chan:
				// Update metadata
				msg.backend.Meta.Set("consul_kv", msg.id, cty.StringVal(msg.value))

				// Send the update
				c.sendUpdate(backend.BackendUpdate{
					Kind:    backend.UpdBackendModified,
					Address: msg.backend.Address,
					Backend: msg.backend,
				})
			case upd := <-c.upd_chan: // Backends changed
				c.backends_mutex.Lock()
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					// Add/Update the backend
					c.backends.Add(upd.Backend.Clone())

					// Set default values
					for _, v := range config.Values {
						c.backends.Get(upd.Address).Meta.Set("consul_kv", v.ID, cty.StringVal(v.Default))
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
						var consul_key string
						known, diags := upd.Backend.ResolveExpression(v.ConsulKey, c.evalCtx, &consul_key)
						if diags.HasErrors() {
							c.log.Error().Msg(diags.Error())
						}
						if known {
							if _, ok := c.watchers[upd.Address]; !ok {
								c.watchers[upd.Address] = []*consulKVWatcher{}
							}
							w := newConsulKVWatcher(c.backends.Get(upd.Address), v.ID, c.url, consul_key, c.default_period, c.max_period, c.backoff_factor, watcher_chan, c.ctx, c.log)
							c.watchers[upd.Address] = append(c.watchers[upd.Address], w)
						}
					}

					// Send the update
					c.sendUpdate(backend.BackendUpdate{
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
						c.sendUpdate(backend.BackendUpdate{
							Kind:    backend.UpdBackendRemoved,
							Address: upd.Address,
						})
					}
				}
				c.backends_mutex.Unlock()
			case <-c.ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return c
}

func (c *ConsulKV) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.subscribers = append(c.subscribers, s)

	go func() {
		c.backends_mutex.RLock()
		defer c.backends_mutex.RUnlock()

		for _, b := range c.backends.GetList() {
			c.sendUpdate(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: b.Address,
				Backend: b,
			})
		}
	}()
}

func (c *ConsulKV) sendUpdate(u backend.BackendUpdate) {
	for _, s := range c.subscribers {
		s.ReceiveUpdate(u)
	}
}

func (c *ConsulKV) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case c.upd_chan <- upd:
	case <-c.upd_chan_stop:
	}
}

func (c *ConsulKV) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(c)
}

func (c *ConsulKV) GetUpdateSource() string {
	return c.source
}

func (c *ConsulKV) GetID() string {
	return c.id
}

func (c *ConsulKV) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

func (c *ConsulKV) Bind(modules module.ModulesList) {
	c.SubscribeTo(modules.GetBackendUpdateProvider(c.source))
}

// Watcher

type consulKVWatcherMessage struct {
	backend *backend.Backend
	id      string
	value   string
}

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

type consulKVValue struct {
	Key   string
	Value string
}

func newConsulKVWatcher(backend *backend.Backend, id string, url string, key string, default_period time.Duration, max_period time.Duration, backoff_factor float64, channel chan *consulKVWatcherMessage, ctx context.Context, log zerolog.Logger) *consulKVWatcher {
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

	w.ticker = misc.NewExponentialBackoffTicker(default_period, max_period, backoff_factor)

	go func() {
		defer w.log.Info().Msg("Consul polling stopped")
		defer w.cancel()
		defer w.ticker.Stop()

		old_value := cty.UnknownVal(cty.String)

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
				if cty.UnknownAsNull(old_value).Equals(cty.UnknownAsNull(value)).False() {
					w.log.Info().Str("value", value.AsString()).Msg("Value changed")

					w.channel <- &consulKVWatcherMessage{
						backend: w.backend,
						id:      w.id,
						value:   value.AsString(),
					}
				}

				old_value = value
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

func (w *consulKVWatcher) fetch() (ret_v cty.Value, ret_e error) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			ret_e = misc.EnsureError(r)
		}
	}()

	w.log.Debug().Msg("Fetching key")

	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()

	rq, err := http.NewRequestWithContext(ctx, "GET", w.url+"/v1/kv/"+w.key+"?index="+w.index+"&timeout=60s", nil)
	misc.PanicIfErr(err)

	resp, err := http.DefaultClient.Do(rq)
	misc.PanicIfErr(err)
	defer resp.Body.Close()

	w.log.Debug().Int("status", resp.StatusCode).Msg("Key value fetched")

	if resp.StatusCode == 404 {
		return cty.UnknownVal(cty.String), nil
	} else if resp.StatusCode != 200 {
		panic(fmt.Errorf("unexpected status code %s", resp.Status))
	}

	body, err := io.ReadAll(resp.Body)
	misc.PanicIfErr(err)

	data := []consulKVValue{}
	err = json.Unmarshal(body, &data)
	misc.PanicIfErr(err)

	data_decoded, err := base64.StdEncoding.DecodeString(data[0].Value)
	misc.PanicIfErr(err)

	data_str := string(data_decoded)

	w.log.Debug().Str("value", data_str).Msg("Key fetched")

	w.index = resp.Header.Get("X-Consul-Index")

	return cty.StringVal(data_str), nil
}
