package consul

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"

	"mlb/backend"
	"mlb/config"
	"mlb/misc"
	"mlb/module"
)

func init() {
	module.RegisterFactory("backends_inventory", "consul", newConsulBackendsInventory, validateConsulBackendsInventoryConfig)
}

type consulService struct {
	Node struct {
		Node string
	}
	Service struct {
		Tags    []string
		Address string
		Port    int
		Weights struct {
			Passing uint64
			Warning uint64
		}
		ModifyIndex int
	}
}

type consulServicesMap map[string]consulService
type consulServicesSlice []consulService

// BackendsInventoryConsul implements a Consul-based backend discovery.
type BackendsInventoryConsul struct {
	id       string
	url      string
	service  string
	index    string
	ticker   *misc.ExponentialBackoffTicker
	ctx      context.Context
	cancel   context.CancelFunc
	backends *backend.Registry
	log      zerolog.Logger
}

// ConsulBackendsInventoryConfig defines the HCL configuration for Consul discovery.
type ConsulBackendsInventoryConfig struct {
	ID                string  `hcl:"id,label"`
	URL               string  `hcl:"url"`
	Service           string  `hcl:"service"`
	Period            string  `hcl:"period,optional"`
	MaxPeriod         string  `hcl:"max_period,optional"`
	BackoffFactor     float64 `hcl:"backoff_factor,optional"`
	LogBackendUpdates bool    `hcl:"log_backend_updates,optional"`
}

// validateConsulBackendsInventoryConfig validates the Consul discovery configuration.
func validateConsulBackendsInventoryConfig(tc *module.Config) hcl.Diagnostics {
	configBody := &ConsulBackendsInventoryConfig{}
	diags := gohcl.DecodeBody(tc.Config, tc.Ctx, configBody)

	config.CheckDuration(&diags, configBody.Period, "period")
	config.CheckDuration(&diags, configBody.MaxPeriod, "max_period")

	return diags
}

func parseConsulBackendsInventoryConfig(tc *module.Config) *ConsulBackendsInventoryConfig {
	config := &ConsulBackendsInventoryConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode consul backend inventory config")
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
	return config
}

func newConsulBackendsInventory(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
	config := parseConsulBackendsInventoryConfig(tc)

	c := &BackendsInventoryConsul{
		id:       config.ID,
		url:      config.URL,
		service:  config.Service,
		log:      log.With().Str("id", config.ID).Logger(),
		backends: backend.NewRegistry(log.With().Str("id", config.ID).Logger(), config.LogBackendUpdates),
	}

	var err error

	defaultPeriod, err := time.ParseDuration(config.Period)
	if err != nil {
		return nil, err
	}

	maxPeriod, err := time.ParseDuration(config.MaxPeriod)
	if err != nil {
		return nil, err
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Str("url", c.url).Msg("Polling Consul")

	c.ticker = misc.NewExponentialBackoffTicker(defaultPeriod, maxPeriod, config.BackoffFactor)

	go func() {
		defer wg.Done()
		defer c.log.Info().Str("url", c.url).Msg("Consul polling stopped")
		defer c.cancel()
		defer c.ticker.Stop()

		var old consulServicesSlice

		for {
			services, err := c.fetch()

			if errors.Is(err, context.Canceled) {
				return
			} else if err != nil {
				c.log.Error().Err(err).Msg("Error while fetching service list from Consul")
				if period, updated := c.ticker.ApplyBackoff(); updated {
					c.log.Warn().Dur("period", period).Msg("Updating fetch period")
				}
			} else {
				if period, updated := c.ticker.Reset(); updated {
					c.log.Warn().Dur("period", period).Msg("Updating fetch period")
				}

				added, modified, removed := consulServicesDiff(old, services)

				for address, service := range added {
					c.backends.Add(&backend.Backend{
						Address: address,
						Meta: backend.NewMetaMap(map[string]backend.MetaBucket{
							"consul": {
								"node":   cty.StringVal(service.Node.Node),
								"weight": cty.NumberUIntVal(service.Service.Weights.Passing),
								"tags":   ctyTagSet(service.Service.Tags),
							},
						}),
					})
					c.backends.Publish(backend.BackendUpdate{
						Kind:    backend.UpdBackendAdded,
						Address: address,
						Backend: c.backends.Get(address),
					})
				}

				for address, service := range modified {
					b := c.backends.Get(address)
					b.Meta.Set("consul", "tags", ctyTagSet(service.Service.Tags))
					b.Meta.Set("consul", "weight", cty.NumberUIntVal(service.Service.Weights.Passing))
					b.Meta.Set("consul", "node", cty.StringVal(service.Node.Node))
					c.backends.Publish(backend.BackendUpdate{
						Kind:    backend.UpdBackendModified,
						Address: address,
						Backend: b,
					})
				}

				for address := range removed {
					c.backends.Remove(address)
					c.backends.Publish(backend.BackendUpdate{
						Kind:    backend.UpdBackendRemoved,
						Address: address,
					})
				}

				old = services
			}

			select {
			case <-c.ticker.C: // Wait next iteration
			case <-c.ctx.Done(): // Context cancelled
				return
			}
		}
	}()

	return c, nil
}

func (c *BackendsInventoryConsul) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

func (c *BackendsInventoryConsul) fetch() (retServices consulServicesSlice, retError error) {
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

	c.log.Debug().Msg("Fetching new service list from Consul")

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	rq, err := http.NewRequestWithContext(ctx, "GET", c.url+"/v1/health/service/"+c.service+"?index="+c.index+"&timeout=60s", nil)
	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(rq)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	c.log.Debug().Int("status", resp.StatusCode).Msg("Service list fetched")

	if resp.StatusCode != 200 {
		panic(fmt.Errorf("unexpected status code %s", resp.Status))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var data consulServicesSlice
	err = json.Unmarshal(body, &data)
	if err != nil {
		panic(err)
	}

	c.index = resp.Header.Get("X-Consul-Index")

	return data, nil
}

func (c *BackendsInventoryConsul) GetBackendList() backend.BackendsList {
	return c.backends.GetList()
}

func consulServicesSliceToMap(services consulServicesSlice) consulServicesMap {
	index := consulServicesMap{}

	for _, s := range services {
		address := fmt.Sprintf("%s:%d", s.Service.Address, s.Service.Port)
		index[address] = s
	}

	return index
}

func consulServicesDiff(old consulServicesSlice, new consulServicesSlice) (added consulServicesMap, modified consulServicesMap, removed consulServicesMap) {
	added = consulServicesMap{}
	modified = consulServicesMap{}
	removed = consulServicesMap{}

	if new == nil {
		new = consulServicesSlice{}
	}

	if old == nil {
		old = consulServicesSlice{}
	}

	oldMap := consulServicesSliceToMap(old)
	newMap := consulServicesSliceToMap(new)

	for address, newSvc := range newMap {
		oldSvc, notNew := oldMap[address]

		// Updated
		if notNew && oldSvc.Service.ModifyIndex != newSvc.Service.ModifyIndex {
			modified[address] = newSvc
			// New
		} else if !notNew {
			added[address] = newSvc
		}
	}

	for address, oldSvc := range oldMap {
		_, notRemoved := newMap[address]

		// Removed
		if !notRemoved {
			removed[address] = oldSvc
		}
	}

	return added, modified, removed
}

func ctyTagSet(tags []string) cty.Value {
	result, _ := gocty.ToCtyValue(tags, cty.Set(cty.String))
	return result
}
