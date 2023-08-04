package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"mlb/backend"
	"mlb/misc"
)

type consulService struct {
	Service struct {
		Tags    []string
		Address string
		Port    int
		Weights struct {
			Passing int
			Warning int
		}
		ModifyIndex int
	}
}

type consulServicesMap map[string]consulService
type consulServicesSlice []consulService

type InventoryConsul struct {
	fullname       string
	url            string
	service        string
	period         time.Duration
	default_period time.Duration
	max_period     time.Duration
	backoff_factor float64
	index          string
	ticker         *time.Ticker
	ctx            context.Context
	cancel         context.CancelFunc
	subscribers    []chan backend.BackendMessage
	backends       map[string]*backend.Backend
	backends_mutex sync.RWMutex
	log            zerolog.Logger
}

type ConsulInventoryConfig struct {
	FullName      string  `hcl:"name,label"`
	URL           string  `hcl:"url"`
	Service       string  `hcl:"service"`
	Period        string  `hcl:"period"`
	MaxPeriod     string  `hcl:"max_period"`
	BackoffFactor float64 `hcl:"backoff_factor"`
}

func NewInventoryConsul(config *ConsulInventoryConfig, wg *sync.WaitGroup, ctx context.Context) *InventoryConsul {
	c := &InventoryConsul{
		fullname:       config.FullName,
		url:            config.URL,
		service:        config.Service,
		backoff_factor: config.BackoffFactor,
		subscribers:    make([]chan backend.BackendMessage, 0),
		backends:       make(map[string]*backend.Backend),
		log:            log.With().Str("id", config.FullName).Logger(),
	}

	var err error

	c.default_period, err = time.ParseDuration(config.Period)
	misc.PanicIfErr(err)
	c.period = c.default_period

	c.max_period, err = time.ParseDuration(config.MaxPeriod)
	misc.PanicIfErr(err)

	c.ctx, c.cancel = context.WithCancel(ctx)

	wg.Add(1)
	c.log.Info().Str("url", c.url).Msg("Polling Consul")

	c.ticker = time.NewTicker(c.period)

	go func() {
		defer wg.Done()
		defer c.log.Info().Str("url", c.url).Msg("Consul polling stopped")
		defer c.cancel()

		var old consulServicesSlice

	mainloop:
		for {
			services, err := c.fetch()

			if errors.Is(err, context.Canceled) {
				return
			} else if err != nil {
				c.log.Error().Err(err).Msg("Error while fetching service list from Consul")
				c.applyBackoff()
			} else {
				c.resetPeriod()

				added, modified, removed := consulServicesDiff(old, services)

				c.backends_mutex.Lock()

				for address, service := range added {
					c.backends[address] = &backend.Backend{
						Address: address,
						Status:  "unk",
						Tags:    service.Service.Tags,
						Weight:  service.Service.Weights.Passing,
					}
					c.sendMessage(backend.BackendMessage{
						Kind:    backend.MsgBackendAdded,
						Address: address,
						Backend: c.backends[address],
					})
				}

				for address, service := range modified {
					c.backends[address].UpdateTags(service.Service.Tags)
					c.backends[address].Weight = service.Service.Weights.Passing
					c.sendMessage(backend.BackendMessage{
						Kind:    backend.MsgBackendModified,
						Address: address,
						Backend: c.backends[address],
					})
				}

				for address := range removed {
					delete(c.backends, address)
					c.sendMessage(backend.BackendMessage{
						Kind:    backend.MsgBackendRemoved,
						Address: address,
					})
				}

				c.backends_mutex.Unlock()

				old = services
			}

			select {
			case <-c.ticker.C: // Wait next iteration
			case <-c.ctx.Done(): // Context cancelled
				c.ticker.Stop()
				break mainloop
			}
		}
	}()

	return c
}

func (c *InventoryConsul) Subscribe() chan backend.BackendMessage {
	ch := make(chan backend.BackendMessage)
	c.subscribers = append(c.subscribers, ch)

	go func() {
		c.backends_mutex.RLock()
		defer c.backends_mutex.RUnlock()

		for _, b := range c.backends {
			c.sendMessage(backend.BackendMessage{
				Kind:    backend.MsgBackendAdded,
				Address: b.Address,
				Backend: b,
			})
		}
	}()

	return ch
}

func (c *InventoryConsul) sendMessage(m backend.BackendMessage) {
	for _, s := range c.subscribers {
		s <- m
	}
}

func (c *InventoryConsul) updatePeriod(period time.Duration) {
	if c.period != period {
		c.period = period
		c.ticker.Reset(c.period)
		c.log.Warn().Dur("period", c.period).Msg("Updating Consul fetch period")
	}
}

func (c *InventoryConsul) resetPeriod() {
	c.updatePeriod(c.default_period)
}

func (c *InventoryConsul) applyBackoff() {
	new_period := time.Duration(float64(c.period) * c.backoff_factor)
	if new_period > c.max_period {
		new_period = c.max_period
	}
	c.updatePeriod(new_period)
}

func (c *InventoryConsul) fetch() (ret_s consulServicesSlice, ret_e error) {
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			ret_e = r.(error)
		}
	}()

	c.log.Debug().Msg("Fetching new service list from Consul")

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	rq, err := http.NewRequestWithContext(ctx, "GET", c.url+"/v1/health/service/"+c.service+"?index="+c.index+"&timeout=60s", nil)
	misc.PanicIfErr(err)

	resp, err := http.DefaultClient.Do(rq)
	misc.PanicIfErr(err)
	defer resp.Body.Close()

	c.log.Debug().Int("status", resp.StatusCode).Msg("Service list fetched")

	if resp.StatusCode != 200 {
		panic(fmt.Errorf("unexpected status code %s", resp.Status))
	}

	body, err := io.ReadAll(resp.Body)
	misc.PanicIfErr(err)

	var data consulServicesSlice
	err = json.Unmarshal(body, &data)
	misc.PanicIfErr(err)

	c.index = resp.Header.Get("X-Consul-Index")

	return data, nil
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

	old_map := consulServicesSliceToMap(old)
	new_map := consulServicesSliceToMap(new)

	for address, new_svc := range new_map {
		old_svc, not_new := old_map[address]

		// Updated
		if not_new && old_svc.Service.ModifyIndex != new_svc.Service.ModifyIndex {
			modified[address] = new_svc
			// New
		} else if !not_new {
			added[address] = new_svc
		}
	}

	for address, old_svc := range old_map {
		_, not_removed := new_map[address]

		// Removed
		if !not_removed {
			removed[address] = old_svc
		}
	}

	return added, modified, removed
}
