package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
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

type consulMessageKind int

const (
	MsgServiceAdded consulMessageKind = iota
	MsgServiceModified
	MsgServiceRemoved
)

type consulMessage struct {
	kind    consulMessageKind
	address string
	service consulService
}

type consul struct {
	url            string
	service        string
	period         float64
	default_period float64
	max_period     float64
	backoff_factor float64
	running        bool
	index          string
	ticker         *time.Ticker
	msg_chan       chan consulMessage
}

func newConsul(url string, service string, default_period float64, max_period float64, backoff_factor float64, msg_chan chan consulMessage) *consul {
	return &consul{
		url:            url,
		service:        service,
		period:         default_period,
		default_period: default_period,
		max_period:     max_period,
		backoff_factor: backoff_factor,
		msg_chan:       msg_chan,
		running:        false,
	}
}

func (c *consul) start(wg *sync.WaitGroup) {
	if c.running {
		return
	}

	c.running = true
	wg.Add(1)

	log.Info().Str("url", c.url).Msg("Polling Consul")

	c.ticker = time.NewTicker(time.Duration(c.period * float64(time.Second)))

	go func() {
		defer wg.Done()
		defer func() { c.running = false }()

		var old consulServicesSlice

		for {
			services, err := c._fetch()
			if err != nil {
				log.Error().Err(err).Msg("Error while fetching service list from Consul")
				c._applyBackoff()
			} else {
				c._resetPeriod()

				added, modified, removed := consulServicesDiff(old, services)

				for address, service := range added {
					c.msg_chan <- consulMessage{
						kind:    MsgServiceAdded,
						address: address,
						service: service,
					}
				}

				for address, service := range modified {
					c.msg_chan <- consulMessage{
						kind:    MsgServiceAdded,
						address: address,
						service: service,
					}
				}

				for address, service := range removed {
					c.msg_chan <- consulMessage{
						kind:    MsgServiceAdded,
						address: address,
						service: service,
					}
				}

				old = services
			}

			// Wait next iteration
			<-c.ticker.C
		}
	}()
}

func (c *consul) _updatePeriod(period float64) {
	if c.running && (c.period != period) {
		c.period = period
		c.ticker.Reset(time.Duration(c.period * float64(time.Second)))
		log.Warn().Float64("period", c.period).Msg("Updating Consul fetch period")
	}
}

func (c *consul) _resetPeriod() {
	c._updatePeriod(c.default_period)
}

func (c *consul) _applyBackoff() {
	new_period := c.period * c.backoff_factor
	if new_period > c.max_period {
		new_period = c.max_period
	}
	c._updatePeriod(new_period)
}

func (c *consul) _fetch() (ret_s consulServicesSlice, ret_e error) {
	defer func() {
		if r := recover(); r != nil {
			ret_e = r.(error)
		}
	}()

	log.Debug().Msg("Fetching new service list from Consul")

	resp, err := http.Get(c.url + "/v1/health/service/" + c.service + "?index=" + c.index + "&timeout=60s")
	panicIfErr(err)
	defer resp.Body.Close()

	log.Debug().Int("status", resp.StatusCode).Msg("Service list fetched")

	if resp.StatusCode != 200 {
		panic(fmt.Sprintf("Unexpected status code %s", resp.Status))
	}

	body, err := io.ReadAll(resp.Body)
	panicIfErr(err)

	var data consulServicesSlice
	err = json.Unmarshal(body, &data)
	panicIfErr(err)

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
