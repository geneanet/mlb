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
	id      string
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

func newConsul(url string, service string, default_period float64, max_period float64, backoff_factor float64, msg_chan chan consulMessage) consul {
	return consul{
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

	c.ticker = time.NewTicker(time.Duration(c.period * float64(time.Second)))

	go func() {
		defer wg.Done()
		defer func() { c.running = false }()

		var old consulServicesSlice

		for {
			services, err := c._fetch()
			if err != nil {
				log.Error().Err(err)
			} else {
				added, modified, removed := consulServicesDiff(old, services)

				for id, service := range added {
					c.msg_chan <- consulMessage{
						kind:    MsgServiceAdded,
						id:      id,
						service: service,
					}
				}

				for id, service := range modified {
					c.msg_chan <- consulMessage{
						kind:    MsgServiceAdded,
						id:      id,
						service: service,
					}
				}

				for id, service := range removed {
					c.msg_chan <- consulMessage{
						kind:    MsgServiceAdded,
						id:      id,
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

func (c *consul) _fetch() (ret_s consulServicesSlice, ret_e error) {
	defer func() {
		if r := recover(); r != nil {
			ret_e = r.(error)
		}
	}()

	resp, err := http.Get(c.url + "/v1/health/service/" + c.service + "?index=" + c.index + "&timeout=60s")
	panicIfErr(err)
	defer resp.Body.Close()

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
		id := fmt.Sprintf("%s:%d", s.Service.Address, s.Service.Port)
		index[id] = s
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

	for id, new_svc := range new_map {
		old_svc, not_new := old_map[id]

		// Updated
		if not_new && old_svc.Service.ModifyIndex != new_svc.Service.ModifyIndex {
			modified[id] = new_svc
			// New
		} else if !not_new {
			added[id] = new_svc
		}
	}

	for id, old_svc := range old_map {
		_, not_removed := new_map[id]

		// Removed
		if !not_removed {
			removed[id] = old_svc
		}
	}

	return added, modified, removed
}
