package backends_inventory

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"mlb/backend"
	"mlb/module"
)

func init() {
	factories["static"] = &staticBackendsInventoryFactory{}
}

type BackendsInventoryStatic struct {
	id          string
	ctx         context.Context
	cancel      context.CancelFunc
	subscribers []chan backend.BackendUpdate
	backends    *backend.BackendsMap
	log         zerolog.Logger
}

type staticBackendsInventoryConfig struct {
	ID    string   `hcl:"id,label"`
	Hosts []string `hcl:"hosts"`
}

type staticBackendsInventoryFactory struct{}

func (w staticBackendsInventoryFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	config := &staticBackendsInventoryConfig{}
	return gohcl.DecodeBody(tc.Config, tc.ctx, config)
}

func (w staticBackendsInventoryFactory) parseConfig(tc *Config) *staticBackendsInventoryConfig {
	config := &staticBackendsInventoryConfig{}
	gohcl.DecodeBody(tc.Config, tc.ctx, config)
	config.ID = fmt.Sprintf("backends_inventory.%s.%s", tc.Type, tc.Name)
	return config
}

func (w staticBackendsInventoryFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	c := &BackendsInventoryStatic{
		id:          config.ID,
		subscribers: make([]chan backend.BackendUpdate, 0),
		backends:    backend.NewBackendsMap(),
		log:         log.With().Str("id", config.ID).Logger(),
	}

	for _, address := range config.Hosts {
		c.backends.Add(&backend.Backend{
			Address: address,
			Meta:    backend.NewEmptyMetaMap(0),
		})
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	return c
}

func (c *BackendsInventoryStatic) ProvideUpdates(ch chan backend.BackendUpdate) {
	c.subscribers = append(c.subscribers, ch)

	go func() {
		for _, b := range c.backends.GetList() {
			c.sendUpdate(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: b.Address,
				Backend: b,
			})
		}
	}()
}

func (c *BackendsInventoryStatic) sendUpdate(m backend.BackendUpdate) {
	for _, s := range c.subscribers {
		s <- m
	}
}

func (c *BackendsInventoryStatic) GetID() string {
	return c.id
}

func (c *BackendsInventoryStatic) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

func (c *BackendsInventoryStatic) Bind(modules module.ModulesList) {

}
