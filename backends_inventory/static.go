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
	module.RegisterFactory("backends_inventory", "static", &StaticBackendsInventoryFactory{})
}

type BackendsInventoryStatic struct {
	id       string
	backends *backend.Registry
	log      zerolog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

type StaticBackendsInventoryConfig struct {
	ID    string   `hcl:"id,label"`
	Hosts []string `hcl:"hosts"`
}

type StaticBackendsInventoryFactory struct{}

func (w StaticBackendsInventoryFactory) ValidateConfig(tc *module.Config) hcl.Diagnostics {
	config := &StaticBackendsInventoryConfig{}
	return gohcl.DecodeBody(tc.Config, tc.Ctx, config)
}

func (w StaticBackendsInventoryFactory) parseConfig(tc *module.Config) *StaticBackendsInventoryConfig {
	config := &StaticBackendsInventoryConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode static backend inventory config")
	}
	config.ID = fmt.Sprintf("backends_inventory.%s.%s", tc.Type, tc.Name)
	return config
}

func (w StaticBackendsInventoryFactory) New(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	c := &BackendsInventoryStatic{
		id:       config.ID,
		backends: backend.NewRegistry(),
		log:      log.With().Str("id", config.ID).Logger(),
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

func (c *BackendsInventoryStatic) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

func (c *BackendsInventoryStatic) GetID() string {
	return c.id
}

func (c *BackendsInventoryStatic) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}

func (c *BackendsInventoryStatic) Bind(modules module.ModulesRegistry) {
	_ = modules
}
