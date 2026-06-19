package static

import (
	"context"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"mlb/backend"
	"mlb/module"
)

func init() {
	module.RegisterFactory("backends_inventory", "static", newStaticBackendsInventory, validateStaticBackendsInventoryConfig)
}

// BackendsInventoryStatic implements a static list of backends.
type BackendsInventoryStatic struct {
	id       string
	backends *backend.Registry
	log      zerolog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

// StaticBackendsInventoryConfig defines the HCL configuration for static backends.
type StaticBackendsInventoryConfig struct {
	ID    string   `hcl:"id,label"`
	Hosts []string `hcl:"hosts"`
}

// validateStaticBackendsInventoryConfig validates the static backends configuration.
func validateStaticBackendsInventoryConfig(tc *module.Config) hcl.Diagnostics {
	config := &StaticBackendsInventoryConfig{}
	return gohcl.DecodeBody(tc.Config, tc.Ctx, config)
}

// parseStaticBackendsInventoryConfig parses the static backends configuration.
func parseStaticBackendsInventoryConfig(tc *module.Config) *StaticBackendsInventoryConfig {
	config := &StaticBackendsInventoryConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode static backend inventory config")
	}
	config.ID = tc.FullID()
	return config
}

// newStaticBackendsInventory creates a new instance of a static backends inventory.
func newStaticBackendsInventory(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) any {
	config := parseStaticBackendsInventoryConfig(tc)

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

// ProvideUpdates registers a subscriber for backend updates.
func (c *BackendsInventoryStatic) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	c.backends.ProvideUpdates(s)
}

// GetBackendList returns the current list of backends.
func (c *BackendsInventoryStatic) GetBackendList() []*backend.Backend {
	return c.backends.GetList()
}
