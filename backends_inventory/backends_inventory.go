package backends_inventory

import (
	"context"
	"mlb/module"
	"sync"

	"github.com/hashicorp/hcl/v2"
)

type Config = module.Config
type FactoryInterface = module.FactoryInterface

var factories = map[string]FactoryInterface{}

func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext) (*Config, hcl.Diagnostics) {
	return module.DecodeConfigBlock(block, ctx, factories, "backends_inventory")
}

func New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	return factories[tc.Type].New(tc, wg, ctx)
}

func ValidateConfig(tc *Config) hcl.Diagnostics {
	return factories[tc.Type].ValidateConfig(tc)
}
