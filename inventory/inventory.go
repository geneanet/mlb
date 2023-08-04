package inventory

import (
	"context"
	"fmt"
	"mlb/backend"
	"sync"

	"github.com/hashicorp/hcl/v2"
)

type Config struct {
	Type   string
	Name   string
	Config hcl.Body
}

func DecodeConfigBlock(block *hcl.Block) *Config {
	return &Config{
		Type:   block.Labels[0],
		Name:   block.Labels[1],
		Config: block.Body,
	}
}

func New(tc *Config, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable {
	return factories[tc.Type].New(tc, wg, ctx)
}

func ValidateConfig(tc *Config) hcl.Diagnostics {
	if _, ok := factories[tc.Type]; !ok {
		return hcl.Diagnostics{
			{
				Severity: hcl.DiagError,
				Summary:  "Reference to unsupported inventory type",
				Detail:   fmt.Sprintf("Inventory type %q is not supported.", tc.Type),
			},
		}
	}
	return factories[tc.Type].ValidateConfig(tc)
}

type FactoryInterface interface {
	New(config *Config, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable
	ValidateConfig(config *Config) hcl.Diagnostics
}

var (
	factories = map[string]FactoryInterface{}
)
