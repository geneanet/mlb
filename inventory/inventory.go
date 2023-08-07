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

func DecodeConfigBlock(block *hcl.Block) (*Config, hcl.Diagnostics) {
	if _, ok := factories[block.Labels[0]]; !ok {
		return nil, hcl.Diagnostics{
			{
				Severity: hcl.DiagError,
				Summary:  "Reference to unsupported inventory type",
				Detail:   fmt.Sprintf("Inventory type %q is not supported.", block.Labels[0]),
				Subject:  &block.LabelRanges[0],
			},
		}
	}
	tc := &Config{
		Type:   block.Labels[0],
		Name:   block.Labels[1],
		Config: block.Body,
	}
	diags := ValidateConfig(tc)
	return tc, diags
}

func New(tc *Config, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable {
	return factories[tc.Type].New(tc, wg, ctx)
}

func ValidateConfig(tc *Config) hcl.Diagnostics {
	return factories[tc.Type].ValidateConfig(tc)
}

type FactoryInterface interface {
	New(config *Config, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable
	ValidateConfig(config *Config) hcl.Diagnostics
}

var (
	factories = map[string]FactoryInterface{}
)
