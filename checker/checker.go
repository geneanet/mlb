package checker

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
				Summary:  "Reference to unsupported checker type",
				Detail:   fmt.Sprintf("Checker type %q is not supported.", block.Labels[0]),
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

func New(tc *Config, sources map[string]backend.BackendUpdateProvider, wg *sync.WaitGroup, ctx context.Context) backend.BackendUpdateProvider {
	return factories[tc.Type].New(tc, sources, wg, ctx)
}

func ValidateConfig(tc *Config) hcl.Diagnostics {
	return factories[tc.Type].ValidateConfig(tc)
}

type FactoryInterface interface {
	New(config *Config, sources map[string]backend.BackendUpdateProvider, wg *sync.WaitGroup, ctx context.Context) backend.BackendUpdateProvider
	ValidateConfig(config *Config) hcl.Diagnostics
}

var (
	factories = map[string]FactoryInterface{}
)
