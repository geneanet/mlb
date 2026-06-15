package module

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/hcl/v2"
)

// Config represents a module's HCL configuration.
type Config struct {
	Type   string
	Name   string
	Config hcl.Body
	Ctx    *hcl.EvalContext
}

// FactoryInterface is the interface that all module factories must implement.
type FactoryInterface interface {
	New(config *Config, wg *sync.WaitGroup, ctx context.Context) Module
	ValidateConfig(config *Config) hcl.Diagnostics
}

// DecodeConfigBlock is a generic helper to decode an HCL block into a Config.
func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext, factories map[string]FactoryInterface, moduleName string) (*Config, hcl.Diagnostics) {
	if _, ok := factories[block.Labels[0]]; !ok {
		return nil, hcl.Diagnostics{
			{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("Reference to unsupported %s type", moduleName),
				Detail:   fmt.Sprintf("%s type %q is not supported.", moduleName, block.Labels[0]),
				Subject:  &block.LabelRanges[0],
			},
		}
	}
	tc := &Config{
		Type:   block.Labels[0],
		Name:   block.Labels[1],
		Config: block.Body,
		Ctx:    ctx,
	}
	diags := factories[tc.Type].ValidateConfig(tc)
	return tc, diags
}
