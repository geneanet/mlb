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

var registries = make(map[string]map[string]FactoryInterface)

// Register adds a module factory to the central registry.
func Register(category string, typeName string, factory FactoryInterface) {
	if _, ok := registries[category]; !ok {
		registries[category] = make(map[string]FactoryInterface)
	}
	registries[category][typeName] = factory
}

// Unregister removes a module factory from the central registry.
func Unregister(category string, typeName string) {
	if reg, ok := registries[category]; ok {
		delete(reg, typeName)
	}
}

// GetFactory returns a module factory from the central registry.
func GetFactory(category string, typeName string) FactoryInterface {
	if reg, ok := registries[category]; ok {
		return reg[typeName]
	}
	return nil
}

// New creates a new module instance using the central registry.
func New(config *Config, wg *sync.WaitGroup, ctx context.Context, category string) Module {
	factory := GetFactory(category, config.Type)
	if factory == nil {
		panic(fmt.Sprintf("module type %q not found in category %q", config.Type, category))
	}
	return factory.New(config, wg, ctx)
}

// ValidateConfig validates a module configuration using the central registry.
func ValidateConfig(config *Config, category string) hcl.Diagnostics {
	factory := GetFactory(category, config.Type)
	if factory == nil {
		panic(fmt.Sprintf("module type %q not found in category %q", config.Type, category))
	}
	return factory.ValidateConfig(config)
}

// DecodeConfigBlock is a generic helper to decode an HCL block into a Config.
func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext, category string) (*Config, hcl.Diagnostics) {
	factory := GetFactory(category, block.Labels[0])
	if factory == nil {
		return nil, hcl.Diagnostics{
			{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("Reference to unsupported %s type", category),
				Detail:   fmt.Sprintf("%s type %q is not supported.", category, block.Labels[0]),
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
	diags := factory.ValidateConfig(tc)
	return tc, diags
}
