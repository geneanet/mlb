package module

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/rs/zerolog/log"
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

var factories = make(map[string]map[string]FactoryInterface)

// RegisterFactory adds a module factory to the central registry.
func RegisterFactory(category string, typeName string, factory FactoryInterface) {
	if _, ok := factories[category]; !ok {
		factories[category] = make(map[string]FactoryInterface)
	}
	factories[category][typeName] = factory
}

// GetFactory returns a module factory from the central registry.
func GetFactory(category string, typeName string) FactoryInterface {
	if reg, ok := factories[category]; ok {
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

type Module interface {
	GetID() string
	Bind(modules ModulesRegistry)
}

type ModulesRegistry map[string]Module

func NewModulesRegistry() ModulesRegistry {
	return ModulesRegistry{}
}

func (ml ModulesRegistry) AddModule(m Module) {
	ml[m.GetID()] = m
}

// TODO: Rewrite Get and Filter as methods of ModulesList when Go 1.27 (supporting generic methods) is released.

func Get[T any](ml ModulesRegistry, id string) T {
	module, ok := ml[id]
	if !ok {
		log.Panic().Str("module", id).Msg("Module does not exist")
	}

	target, ok := module.(T)
	if !ok {
		log.Panic().
			Str("module", id).
			Str("expected", fmt.Sprintf("%T", *new(T))).
			Str("actual", fmt.Sprintf("%T", module)).
			Msg("Module is not of the expected type")
	}

	return target
}

func Filter[T any](ml ModulesRegistry) ModulesRegistry {
	result := NewModulesRegistry()

	for _, m := range ml {
		if _, ok := m.(T); ok {
			result.AddModule(m)
		}
	}

	return result
}
