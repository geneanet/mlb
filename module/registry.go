package module

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/hcl/v2"
)

// Config represents a module's HCL configuration.
type Config struct {
	Category string
	Type     string
	Name     string
	Config   hcl.Body
	Ctx      *hcl.EvalContext
	RawHCL   string
}

// FullID returns the canonical identifier for the module, prefixed by its category.
func (c *Config) FullID() string {
	return fmt.Sprintf("%s.%s.%s", c.Category, c.Type, c.Name)
}

// NewFunc is a function type for creating a new module instance.
// It takes the module's configuration, a WaitGroup for tracking goroutines,
// and a context for lifecycle management.
type NewFunc func(config *Config, wg *sync.WaitGroup, ctx context.Context) (any, error)

// ValidateFunc is a function type for validating a module's configuration.
// It returns HCL diagnostics if any validation errors occur.
type ValidateFunc func(config *Config) hcl.Diagnostics

// factory internal struct to hold the functions.
type factory struct {
	new      NewFunc
	validate ValidateFunc
}

var (
	factories   = make(map[string]map[string]factory)
	factoriesMu sync.RWMutex
)

// RegisterFactory registers a new module type within a specific category.
// It takes two function pointers: newFn for instantiation and validateFn for
// configuration validation. This functional approach avoids the need for
// intermediate factory structures in module implementations.
func RegisterFactory(category, typeName string, newFn NewFunc, validateFn ValidateFunc) {
	factoriesMu.Lock()
	defer factoriesMu.Unlock()

	if _, ok := factories[category]; !ok {
		factories[category] = make(map[string]factory)
	}
	factories[category][typeName] = factory{
		new:      newFn,
		validate: validateFn,
	}
}

// getFactory returns a module factory from the central registry.
func getFactory(category string, typeName string) *factory {
	factoriesMu.RLock()
	defer factoriesMu.RUnlock()

	if reg, ok := factories[category]; ok {
		if f, ok := reg[typeName]; ok {
			return &f
		}
	}
	return nil
}

// New creates a new module instance using the central registry.
func New(config *Config, wg *sync.WaitGroup, ctx context.Context, category string) (any, error) {
	f := getFactory(category, config.Type)
	if f == nil {
		return nil, fmt.Errorf("module type %q not found in category %q", config.Type, category)
	}
	return f.new(config, wg, ctx)
}

// ValidateConfig validates a module configuration using the central registry.
func ValidateConfig(config *Config, category string) hcl.Diagnostics {
	f := getFactory(category, config.Type)
	if f == nil {
		return hcl.Diagnostics{
			{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("Reference to unsupported %s type", category),
				Detail:   fmt.Sprintf("%s type %q is not supported.", category, config.Type),
			},
		}
	}
	return f.validate(config)
}

// DecodeConfigBlock is a generic helper to decode an HCL block into a Config.
func DecodeConfigBlock(block *hcl.Block, ctx *hcl.EvalContext, category string) (*Config, hcl.Diagnostics) {
	f := getFactory(category, block.Labels[0])
	if f == nil {
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
		Category: category,
		Type:     block.Labels[0],
		Name:     block.Labels[1],
		Config:   block.Body,
		Ctx:      ctx,
	}
	diags := f.validate(tc)
	return tc, diags
}

// Binder is an optional interface for modules that need to be cross-linked
// with other modules after they have all been instantiated.
type Binder interface {
	Bind(modules ModulesRegistry) error
}

// ModulesRegistry stores all active module instances indexed by their
// configuration name. It uses 'any' to allow modules to be of any type,
// relying on type assertions at retrieval time for safety and simplicity.
type ModulesRegistry map[string]any

// AddModule registers a module instance in the registry.
func (ml ModulesRegistry) AddModule(id string, m any) {
	ml[id] = m
}

// TODO: Rewrite Get and Filter as methods of ModulesRegistry when Go 1.27 (supporting generic methods) is released.

// Get retrieves a module from the registry by ID and casts it to the desired type T.
// It returns an error if the module does not exist or if the type assertion fails.
func Get[T any](ml ModulesRegistry, id string) (T, error) {
	m, ok := ml[id]
	if !ok {
		return *new(T), fmt.Errorf("module %q does not exist", id)
	}

	target, ok := m.(T)
	if !ok {
		return *new(T), fmt.Errorf("module %q is not of the expected type %T (actual: %T)", id, *new(T), m)
	}

	return target, nil
}

// Filter returns a subset of the registry containing only modules that implement type T.
func Filter[T any](ml ModulesRegistry) ModulesRegistry {
	result := make(ModulesRegistry)

	for id, m := range ml {
		if _, ok := m.(T); ok {
			result[id] = m
		}
	}

	return result
}
