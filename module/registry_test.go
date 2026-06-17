package module

import (
	"context"
	"mlb/backend"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
)

type mockFactory struct{}

func (f *mockFactory) New(config *Config, wg *sync.WaitGroup, ctx context.Context) Module {
	return &dummyModule{id: config.Name}
}

func (f *mockFactory) ValidateConfig(config *Config) hcl.Diagnostics {
	return nil
}

// TestRegistry verifies the central module registry functionality.
func TestRegistry(t *testing.T) {
	category := "test_category"
	typeName := "mock"
	factory := &mockFactory{}

	RegisterFactory(category, typeName, factory)

	if GetFactory(category, typeName) != factory {
		t.Error("Expected factory to be registered")
	}

	block := &hcl.Block{
		Type:   category,
		Labels: []string{typeName, "instance1"},
	}
	ctx := &hcl.EvalContext{}

	// Test DecodeConfigBlock
	cfg, diags := DecodeConfigBlock(block, ctx, category)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %v", diags)
	}
	if cfg == nil {
		t.Fatal("Expected config not to be nil")
	}
	if cfg.Type != typeName || cfg.Name != "instance1" {
		t.Errorf("Unexpected config values: %+v", cfg)
	}

	// Test ValidateConfig
	diags = ValidateConfig(cfg, category)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors from ValidateConfig: %v", diags)
	}

	// Test New
	wg := &sync.WaitGroup{}
	mod := New(cfg, wg, context.Background(), category)
	if mod == nil {
		t.Fatal("Expected module to be created")
	}

	// Test GetFactory with unregistered category
	if GetFactory("non_existent", "mock") != nil {
		t.Error("Expected nil for unregistered category")
	}

	// Test New with unregistered type (should panic)
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for New with unregistered type")
			}
		}()
		New(&Config{Type: "unknown"}, &sync.WaitGroup{}, context.Background(), category)
	}()

	// Test ValidateConfig with unregistered type (should panic)
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic for ValidateConfig with unregistered type")
			}
		}()
		ValidateConfig(&Config{Type: "unknown"}, category)
	}()

	// Test Unsupported type
	blockUnsupported := &hcl.Block{
		Type:   category,
		Labels: []string{"unknown", "instance2"},
		LabelRanges: []hcl.Range{
			{Start: hcl.Pos{Line: 1, Column: 1}, End: hcl.Pos{Line: 1, Column: 10}},
		},
	}
	_, diags = DecodeConfigBlock(blockUnsupported, ctx, category)
	if !diags.HasErrors() {
		t.Fatal("Expected errors for unsupported type, but got none")
	}
	expectedSummary := "Reference to unsupported test_category type"
	if diags[0].Summary != expectedSummary {
		t.Errorf("Expected summary %q, got %q", expectedSummary, diags[0].Summary)
	}
}

// Mock module structures for testing the ModulesRegistry functionality.

// dummyModule is a basic mock that implements the module.Module interface.
type dummyModule struct {
	id string
}

func (d *dummyModule) GetID() string {
	return d.id
}

func (d *dummyModule) Bind(modules ModulesRegistry) {}

// dummyUpdateProvider implements backend.BackendUpdateProvider.
type dummyUpdateProvider struct {
	dummyModule
}

func (d *dummyUpdateProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {}

// dummyListProvider implements backend.BackendListProvider.
type dummyListProvider struct {
	dummyModule
}

func (d *dummyListProvider) GetBackendList() []*backend.Backend {
	return nil
}

// Tests

// TestNewModulesRegistry verifies the correct initialization of an empty ModulesRegistry.
func TestNewModulesRegistry(t *testing.T) {
	ml := NewModulesRegistry()
	if ml == nil {
		t.Fatal("Expected NewModulesRegistry to return a non-nil object")
	}
	if len(ml) != 0 {
		t.Errorf("Expected fresh ModulesRegistry to be empty, got size %d", len(ml))
	}
}

// TestModulesRegistryAdd verifies that modules can be added to and retrieved from the ModulesRegistry by their ID.
func TestModulesRegistryAdd(t *testing.T) {
	ml := NewModulesRegistry()
	m := &dummyModule{id: "m1"}
	ml.AddModule(m)

	if len(ml) != 1 {
		t.Fatalf("Expected ModulesRegistry size 1, got %d", len(ml))
	}

	if ml["m1"] != m {
		t.Errorf("Retrieved module does not match the added module")
	}
}

// TestModulesRegistryGet verifies the retrieval of modules using the generic Get function.
func TestModulesRegistryGet(t *testing.T) {
	ml := NewModulesRegistry()
	m := &dummyUpdateProvider{dummyModule: dummyModule{id: "m1"}}
	ml.AddModule(m)

	// Scenario 1: Correct retrieval
	bup := Get[backend.BackendUpdateProvider](ml, "m1")
	if bup == nil {
		t.Errorf("Expected to retrieve a valid BackendUpdateProvider for 'm1'")
	}

	// Scenario 2: Module ID not found (expects panic)
	assertPanic(t, func() { Get[backend.BackendUpdateProvider](ml, "missing") }, "Expected panic for missing module ID")

	// Scenario 3: Module found but interface not implemented (expects panic)
	mWrong := &dummyModule{id: "m2"}
	ml.AddModule(mWrong)
	assertPanic(t, func() { Get[backend.BackendUpdateProvider](ml, "m2") }, "Expected panic for module not implementing BackendUpdateProvider")
}

// TestModulesRegistryFilter verifies that Filter correctly filters
// and returns all modules in the list that implement the requested interface.
func TestModulesRegistryFilter(t *testing.T) {
	ml := NewModulesRegistry()

	m1 := &dummyListProvider{dummyModule: dummyModule{id: "m1"}}
	m2 := &dummyModule{id: "m2"} // Does not implement BackendListProvider
	m3 := &dummyListProvider{dummyModule: dummyModule{id: "m3"}}

	ml.AddModule(m1)
	ml.AddModule(m2)
	ml.AddModule(m3)

	providers := Filter[backend.BackendListProvider](ml)

	if len(providers) != 2 {
		t.Fatalf("Expected exactly 2 BackendListProviders, got %d", len(providers))
	}

	if _, ok := providers["m1"]; !ok {
		t.Errorf("Expected 'm1' to be in the filtered results")
	}
	if _, ok := providers["m3"]; !ok {
		t.Errorf("Expected 'm3' to be in the filtered results")
	}
}

// assertPanic is a test helper that fails the test if the provided function does not panic.
func assertPanic(t *testing.T, f func(), message string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error(message)
		}
	}()
	f()
}
