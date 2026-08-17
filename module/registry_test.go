package module

import (
	"context"
	"mlb/backend"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
)

// TestRegistry verifies the central module registry functionality.
func TestRegistry(t *testing.T) {
	category := "test_category"
	typeName := "mock"
	newFn := func(config *Config, wg *sync.WaitGroup, ctx context.Context) (any, error) {
		return &dummyModule{id: config.Name}, nil
	}
	validateFn := func(config *Config) hcl.Diagnostics {
		return nil
	}

	RegisterFactory(category, typeName, newFn, validateFn)

	if getFactory(category, typeName) == nil {
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
	mod, err := New(cfg, wg, context.Background(), category)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if mod == nil {
		t.Fatal("Expected module to be created")
	}

	// Test getFactory with unregistered category
	if getFactory("non_existent", "mock") != nil {
		t.Error("Expected nil for unregistered category")
	}

	// Test New with unregistered type (should return error)
	_, err = New(&Config{Type: "unknown"}, &sync.WaitGroup{}, context.Background(), category)
	if err == nil {
		t.Error("Expected error for New with unregistered type")
	}

	// Test ValidateConfig with unregistered type (should return error)
	diags = ValidateConfig(&Config{Type: "unknown"}, category)
	if !diags.HasErrors() {
		t.Error("Expected error for ValidateConfig with unregistered type")
	}

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

// dummyModule is a basic mock.
type dummyModule struct {
	id string
}

func (d *dummyModule) Bind(modules ModulesRegistry) error {
	return nil
}

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

func TestConfigFullID(t *testing.T) {
	cfg := &Config{
		Category: "cat",
		Type:     "typ",
		Name:     "nam",
	}
	expected := "cat.typ.nam"
	if cfg.FullID() != expected {
		t.Errorf("Expected FullID %q, got %q", expected, cfg.FullID())
	}
}

// TestModulesRegistryAdd verifies that modules can be added to and retrieved from the ModulesRegistry by their ID.
func TestModulesRegistryAdd(t *testing.T) {
	ml := make(ModulesRegistry)
	cfg := &Config{
		Category: "cat",
		Type:     "typ",
		Name:     "m1",
	}
	m := &dummyModule{id: cfg.Name}
	ml.AddModule(cfg.FullID(), m)

	if len(ml) != 1 {
		t.Fatalf("Expected ModulesRegistry size 1, got %d", len(ml))
	}

	if ml[cfg.FullID()] != m {
		t.Errorf("Retrieved module does not match the added module")
	}
}

// TestModulesRegistryGet verifies the retrieval of modules using the generic Get function.
func TestModulesRegistryGet(t *testing.T) {
	ml := make(ModulesRegistry)
	m := &dummyUpdateProvider{dummyModule: dummyModule{id: "m1"}}
	ml.AddModule("m1", m)

	// Scenario 1: Correct retrieval
	bup, err := Get[backend.BackendUpdateProvider](ml, "m1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if bup == nil {
		t.Errorf("Expected to retrieve a valid BackendUpdateProvider for 'm1'")
	}

	// Scenario 2: Module ID not found (expects error)
	_, err = Get[backend.BackendUpdateProvider](ml, "missing")
	if err == nil {
		t.Error("Expected error for missing module ID")
	}

	// Scenario 3: Module found but interface not implemented (expects error)
	mWrong := &dummyModule{id: "m2"}
	ml.AddModule("m2", mWrong)
	_, err = Get[backend.BackendUpdateProvider](ml, "m2")
	if err == nil {
		t.Error("Expected error for module not implementing BackendUpdateProvider")
	}
}

// TestModulesRegistryFilter verifies that Filter correctly filters
// and returns all modules in the list that implement the requested interface.
func TestModulesRegistryFilter(t *testing.T) {
	ml := make(ModulesRegistry)

	m1 := &dummyListProvider{dummyModule: dummyModule{id: "m1"}}
	m2 := &dummyModule{id: "m2"} // Does not implement BackendListProvider
	m3 := &dummyListProvider{dummyModule: dummyModule{id: "m3"}}

	ml.AddModule("m1", m1)
	ml.AddModule("m2", m2)
	ml.AddModule("m3", m3)

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

type mockReadyReporter struct {
	ready chan struct{}
}

func (m *mockReadyReporter) Ready() <-chan struct{} {
	return m.ready
}

func TestWaitReady(t *testing.T) {
	t.Run("AllReady", func(t *testing.T) {
		r1 := &mockReadyReporter{ready: make(chan struct{})}
		r2 := &mockReadyReporter{ready: make(chan struct{})}
		r3 := &dummyModule{} // Not a ReadyReporter

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			WaitReady(ctx, r1, r2, r3)
			close(done)
		}()

		close(r1.ready)
		close(r2.ready)

		select {
		case <-done:
			// OK
		case <-ctx.Done():
			t.Fatal("WaitReady timed out")
		}
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		r1 := &mockReadyReporter{ready: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			WaitReady(ctx, r1)
			close(done)
		}()

		cancel()

		select {
		case <-done:
			// OK
		case <-time.After(100 * time.Millisecond):
			t.Fatal("WaitReady did not return after context cancellation")
		}
	})
}
