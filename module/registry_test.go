package module

import (
	"context"
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

	Register(category, typeName, factory)
	defer Unregister(category, typeName)

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

	// Test Unregister with non-existent category
	Unregister("non_existent_category", "mock")

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
