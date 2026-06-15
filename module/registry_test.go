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

// TestDecodeConfigBlock verifies the shared configuration decoding logic.
func TestDecodeConfigBlock(t *testing.T) {
	factories := map[string]FactoryInterface{
		"mock": &mockFactory{},
	}

	block := &hcl.Block{
		Type:   "test_module",
		Labels: []string{"mock", "instance1"},
	}
	ctx := &hcl.EvalContext{}

	// Scenario 1: Supported type
	cfg, diags := DecodeConfigBlock(block, ctx, factories, "test_module")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %v", diags)
	}
	if cfg == nil {
		t.Fatal("Expected config not to be nil")
	}
	if cfg.Type != "mock" || cfg.Name != "instance1" {
		t.Errorf("Unexpected config values: %+v", cfg)
	}

	// Scenario 2: Unsupported type
	blockUnsupported := &hcl.Block{
		Type:   "test_module",
		Labels: []string{"unknown", "instance2"},
		LabelRanges: []hcl.Range{
			{Start: hcl.Pos{Line: 1, Column: 1}, End: hcl.Pos{Line: 1, Column: 10}},
		},
	}
	_, diags = DecodeConfigBlock(blockUnsupported, ctx, factories, "test_module")
	if !diags.HasErrors() {
		t.Fatal("Expected errors for unsupported type, but got none")
	}
	expectedSummary := "Reference to unsupported test_module type"
	if diags[0].Summary != expectedSummary {
		t.Errorf("Expected summary %q, got %q", expectedSummary, diags[0].Summary)
	}
}
