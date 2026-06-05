package proxy

import (
	"context"
	"mlb/module"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
)

// mockModule is a simple mock implementation of the module.Module interface.
type mockModule struct {
	id string
}

func (m *mockModule) GetID() string {
	return m.id
}

func (m *mockModule) Bind(modules module.ModulesList) {}

// mockProxyFactory implements FactoryInterface for testing purposes.
type mockProxyFactory struct {
	newCalled      bool
	validateCalled bool
	tcPassed       *Config
	wgPassed       *sync.WaitGroup
	ctxPassed      context.Context
	mockModule     module.Module
	mockDiags      hcl.Diagnostics
}

func (f *mockProxyFactory) New(tc *Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	f.newCalled = true
	f.tcPassed = tc
	f.wgPassed = wg
	f.ctxPassed = ctx
	return f.mockModule
}

func (f *mockProxyFactory) ValidateConfig(tc *Config) hcl.Diagnostics {
	f.validateCalled = true
	f.tcPassed = tc
	return f.mockDiags
}

// TestDecodeConfigBlock_UnsupportedType verifies that DecodeConfigBlock returns an error
// diagnostic when referenced with an unsupported/unregistered proxy type.
func TestDecodeConfigBlock_UnsupportedType(t *testing.T) {
	block := &hcl.Block{
		Type:   "proxy",
		Labels: []string{"unsupported_proxy_type", "my_proxy"},
		LabelRanges: []hcl.Range{
			{
				Filename: "test.hcl",
				Start:    hcl.Pos{Line: 1, Column: 1},
				End:      hcl.Pos{Line: 1, Column: 23},
			},
		},
	}
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if cfg != nil {
		t.Errorf("expected cfg to be nil, got %v", cfg)
	}
	if !diags.HasErrors() {
		t.Errorf("expected diags to have errors")
	}
	if len(diags) != 1 {
		t.Errorf("expected 1 diagnostic, got %d", len(diags))
	}
	if diags[0].Summary != "Reference to unsupported proxy type" {
		t.Errorf("expected summary 'Reference to unsupported proxy type', got '%s'", diags[0].Summary)
	}
	if !strings.Contains(diags[0].Detail, "unsupported_proxy_type") {
		t.Errorf("expected detail to contain 'unsupported_proxy_type', got '%s'", diags[0].Detail)
	}
}

// TestDecodeConfigBlock_SupportedType verifies that DecodeConfigBlock correctly parses
// a block and validates the configuration when using a registered proxy type.
func TestDecodeConfigBlock_SupportedType(t *testing.T) {
	mockFactory := &mockProxyFactory{
		mockDiags: hcl.Diagnostics{
			{
				Severity: hcl.DiagWarning,
				Summary:  "Mock warning",
			},
		},
	}
	// Register the mock factory temporarily
	factories["mock_test_proxy"] = mockFactory
	defer delete(factories, "mock_test_proxy")

	block := &hcl.Block{
		Type:   "proxy",
		Labels: []string{"mock_test_proxy", "my_proxy"},
	}
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if cfg == nil {
		t.Fatal("expected cfg not to be nil")
	}
	if cfg.Type != "mock_test_proxy" {
		t.Errorf("expected type 'mock_test_proxy', got '%s'", cfg.Type)
	}
	if cfg.Name != "my_proxy" {
		t.Errorf("expected name 'my_proxy', got '%s'", cfg.Name)
	}
	if cfg.ctx != ctx {
		t.Errorf("expected ctx %v, got %v", ctx, cfg.ctx)
	}
	if !mockFactory.validateCalled {
		t.Errorf("expected ValidateConfig to be called")
	}
	if !reflect.DeepEqual(mockFactory.mockDiags, diags) {
		t.Errorf("expected diags %v, got %v", mockFactory.mockDiags, diags)
	}
}

// TestNew verifies that New delegates instantiation correctly to the appropriate factory.
func TestNew(t *testing.T) {
	expectedModule := &mockModule{id: "mock_module_id"}
	mockFactory := &mockProxyFactory{
		mockModule: expectedModule,
	}
	// Register the mock factory temporarily
	factories["mock_test_proxy"] = mockFactory
	defer delete(factories, "mock_test_proxy")

	tc := &Config{
		Type: "mock_test_proxy",
		Name: "my_proxy",
	}
	wg := &sync.WaitGroup{}
	ctx := context.Background()

	mod := New(tc, wg, ctx)
	if mod != expectedModule {
		t.Errorf("expected module %v, got %v", expectedModule, mod)
	}
	if !mockFactory.newCalled {
		t.Errorf("expected New to be called")
	}
	if mockFactory.tcPassed != tc {
		t.Errorf("expected tc %v, got %v", tc, mockFactory.tcPassed)
	}
	if mockFactory.wgPassed != wg {
		t.Errorf("expected wg %v, got %v", wg, mockFactory.wgPassed)
	}
	if mockFactory.ctxPassed != ctx {
		t.Errorf("expected ctx %v, got %v", ctx, mockFactory.ctxPassed)
	}
}

// TestValidateConfig verifies that ValidateConfig delegates the validation correctly to the factory.
func TestValidateConfig(t *testing.T) {
	expectedDiags := hcl.Diagnostics{
		{
			Severity: hcl.DiagError,
			Summary:  "Mock validation error",
		},
	}
	mockFactory := &mockProxyFactory{
		mockDiags: expectedDiags,
	}
	// Register the mock factory temporarily
	factories["mock_test_proxy"] = mockFactory
	defer delete(factories, "mock_test_proxy")

	tc := &Config{
		Type: "mock_test_proxy",
		Name: "my_proxy",
	}

	diags := ValidateConfig(tc)
	if !reflect.DeepEqual(expectedDiags, diags) {
		t.Errorf("expected diags %v, got %v", expectedDiags, diags)
	}
	if !mockFactory.validateCalled {
		t.Errorf("expected ValidateConfig to be called")
	}
	if mockFactory.tcPassed != tc {
		t.Errorf("expected tc %v, got %v", tc, mockFactory.tcPassed)
	}
}
