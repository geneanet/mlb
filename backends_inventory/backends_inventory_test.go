package backends_inventory

import (
	"context"
	"mlb/backend"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// dummySubscriber implements backend.BackendUpdateSubscriber for testing.
type dummySubscriber struct {
	updates []backend.BackendUpdate
	wg      sync.WaitGroup
}

// ReceiveUpdate records an update and decrements the internal WaitGroup.
func (d *dummySubscriber) ReceiveUpdate(u backend.BackendUpdate) {
	d.updates = append(d.updates, u)
	d.wg.Done()
}

// SubscribeTo is a no-op for this mock.
func (d *dummySubscriber) SubscribeTo(p backend.BackendUpdateProvider) {
}

// GetUpdateSource returns a default identifier for the subscriber.
func (d *dummySubscriber) GetUpdateSource() string {
	return "dummy"
}

// parseHCL is a helper that parses a HCL string into an hcl.Block.
func parseHCL(t *testing.T, src string) *hcl.Block {
	t.Helper()
	file, diags := hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("Failed to parse config: %s", diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		t.Fatalf("Failed to get body")
	}
	if len(body.Blocks) == 0 {
		t.Fatalf("No blocks found")
	}
	// Convert hclsyntax.Block to hcl.Block
	b := body.Blocks[0]
	return b.AsHCLBlock()
}

// TestBackendsInventory_DecodeConfigBlock_Success verifies that a valid backends_inventory
// block is correctly decoded into a Config object.
func TestBackendsInventory_DecodeConfigBlock_Success(t *testing.T) {
	src := `
backends_inventory "static" "test" {
	hosts = ["127.0.0.1:8080"]
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.Type != "static" {
		t.Errorf("Expected type static, got %s", cfg.Type)
	}
	if cfg.Name != "test" {
		t.Errorf("Expected name test, got %s", cfg.Name)
	}
}

// TestBackendsInventory_DecodeConfigBlock_Unsupported verifies that an error is returned
// when attempting to decode an unsupported backends_inventory type.
func TestBackendsInventory_DecodeConfigBlock_Unsupported(t *testing.T) {
	src := `
backends_inventory "unsupported" "test" {
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	_, diags := DecodeConfigBlock(block, ctx)
	if !diags.HasErrors() {
		t.Fatalf("Expected errors but got none")
	}
	if diags[0].Summary != "Reference to unsupported backends_inventory type" {
		t.Errorf("Unexpected diagnostic summary: %s", diags[0].Summary)
	}
}

// TestBackendsInventory_NewAndValidate verifies the validation and instantiation
// process for backends_inventory modules.
func TestBackendsInventory_NewAndValidate(t *testing.T) {
	src := `
backends_inventory "static" "test" {
	hosts = ["127.0.0.1:8080"]
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	diags = ValidateConfig(cfg)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	mod := New(cfg, wg, context.Background())
	if mod == nil {
		t.Fatalf("Expected module, got nil")
	}
}
