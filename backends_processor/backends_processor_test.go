package backends_processor

import (
	"context"
	"mlb/backend"
	"mlb/module"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

type dummySubscriber struct {
	updates []backend.BackendUpdate
	wg      sync.WaitGroup
	source  string
}

func (d *dummySubscriber) ReceiveUpdate(u backend.BackendUpdate) {
	d.updates = append(d.updates, u)
	d.wg.Done()
}

func (d *dummySubscriber) SubscribeTo(p backend.BackendUpdateProvider) {
	p.ProvideUpdates(d)
}

func (d *dummySubscriber) GetUpdateSource() string {
	if d.source != "" {
		return d.source
	}
	return "dummy"
}

type dummyProvider struct {
	id          string
	subscribers []backend.BackendUpdateSubscriber
	mu          sync.Mutex
}

func (d *dummyProvider) GetID() string {
	if d.id != "" {
		return d.id
	}
	return "dummy"
}

func (d *dummyProvider) Bind(modules module.ModulesList) {}

func (d *dummyProvider) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.subscribers = append(d.subscribers, s)
}

func (d *dummyProvider) sendUpdate(u backend.BackendUpdate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.subscribers {
		s.ReceiveUpdate(u)
	}
}

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

func TestBackendsProcessor_DecodeConfigBlock_Success(t *testing.T) {
	src := `
backends_processor "simple_filter" "test" {
	source = "foo"
	condition = true
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.Type != "simple_filter" {
		t.Errorf("Expected type simple_filter, got %s", cfg.Type)
	}
	if cfg.Name != "test" {
		t.Errorf("Expected name test, got %s", cfg.Name)
	}
}

func TestBackendsProcessor_DecodeConfigBlock_Unsupported(t *testing.T) {
	src := `
backends_processor "unsupported" "test" {
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	_, diags := DecodeConfigBlock(block, ctx)
	if !diags.HasErrors() {
		t.Fatalf("Expected errors but got none")
	}
	if diags[0].Summary != "Reference to unsupported backend processor type" {
		t.Errorf("Unexpected diagnostic summary: %s", diags[0].Summary)
	}
}

func TestBackendsProcessor_NewAndValidate(t *testing.T) {
	src := `
backends_processor "simple_filter" "test" {
	source = "foo"
	condition = true
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
