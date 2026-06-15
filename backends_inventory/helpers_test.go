package backends_inventory

import (
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
