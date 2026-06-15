package backends_processor

import (
	"mlb/backend"
	"mlb/module"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// dummySubscriber implements backend.BackendUpdateSubscriber for testing.
type dummySubscriber struct {
	updates []backend.BackendUpdate
	wg      sync.WaitGroup
	source  string
}

// ReceiveUpdate records an update and decrements the WaitGroup.
func (d *dummySubscriber) ReceiveUpdate(u backend.BackendUpdate) {
	d.updates = append(d.updates, u)
	d.wg.Done()
}

// SubscribeTo registers this subscriber with a provider.
func (d *dummySubscriber) SubscribeTo(p backend.BackendUpdateProvider) {
	p.ProvideUpdates(d)
}

// GetUpdateSource returns the configured source or a default value.
func (d *dummySubscriber) GetUpdateSource() string {
	if d.source != "" {
		return d.source
	}
	return "dummy"
}

// dummyProvider implements backend.BackendUpdateProvider for testing.
type dummyProvider struct {
	id       string
	backends *backend.Registry
}

// GetID returns the provider's identifier.
func (d *dummyProvider) GetID() string {
	if d.id != "" {
		return d.id
	}
	return "dummy"
}

// Bind is a no-op implementation of the module.Module interface.
func (d *dummyProvider) Bind(modules module.ModulesRegistry) {}

// ProvideUpdates registers a subscriber.
func (d *dummyProvider) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	d.backends.Subscribe(s)
}

// sendUpdate broadcasts an update to all registered subscribers.
func (d *dummyProvider) sendUpdate(u backend.BackendUpdate) {
	d.backends.Publish(u)
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
