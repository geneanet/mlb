package backends_inventory

import (
	"context"
	"mlb/backend"
	"mlb/module"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
)

// TestStaticBackendsInventory_Methods tests the static inventory implementation,
// ensuring it correctly parses the host list and provides appropriate backend updates.
func TestStaticBackendsInventory_Methods(t *testing.T) {
	src := `
backends_inventory "static" "test" {
	hosts = ["127.0.0.1:8080", "127.0.0.1:8081"]
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_inventory")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG := context.Background()
	mod := module.New(cfg, wg, ctxBG, "backends_inventory")

	staticMod, ok := mod.(*BackendsInventoryStatic)
	if !ok {
		t.Fatalf("Expected *BackendsInventoryStatic")
	}

	if staticMod.GetID() != "backends_inventory.static.test" {
		t.Errorf("Unexpected ID: %s", staticMod.GetID())
	}

	backends := staticMod.GetBackendList()
	if len(backends) != 2 {
		t.Errorf("Expected 2 backends, got %d", len(backends))
	}

	staticMod.Bind(module.ModulesRegistry{}) // Should do nothing

	// Test ProvideUpdates functionality: new subscribers should receive current backends.
	sub := &dummySubscriber{
		wg: sync.WaitGroup{},
	}
	sub.wg.Add(2) // Expecting 2 updates (one for each static host)

	staticMod.ProvideUpdates(sub)

	// Wait for updates to be delivered
	done := make(chan struct{})
	go func() {
		sub.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Errorf("Timeout waiting for updates")
	}

	if len(sub.updates) != 2 {
		t.Errorf("Expected 2 updates, got %d", len(sub.updates))
	}

	has8080 := false
	has8081 := false
	for _, u := range sub.updates {
		if u.Kind != backend.UpdBackendAdded {
			t.Errorf("Expected UpdBackendAdded, got %v", u.Kind)
		}
		if u.Address == "127.0.0.1:8080" {
			has8080 = true
		} else if u.Address == "127.0.0.1:8081" {
			has8081 = true
		}
	}

	if !has8080 || !has8081 {
		t.Errorf("Missing expected addresses in updates")
	}

	// Test sendUpdate (even if it's currently unused in normal operation)
	// We use a fresh inventory to avoid interference with previous tests and subscribers
	mod2 := module.New(cfg, wg, ctxBG, "backends_inventory")
	staticMod2 := mod2.(*BackendsInventoryStatic)

	sub2 := &dummySubscriber{
		wg: sync.WaitGroup{},
	}
	sub2.wg.Add(1)
	staticMod2.backends.Subscribe(sub2)

	testUpdate := backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "127.0.0.1:9999",
	}
	staticMod2.backends.Publish(testUpdate)

	sub2.wg.Wait()
	if len(sub2.updates) != 1 {
		t.Errorf("Expected 1 update, got %d", len(sub2.updates))
	}
	if sub2.updates[0].Address != "127.0.0.1:9999" {
		t.Errorf("Expected address 127.0.0.1:9999, got %s", sub2.updates[0].Address)
	}
}

// TestStaticBackendsInventory_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestStaticBackendsInventory_ParseConfigError(t *testing.T) {
	// Invalid config (hosts should be a list of strings, not a single string)
	src := `
backends_inventory "static" "test" {
	hosts = "invalid"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &module.Config{
		Type:   "static",
		Name:   "test",
		Config: block.Body,
		Ctx:    ctx,
	}

	factory := StaticBackendsInventoryFactory{}
	// This will trigger log.Error() and still return a config object
	config := factory.parseConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}
