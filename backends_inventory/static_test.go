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

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG := context.Background()
	mod := New(cfg, wg, ctxBG)

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

	staticMod.Bind(module.ModulesList{}) // Should do nothing

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
}
