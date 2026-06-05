package backends_processor

import (
	"context"
	"mlb/backend"
	"mlb/module"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// TestSimpleFilter_Methods tests the core functionality of the SimpleFilter,
// including backend filtering based on address and handling of various update types.
func TestSimpleFilter_Methods(t *testing.T) {
	src := `
backends_processor "simple_filter" "test" {
	source = "foo"
	condition = backend.address == "127.0.0.1:8080"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
	filterMod, ok := mod.(*SimpleFilter)
	if !ok {
		t.Fatalf("Expected *SimpleFilter")
	}

	if filterMod.GetID() != "backends_processor.simple_filter.test" {
		t.Errorf("Unexpected ID: %s", filterMod.GetID())
	}
	if filterMod.GetUpdateSource() != "foo" {
		t.Errorf("Unexpected update source: %s", filterMod.GetUpdateSource())
	}

	// Create a dummy provider to feed updates
	dp := &dummyProvider{id: "foo"}

	// Create a subscriber
	sub := &dummySubscriber{wg: sync.WaitGroup{}}
	filterMod.ProvideUpdates(sub)

	modules := module.NewModulesList()
	modules.AddModule(dp)
	filterMod.Bind(modules)

	// Wait for goroutines to settle
	time.Sleep(10 * time.Millisecond)

	// Test 1: Add passing backend
	b1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b1.Address, Backend: b1})
	waitSub(t, sub, "Add passing backend")

	if len(filterMod.GetBackendList()) != 1 {
		t.Errorf("Expected 1 backend in filter")
	}

	// Test 2: Add non-passing backend
	b2 := &backend.Backend{Address: "127.0.0.1:8081", Meta: backend.NewEmptyMetaMap(0)}
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b2.Address, Backend: b2})
	time.Sleep(10 * time.Millisecond)
	if len(filterMod.GetBackendList()) != 1 {
		t.Errorf("Expected 1 backend in filter")
	}

	// Test 3: Modify passing to passing
	sub.wg.Add(1)
	b1Mod := b1.Clone()
	b1Mod.Meta.Set("test", "test", cty.StringVal("foo"))
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: b1Mod.Address, Backend: b1Mod})
	waitSub(t, sub, "Modify passing to passing")

	// Test 5: Remove passing
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b1.Address})
	waitSub(t, sub, "Remove passing backend")

	// Test 6: Remove non-passing (does nothing)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b2.Address})
	time.Sleep(10 * time.Millisecond)
}

// TestSimpleFilter_ProvideUpdates_WithExisting verifies that a new subscriber
// receives updates for all backends already present in the filter.
func TestSimpleFilter_ProvideUpdates_WithExisting(t *testing.T) {
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

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
	filterMod := mod.(*SimpleFilter)

	// Add an item directly to bypass wait issues, or via provider
	dp := &dummyProvider{id: "foo"}
	modules := module.NewModulesList()
	modules.AddModule(dp)
	filterMod.Bind(modules)

	// We need a subscriber to wait for the backend to be added, otherwise we can't be sure it's processed
	sub1 := &dummySubscriber{wg: sync.WaitGroup{}}
	filterMod.ProvideUpdates(sub1)

	b := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	sub1.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b.Address, Backend: b})
	waitSub(t, sub1, "Wait for first add")

	// Now add sub2
	sub2 := &dummySubscriber{wg: sync.WaitGroup{}}
	sub2.wg.Add(1)
	// sub1 will ALSO receive this because of the loop in ProvideUpdates calling sendUpdate
	sub1.wg.Add(1)
	filterMod.ProvideUpdates(sub2)
	waitSub(t, sub2, "ProvideUpdates with existing backend for sub2")
	waitSub(t, sub1, "ProvideUpdates with existing backend for sub1")
}

// TestSimpleFilter_ConditionChange tests that backend membership in the filtered list
// is dynamically updated when backend metadata changes and affects the condition.
func TestSimpleFilter_ConditionChange(t *testing.T) {
	src := `
backends_processor "simple_filter" "test_meta" {
	source = "foo"
	condition = backend.meta.test.active == true
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
	filterMod := mod.(*SimpleFilter)

	dp := &dummyProvider{id: "foo"}

	sub := &dummySubscriber{wg: sync.WaitGroup{}}
	filterMod.ProvideUpdates(sub)

	modules := module.NewModulesList()
	modules.AddModule(dp)
	filterMod.Bind(modules)

	// Add backend (matches initially)
	b := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	b.Meta.Set("test", "active", cty.BoolVal(true))

	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b.Address, Backend: b})
	waitSub(t, sub, "Add matching backend")

	if len(filterMod.GetBackendList()) != 1 {
		t.Errorf("Expected 1 backend")
	}

	// Modify to non-matching
	bMod := b.Clone()
	bMod.Meta.Set("test", "active", cty.BoolVal(false))
	sub.wg.Add(1) // Should receive remove update
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: bMod.Address, Backend: bMod})
	waitSub(t, sub, "Modify to non-matching")

	if len(filterMod.GetBackendList()) != 0 {
		t.Errorf("Expected 0 backend")
	}

	// Error in condition evaluation: wrong type (string instead of bool)
	bErr := b.Clone()
	bErr.Meta.Set("test", "active", cty.StringVal("not_a_bool"))
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: bErr.Address, Backend: bErr})
	time.Sleep(10 * time.Millisecond)
	if len(filterMod.GetBackendList()) != 0 {
		t.Errorf("Expected 0 backend due to eval error or false eval")
	}

	// Real error in condition evaluation: attribute does not exist
	bErr2 := &backend.Backend{Address: "127.0.0.1:8082", Meta: backend.NewEmptyMetaMap(0)}
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: bErr2.Address, Backend: bErr2})
	time.Sleep(10 * time.Millisecond)
	if len(filterMod.GetBackendList()) != 0 {
		t.Errorf("Expected 0 backend due to real eval error")
	}
}

// waitSub is a helper function that waits for a dummySubscriber's WaitGroup with a timeout.
func waitSub(t *testing.T, sub *dummySubscriber, name string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		sub.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for sub in %s", name)
	}
}

// TestSimpleFilter_ReceiveUpdateClosed verifies that the filter handles updates
// gracefully after it has been shut down.
func TestSimpleFilter_ReceiveUpdateClosed(t *testing.T) {
	factory := factories["simple_filter"]
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":    {Name: "source", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("test")}}}},
			"condition": {Name: "condition", Expr: &hclsyntax.LiteralValueExpr{Val: cty.BoolVal(true)}},
		},
	}
	config := &Config{Name: "test", Type: "simple_filter", Config: body, ctx: &hcl.EvalContext{}}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	mod := factory.New(config, wg, ctx)

	cancel()
	wg.Wait() // Wait for the component to gracefully shut down

	// Should hit <-f.updChanStop directly instead of blocking on the main loop
	mod.(*SimpleFilter).ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
}
