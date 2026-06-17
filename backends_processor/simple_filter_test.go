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

	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
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
	dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}

	// Create a subscriber
	sub := &dummySubscriber{wg: sync.WaitGroup{}}
	filterMod.ProvideUpdates(sub)

	modules := module.NewModulesRegistry()
	modules.AddModule(dp)
	filterMod.Bind(modules)

	// Wait for goroutines to settle
	time.Sleep(2 * time.Millisecond)

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

	// Add a passing one to be sure the previous one was processed
	b1ModMarker := b1.Clone()
	b1ModMarker.Meta.Set("marker", "marker", cty.StringVal("1"))
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: b1ModMarker.Address, Backend: b1ModMarker})
	waitSub(t, sub, "Marker update")

	if len(filterMod.GetBackendList()) != 1 {
		t.Errorf("Expected 1 backend in filter")
	}

	// Test 3: Modify passing to passing
	sub.wg.Add(1)
	b1Mod := b1ModMarker.Clone()
	b1Mod.Meta.Set("test", "test", cty.StringVal("foo"))
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: b1Mod.Address, Backend: b1Mod})
	waitSub(t, sub, "Modify passing to passing")

	// Test 4: Remove passing
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b1.Address})
	waitSub(t, sub, "Remove passing backend")

	// Test 5: Remove non-passing (does nothing)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b2.Address})

	// Marker update (add passing one then remove it)
	b3 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b3.Address, Backend: b3})
	waitSub(t, sub, "Marker add")
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b3.Address})
	waitSub(t, sub, "Marker remove")
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
	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	filterMod := mod.(*SimpleFilter)

	// Add an item directly to bypass wait issues, or via provider
	dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}
	modules := module.NewModulesRegistry()
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
	filterMod.ProvideUpdates(sub2)
	waitSub(t, sub2, "ProvideUpdates with existing backend for sub2")
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
	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	filterMod := mod.(*SimpleFilter)

	dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}

	sub := &dummySubscriber{wg: sync.WaitGroup{}}
	filterMod.ProvideUpdates(sub)

	modules := module.NewModulesRegistry()
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

	// Add a separate passing marker backend to be sure the previous one was processed
	bMarker := &backend.Backend{Address: "127.0.0.1:9999", Meta: backend.NewEmptyMetaMap(0)}
	bMarker.Meta.Set("test", "active", cty.BoolVal(true))
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: bMarker.Address, Backend: bMarker})
	waitSub(t, sub, "Marker add 1")

	if len(filterMod.GetBackendList()) != 1 { // Only bMarker should be here
		t.Errorf("Expected 1 backend due to eval error or false eval")
	}

	// Real error in condition evaluation: attribute does not exist
	bErr2 := &backend.Backend{Address: "127.0.0.1:8082", Meta: backend.NewEmptyMetaMap(0)}
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: bErr2.Address, Backend: bErr2})

	// Marker update
	bMarkerMod := bMarker.Clone()
	bMarkerMod.Meta.Set("marker", "marker", cty.StringVal("2"))
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: bMarkerMod.Address, Backend: bMarkerMod})
	waitSub(t, sub, "Marker update 4")

	if len(filterMod.GetBackendList()) != 1 {
		t.Errorf("Expected 1 backend due to real eval error")
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
	factory := module.GetFactory("backends_processor", "simple_filter")
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":    {Name: "source", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("test")}}}},
			"condition": {Name: "condition", Expr: &hclsyntax.LiteralValueExpr{Val: cty.BoolVal(true)}},
		},
	}
	config := &module.Config{Name: "test", Type: "simple_filter", Config: body, Ctx: &hcl.EvalContext{}}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	mod := factory.New(config, wg, ctx)

	cancel()
	wg.Wait() // Wait for the component to gracefully shut down

	// Should hit <-f.updChanStop directly instead of blocking on the main loop
	mod.(*SimpleFilter).ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
}

// TestSimpleFilter_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestSimpleFilter_ParseConfigError(t *testing.T) {
	// Invalid config (source should be a string, not a list)
	src := `
backends_processor "simple_filter" "test" {
	source = ["a", "b"]
	condition = true
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &module.Config{
		Type:   "simple_filter",
		Name:   "test",
		Config: block.Body,
		Ctx:    ctx,
	}

	factory := SimpleFilterFactory{}
	// This will trigger log.Error() and still return a config object
	config := factory.parseConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}

// TestSimpleFilter_SortAndLimit tests the sorting and limiting functionality
func TestSimpleFilter_SortAndLimit(t *testing.T) {
	src := `
backends_processor "simple_filter" "test" {
	source = "foo"
	condition = true
	sort_by = backend.meta.test.weight
	sort_order = "desc"
	limit = 2
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	filterMod := mod.(*SimpleFilter)

	dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}
	sub := &dummySubscriber{wg: sync.WaitGroup{}}
	filterMod.ProvideUpdates(sub)

	modules := module.NewModulesRegistry()
	modules.AddModule(dp)
	filterMod.Bind(modules)

	// Add 3 backends with different weights
	b1 := &backend.Backend{Address: "127.0.0.1:8081", Meta: backend.NewEmptyMetaMap(0)}
	b1.Meta.Set("test", "weight", cty.NumberIntVal(10))

	b2 := &backend.Backend{Address: "127.0.0.1:8082", Meta: backend.NewEmptyMetaMap(0)}
	b2.Meta.Set("test", "weight", cty.NumberIntVal(20))

	b3 := &backend.Backend{Address: "127.0.0.1:8083", Meta: backend.NewEmptyMetaMap(0)}
	b3.Meta.Set("test", "weight", cty.NumberIntVal(30))

	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b1.Address, Backend: b1})
	waitSub(t, sub, "Add b1")

	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b2.Address, Backend: b2})
	waitSub(t, sub, "Add b2")

	// Limit is 2, so b1 and b2 should be there (sorted desc: b2, b1)
	list := filterMod.GetBackendList()
	if len(list) != 2 {
		t.Errorf("Expected 2 backends, got %d", len(list))
	}

	sub.wg.Add(2) // b3 added, b1 removed (because b3 > b2 > b1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b3.Address, Backend: b3})
	waitSub(t, sub, "Add b3")

	list = filterMod.GetBackendList()
	if len(list) != 2 {
		t.Errorf("Expected 2 backends, got %d", len(list))
	}

	// Check that we have b3 and b2
	foundB3 := false
	foundB2 := false
	for _, b := range list {
		if b.Address == b3.Address {
			foundB3 = true
		}
		if b.Address == b2.Address {
			foundB2 = true
		}
	}
	if !foundB3 || !foundB2 {
		t.Errorf("Expected b2 and b3, got %v", list)
	}

	// Now modify b1 to have highest weight
	b1Mod := b1.Clone()
	b1Mod.Meta.Set("test", "weight", cty.NumberIntVal(100))
	sub.wg.Add(2) // b1 added back, b2 removed
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: b1Mod.Address, Backend: b1Mod})
	waitSub(t, sub, "Modify b1")

	list = filterMod.GetBackendList()
	foundB1 := false
	foundB3 = false
	for _, b := range list {
		if b.Address == b1.Address {
			foundB1 = true
		}
		if b.Address == b3.Address {
			foundB3 = true
		}
	}
	if !foundB1 || !foundB3 {
		t.Errorf("Expected b1 and b3, got %v", list)
	}
}

// TestSimpleFilter_SortAdditional tests additional sorting cases (strings, booleans, addresses, ascending)
func TestSimpleFilter_SortAdditional(t *testing.T) {
	// Case 1: Sort by address ascending (default)
	src1 := `
backends_processor "simple_filter" "test1" {
	source = "foo"
	condition = true
}
`
	// Case 2: Sort by metadata string ascending
	src2 := `
backends_processor "simple_filter" "test2" {
	source = "foo"
	condition = true
	sort_by = backend.meta.test.name
	sort_order = "asc"
}
`
	// Case 3: Sort by metadata boolean descending
	src3 := `
backends_processor "simple_filter" "test3" {
	source = "foo"
	condition = true
	sort_by = backend.meta.test.active
	sort_order = "desc"
}
`

	runTest := func(t *testing.T, src string, setup func(dp *dummyProvider), verify func(list []*backend.Backend)) {
		block := parseHCL(t, src)
		ctx := &hcl.EvalContext{}
		cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_processor")
		wg := &sync.WaitGroup{}
		ctxBG, cancel := context.WithCancel(context.Background())
		defer cancel()
		mod := module.New(cfg, wg, ctxBG, "backends_processor")
		filterMod := mod.(*SimpleFilter)
		dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}
		
		modules := module.NewModulesRegistry()
		modules.AddModule(dp)
		filterMod.Bind(modules)

		setup(dp)
		// Wait a bit for processing
		time.Sleep(10 * time.Millisecond)
		verify(filterMod.GetBackendList())
	}

	t.Run("AddressAsc", func(t *testing.T) {
		runTest(t, src1, func(dp *dummyProvider) {
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "1.1.1.1", Backend: &backend.Backend{Address: "1.1.1.1", Meta: backend.NewEmptyMetaMap(0)}})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "2.2.2.2", Backend: &backend.Backend{Address: "2.2.2.2", Meta: backend.NewEmptyMetaMap(0)}})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "0.0.0.0", Backend: &backend.Backend{Address: "0.0.0.0", Meta: backend.NewEmptyMetaMap(0)}})
		}, func(list []*backend.Backend) {
			if len(list) != 3 || list[0].Address != "0.0.0.0" || list[1].Address != "1.1.1.1" || list[2].Address != "2.2.2.2" {
				addrs := []string{}
				for _, b := range list {
					addrs = append(addrs, b.Address)
				}
				t.Errorf("Unexpected sort order: %v", addrs)
			}
		})
	})

	t.Run("StringAsc", func(t *testing.T) {
		runTest(t, src2, func(dp *dummyProvider) {
			b1 := &backend.Backend{Address: "a", Meta: backend.NewEmptyMetaMap(0)}
			b1.Meta.Set("test", "name", cty.StringVal("charlie"))
			b2 := &backend.Backend{Address: "b", Meta: backend.NewEmptyMetaMap(0)}
			b2.Meta.Set("test", "name", cty.StringVal("alpha"))
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "a", Backend: b1})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "b", Backend: b2})
		}, func(list []*backend.Backend) {
			if len(list) != 2 || list[0].Address != "b" || list[1].Address != "a" {
				addrs := []string{}
				for _, b := range list {
					addrs = append(addrs, b.Address)
				}
				t.Errorf("Unexpected sort order: %v", addrs)
			}
		})
	})

	t.Run("BoolDesc", func(t *testing.T) {
		runTest(t, src3, func(dp *dummyProvider) {
			b1 := &backend.Backend{Address: "a", Meta: backend.NewEmptyMetaMap(0)}
			b1.Meta.Set("test", "active", cty.BoolVal(false))
			b2 := &backend.Backend{Address: "b", Meta: backend.NewEmptyMetaMap(0)}
			b2.Meta.Set("test", "active", cty.BoolVal(true))
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "a", Backend: b1})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "b", Backend: b2})
		}, func(list []*backend.Backend) {
			// true should come before false in desc
			if len(list) != 2 || list[0].Address != "b" || list[1].Address != "a" {
				addrs := []string{}
				for _, b := range list {
					addrs = append(addrs, b.Address)
				}
				t.Errorf("Unexpected sort order: %v", addrs)
			}
		})
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		// Mixed types for sort_by will trigger the Type() mismatch path
		runTest(t, src2, func(dp *dummyProvider) {
			b1 := &backend.Backend{Address: "a", Meta: backend.NewEmptyMetaMap(0)}
			b1.Meta.Set("test", "name", cty.StringVal("z"))
			b2 := &backend.Backend{Address: "b", Meta: backend.NewEmptyMetaMap(0)}
			b2.Meta.Set("test", "name", cty.NumberIntVal(1))
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "a", Backend: b1})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "b", Backend: b2})
		}, func(list []*backend.Backend) {
			if len(list) != 2 {
				t.Errorf("Expected 2 backends")
			}
		})
	})

	t.Run("Numbers", func(t *testing.T) {
		srcNum := `
backends_processor "simple_filter" "test_num" {
	source = "foo"
	condition = true
	sort_by = backend.meta.test.val
	sort_order = "asc"
}
`
		runTest(t, srcNum, func(dp *dummyProvider) {
			b1 := &backend.Backend{Address: "a", Meta: backend.NewEmptyMetaMap(0)}
			b1.Meta.Set("test", "val", cty.NumberIntVal(100))
			b2 := &backend.Backend{Address: "b", Meta: backend.NewEmptyMetaMap(0)}
			b2.Meta.Set("test", "val", cty.NumberIntVal(50))
			b3 := &backend.Backend{Address: "c", Meta: backend.NewEmptyMetaMap(0)}
			b3.Meta.Set("test", "val", cty.NumberIntVal(50))
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "a", Backend: b1})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "b", Backend: b2})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "c", Backend: b3})
		}, func(list []*backend.Backend) {
			if len(list) != 3 || list[0].Address != "b" || list[1].Address != "c" || list[2].Address != "a" {
				t.Errorf("Unexpected sort order: %v", list)
			}
		})
	})

	t.Run("OtherTypes", func(t *testing.T) {
		srcOther := `
backends_processor "simple_filter" "test_other" {
	source = "foo"
	condition = true
	sort_by = backend.meta.test.val
}
`
		runTest(t, srcOther, func(dp *dummyProvider) {
			b1 := &backend.Backend{Address: "a", Meta: backend.NewEmptyMetaMap(0)}
			b1.Meta.Set("test", "val", cty.ListVal([]cty.Value{cty.True}))
			b2 := &backend.Backend{Address: "b", Meta: backend.NewEmptyMetaMap(0)}
			b2.Meta.Set("test", "val", cty.ListVal([]cty.Value{cty.False}))
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "a", Backend: b1})
			dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "b", Backend: b2})
		}, func(list []*backend.Backend) {
			if len(list) != 2 {
				t.Errorf("Expected 2 backends")
			}
		})
	})
}
