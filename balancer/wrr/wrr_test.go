package wrr

import (
	"context"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// TestWRRBalancer_ValidateConfig verifies that a valid WRR balancer configuration
// passes the validation check.
func TestWRRBalancer_ValidateConfig(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: &hcl.EvalContext{}}
	diags := validateWRRBalancerConfig(cfg)
	if diags.HasErrors() {
		t.Errorf("Unexpected diags: %s", diags.Error())
	}
}

// TestWRRBalancer_DefaultTimeout ensures that the WRR balancer defaults to a 0s timeout
// if none is specified in the configuration.
func TestWRRBalancer_DefaultTimeout(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWRRBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WRRBalancer)

	if balancer.timeout != 0 {
		t.Errorf("Expected 0s timeout, got %v", balancer.timeout)
	}
}

// TestWRRBalancer_InvalidTimeout verifies that the WRR balancer returns an error when
// initialized with an invalid timeout string.
func TestWRRBalancer_InvalidTimeout(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":  {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight":  {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
			"timeout": {Name: "timeout", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("invalid")}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := newWRRBalancer(cfg, wg, ctx)
	if err == nil {
		t.Errorf("Expected error due to invalid timeout")
	}
}

// TestWRRBalancer_WaitBackend tests the blocking behavior of GetBackend(true)
// when no reg are initially available.
func TestWRRBalancer_WaitBackend(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":  {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight":  {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(1)}},
			"timeout": {Name: "timeout", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("1s")}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWRRBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WRRBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	provider.ProvideUpdates(balancer)

	backendChan := make(chan *backend.Backend)
	go func() {
		be, rel := balancer.GetBackend(true)
		defer rel()
		backendChan <- be
	}()

	// Delay briefly so GetBackend(true) enters block state.
	// We use a small sleep here because there is no exported state to poll for "being blocked".
	time.Sleep(20 * time.Millisecond)

	backend1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend1.Address, Backend: backend1})

	select {
	case returnedBackend := <-backendChan:
		if returnedBackend == nil || returnedBackend.Address != "127.0.0.1:8080" {
			t.Errorf("Expected backend to be unblocked and returned")
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Timeout waiting for GetBackend to unblock")
	}
}

// TestWRRBalancer_Workflow tests the full lifecycle and operational flow of the WRR balancer,
// including backend additions, modifications, removals, and handling evaluation errors.
func TestWRRBalancer_Workflow(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.ScopeTraversalExpr{
				Traversal: hcl.Traversal{hcl.TraverseRoot{Name: "var_weight"}},
			}},
			"timeout": {Name: "timeout", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("100ms")}},
		},
	}
	evalCtx := &hcl.EvalContext{
		Variables: map[string]cty.Value{
			"var_weight": cty.NumberIntVal(2),
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: evalCtx}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	mod, err := newWRRBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WRRBalancer)

	if len(balancer.GetBackendList()) != 0 {
		t.Errorf("Expected 0 reg")
	}

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	modules := make(module.ModulesRegistry)
	modules.AddModule("src1", provider)
	_ = balancer.Bind(modules)

	// Test timeout when no reg are available
	start := time.Now()
	timeoutBackend, rel := balancer.GetBackend(true)
	defer rel()
	if timeoutBackend != nil {
		t.Errorf("Expected nil backend on timeout")
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Errorf("GetBackend(true) did not wait for timeout")
	}

	// Add a backend and verify it can be retrieved
	backend1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend1.Address, Backend: backend1})

	testutil.Eventually(t, func() bool {
		return balancer.state.Load().length == 2
	}, 1*time.Second, 10*time.Millisecond)

	retrievedBackend, rel := balancer.GetBackend(true) // Should return immediately now
	defer rel()
	if retrievedBackend == nil || retrievedBackend.Address != "127.0.0.1:8080" {
		t.Errorf("Expected 127.0.0.1:8080, got %v", retrievedBackend)
	}

	// Modify backend1 - adjust weight to 3
	evalCtx.Variables["var_weight"] = cty.NumberIntVal(3)
	backend1Mod := backend1.Clone()
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})

	testutil.Eventually(t, func() bool {
		return balancer.state.Load().length == 3
	}, 1*time.Second, 10*time.Millisecond)

	// Modify backend1 - keeping the same weight (3) to test idempotency
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})

	testutil.Eventually(t, func() bool {
		return balancer.state.Load().length == 3
	}, 1*time.Second, 10*time.Millisecond)

	// Modify backend1 - introduce error in evaluating weight expression
	delete(evalCtx.Variables, "var_weight") // HCL resolving will fail without this
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})

	testutil.Eventually(t, func() bool {
		return balancer.state.Load().length == 0
	}, 1*time.Second, 10*time.Millisecond)

	// Add backend2 - despite active evaluation error, it should still be tracked
	backend2 := &backend.Backend{Address: "127.0.0.1:8081", Meta: backend.NewEmptyMetaMap(0)}
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend2.Address, Backend: backend2})

	testutil.Eventually(t, func() bool {
		return balancer.backends.Has(backend2.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Remove backend1
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend1.Address})

	testutil.Eventually(t, func() bool {
		return !balancer.backends.Has(backend1.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Restore var_weight and add a final backend to verify recovery
	evalCtx.Variables["var_weight"] = cty.NumberIntVal(2)
	backend3 := &backend.Backend{Address: "127.0.0.1:8082", Meta: backend.NewEmptyMetaMap(0)}
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend3.Address, Backend: backend3})

	testutil.Eventually(t, func() bool {
		return balancer.backends.Has(backend3.Address)
	}, 1*time.Second, 10*time.Millisecond)

	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend3.Address})

	testutil.Eventually(t, func() bool {
		return !balancer.backends.Has(backend3.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Cancel context to stop main loop
	cancel()
	wg.Wait()

	// Test ReceiveUpdate after shutdown
	balancer.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
}

// TestWRRBalancer_RegistryIntegration verifies that the WRR balancer can be correctly
// decoded, validated, and instantiated using the global module registry functions.
func TestWRRBalancer_RegistryIntegration(t *testing.T) {
	src := `
balancer "wrr" "test" {
	source = "src1"
	weight = 2
}
`
	block := testutil.ParseHCL(t, src)
	ctx := &hcl.EvalContext{}

	// 1. Test DecodeConfigBlock
	cfg, diags := module.DecodeConfigBlock(block, ctx, "balancer")
	if diags.HasErrors() {
		t.Fatalf("DecodeConfigBlock failed: %s", diags.Error())
	}
	if cfg.Type != "wrr" || cfg.Name != "test" {
		t.Errorf("Unexpected config: %+v", cfg)
	}

	// 2. Test ValidateConfig
	diags = module.ValidateConfig(cfg, "balancer")
	if diags.HasErrors() {
		t.Fatalf("ValidateConfig failed: %s", diags.Error())
	}

	// 3. Test New
	wg := &sync.WaitGroup{}
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := module.New(cfg, wg, bgCtx, "balancer")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if mod == nil {
		t.Fatal("module.New returned nil")
	}
	if _, ok := mod.(*WRRBalancer); !ok {
		t.Errorf("Expected *WRRBalancer, got %T", mod)
	}
}

// TestWRRBalancer_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestWRRBalancer_ParseConfigError(t *testing.T) {
	// Invalid config (source should be a string, not a list)
	src := `
balancer "wrr" "test" {
	source = ["a"]
	weight = 1
}
`
	block := testutil.ParseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &module.Config{
		Category: "balancer",
		Type:     "wrr",
		Name:     "test",
		Config:   block.Body,
		Ctx:      ctx,
	}

	// This will trigger log.Error() and still return a config object
	config := parseWRRBalancerConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}

// TestWRRBalancer_ContextCancellation verifies that the WRR balancer properly
// initializes a context for each added backend and cancels it upon removal.
func TestWRRBalancer_ContextCancellation(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(1)}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWRRBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WRRBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	provider.ProvideUpdates(balancer)

	backend1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend1.Address, Backend: backend1})

	var retrievedBackend *backend.Backend
	testutil.Eventually(t, func() bool {
		var rel func()
		retrievedBackend, rel = balancer.GetBackend(false)
		if rel != nil {
			defer rel()
		}
		return retrievedBackend != nil
	}, 1*time.Second, 10*time.Millisecond)

	if retrievedBackend.Ctx == nil {
		t.Fatalf("Expected Ctx to be set")
	}

	// Remove backend
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend1.Address})

	select {
	case <-retrievedBackend.Ctx.Done():
		// Success
	case <-time.After(1 * time.Second):
		t.Errorf("Backend context was not cancelled after removal")
	}
}

// TestWRRBalancer_SmoothDistribution verifies that the SWRR algorithm produces
// a smooth distribution of backends.
func TestWRRBalancer_SmoothDistribution(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.ScopeTraversalExpr{
				Traversal: hcl.Traversal{
					hcl.TraverseRoot{Name: "backend"},
					hcl.TraverseAttr{Name: "meta"},
					hcl.TraverseAttr{Name: "wrr"},
					hcl.TraverseAttr{Name: "weight"},
				},
			}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wrr", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWRRBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WRRBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	provider.ProvideUpdates(balancer)

	// Add 3 backends with weights: A=5, B=1, C=1
	// Total weight = 7
	beA := &backend.Backend{Address: "A", Meta: backend.NewEmptyMetaMap(0)}
	beA.Meta.Set("wrr", "weight", cty.NumberIntVal(5))
	beB := &backend.Backend{Address: "B", Meta: backend.NewEmptyMetaMap(0)}
	beB.Meta.Set("wrr", "weight", cty.NumberIntVal(1))
	beC := &backend.Backend{Address: "C", Meta: backend.NewEmptyMetaMap(0)}
	beC.Meta.Set("wrr", "weight", cty.NumberIntVal(1))

	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "A", Backend: beA})
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "B", Backend: beB})
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "C", Backend: beC})

	testutil.Eventually(t, func() bool {
		return balancer.state.Load().length == 7
	}, 1*time.Second, 10*time.Millisecond)

	// The expected "smooth" sequence for weights {A:5, B:1, C:1} is:
	// A, A, B, A, C, A, A (or similar, depending on initial tie-breaks)
	// Key is that "A" is not appearing 5 times in a row.
	sequence := make([]string, 7)
	for i := 0; i < 7; i++ {
		be, rel := balancer.GetBackend(false)
		if be == nil {
			t.Fatalf("GetBackend returned nil at index %d", i)
		}
		sequence[i] = be.Address
		rel()
	}

	// Verify total counts
	counts := make(map[string]int)
	for _, addr := range sequence {
		counts[addr]++
	}

	if counts["A"] != 5 || counts["B"] != 1 || counts["C"] != 1 {
		t.Errorf("Unexpected distribution counts: %v", counts)
	}

	// Verify smoothness: A should not appear 5 times in a row
	maxConsecutiveA := 0
	currentConsecutiveA := 0
	for _, addr := range sequence {
		if addr == "A" {
			currentConsecutiveA++
		} else {
			if currentConsecutiveA > maxConsecutiveA {
				maxConsecutiveA = currentConsecutiveA
			}
			currentConsecutiveA = 0
		}
	}
	if currentConsecutiveA > maxConsecutiveA {
		maxConsecutiveA = currentConsecutiveA
	}

	if maxConsecutiveA >= 5 {
		t.Errorf("Distribution is not smooth, found %d consecutive A: %v", maxConsecutiveA, sequence)
	}
}
