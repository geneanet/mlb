package balancer

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

// mockProvider implements backend.BackendUpdateProvider for testing purposes.
type mockProvider struct {
	id       string
	backends *backend.Registry
}

// GetID returns the provider's identifier.
func (mp *mockProvider) GetID() string {
	return mp.id
}

// Bind is a no-op implementation of the module.Module interface.
func (mp *mockProvider) Bind(modules module.ModulesList) {}

// ProvideUpdates registers a subscriber to receive backend updates.
func (mp *mockProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {
	mp.backends.Subscribe(sub)
}

// sendUpdate broadcasts a backend update to all registered subscribers.
func (mp *mockProvider) sendUpdate(upd backend.BackendUpdate) {
	mp.backends.Publish(upd)
}

// TestWRRBalancer_ValidateConfig verifies that a valid WRR balancer configuration
// passes the validation check.
func TestWRRBalancer_ValidateConfig(t *testing.T) {
	factory := factories["wrr"]
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
		},
	}
	cfg := &Config{Name: "test", Type: "wrr", Config: body, ctx: &hcl.EvalContext{}}
	diags := factory.ValidateConfig(cfg)
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
	cfg := &Config{Name: "test", Type: "wrr", Config: body, ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := factories["wrr"]
	mod := factory.New(cfg, wg, ctx)
	balancer := mod.(*WRRBalancer)

	if balancer.timeout != 0 {
		t.Errorf("Expected 0s timeout, got %v", balancer.timeout)
	}
}

// TestWRRBalancer_InvalidTimeout verifies that the WRR balancer panics when
// initialized with an invalid timeout string.
func TestWRRBalancer_InvalidTimeout(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic due to invalid timeout")
		}
	}()
	factory := factories["wrr"]
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":  {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight":  {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
			"timeout": {Name: "timeout", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("invalid")}},
		},
	}
	cfg := &Config{Name: "test", Type: "wrr", Config: body, ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	factory.New(cfg, wg, ctx)
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
	cfg := &Config{Name: "test", Type: "wrr", Config: body, ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := factories["wrr"]
	mod := factory.New(cfg, wg, ctx)
	balancer := mod.(*WRRBalancer)

	provider := &mockProvider{id: "src1", backends: backend.NewRegistry()}
	balancer.SubscribeTo(provider)

	backendChan := make(chan *backend.Backend)
	go func() {
		backendChan <- balancer.GetBackend(true)
	}()

	// Delay briefly so GetBackend(true) enters block state.
	// We use a small sleep here because there is no exported state to poll for "being blocked".
	time.Sleep(20 * time.Millisecond)

	backend1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend1.Address, Backend: backend1})

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
	cfg := &Config{Name: "test", Type: "wrr", Config: body, ctx: evalCtx}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	factory := factories["wrr"]
	mod := factory.New(cfg, wg, ctx)
	balancer := mod.(*WRRBalancer)

	if balancer.GetID() != "balancer.wrr.test" {
		t.Errorf("Unexpected ID: %s", balancer.GetID())
	}
	if balancer.GetUpdateSource() != "src1" {
		t.Errorf("Unexpected source: %s", balancer.GetUpdateSource())
	}
	if len(balancer.GetBackendList()) != 0 {
		t.Errorf("Expected 0 reg")
	}

	provider := &mockProvider{id: "src1", backends: backend.NewRegistry()}
	modules := module.NewModulesList()
	modules.AddModule(provider)
	balancer.Bind(modules)

	// Test timeout when no reg are available
	start := time.Now()
	timeoutBackend := balancer.GetBackend(true)
	if timeoutBackend != nil {
		t.Errorf("Expected nil backend on timeout")
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Errorf("GetBackend(true) did not wait for timeout")
	}

	// Add a backend and verify it can be retrieved
	backend1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend1.Address, Backend: backend1})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return len(balancer.weightedList) == 2
	}, 1*time.Second, 10*time.Millisecond)

	retrievedBackend := balancer.GetBackend(true) // Should return immediately now
	if retrievedBackend == nil || retrievedBackend.Address != "127.0.0.1:8080" {
		t.Errorf("Expected 127.0.0.1:8080, got %v", retrievedBackend)
	}

	// Modify backend1 - adjust weight to 3
	evalCtx.Variables["var_weight"] = cty.NumberIntVal(3)
	backend1Mod := backend1.Clone()
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return len(balancer.weightedList) == 3
	}, 1*time.Second, 10*time.Millisecond)

	// Modify backend1 - keeping the same weight (3) to test idempotency
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return len(balancer.weightedList) == 3
	}, 1*time.Second, 10*time.Millisecond)

	// Modify backend1 - introduce error in evaluating weight expression
	delete(evalCtx.Variables, "var_weight") // HCL resolving will fail without this
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return len(balancer.weightedList) == 0
	}, 1*time.Second, 10*time.Millisecond)

	// Add backend2 - despite active evaluation error, it should still be tracked
	backend2 := &backend.Backend{Address: "127.0.0.1:8081", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend2.Address, Backend: backend2})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return balancer.backends.Has(backend2.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Remove backend1
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend1.Address})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return !balancer.backends.Has(backend1.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Restore var_weight and add a final backend to verify recovery
	evalCtx.Variables["var_weight"] = cty.NumberIntVal(2)
	backend3 := &backend.Backend{Address: "127.0.0.1:8082", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend3.Address, Backend: backend3})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return balancer.backends.Has(backend3.Address)
	}, 1*time.Second, 10*time.Millisecond)

	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend3.Address})

	testutil.Eventually(t, func() bool {
		balancer.mu.RLock()
		defer balancer.mu.RUnlock()
		return !balancer.backends.Has(backend3.Address)
	}, 1*time.Second, 10*time.Millisecond)

	// Cancel context to stop main loop
	cancel()
	wg.Wait()

	// Test ReceiveUpdate after shutdown
	balancer.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
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

// TestWRRBalancer_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestWRRBalancer_ParseConfigError(t *testing.T) {
	// Invalid config (source should be a string, not a list)
	src := `
balancer "wrr" "test" {
	source = ["a"]
	weight = 1
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &Config{
		Type:   "wrr",
		Name:   "test",
		Config: block.Body,
		ctx:    ctx,
	}

	factory := WRRBalancerFactory{}
	// This will trigger log.Error() and still return a config object
	config := factory.parseConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}
