package balancer

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

type mockProvider struct {
	id          string
	subscribers []backend.BackendUpdateSubscriber
}

func (mp *mockProvider) GetID() string {
	return mp.id
}

func (mp *mockProvider) Bind(modules module.ModulesList) {}

func (mp *mockProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {
	mp.subscribers = append(mp.subscribers, sub)
}

func (mp *mockProvider) sendUpdate(upd backend.BackendUpdate) {
	for _, sub := range mp.subscribers {
		sub.ReceiveUpdate(upd)
	}
}

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

	provider := &mockProvider{id: "src1"}
	balancer.SubscribeTo(provider)

	backendChan := make(chan *backend.Backend)
	go func() {
		backendChan <- balancer.GetBackend(true)
	}()

	// Delay briefly so GetBackend(true) enters block state
	time.Sleep(50 * time.Millisecond)

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
		t.Errorf("Expected 0 backends")
	}

	provider := &mockProvider{id: "src1"}
	modules := module.NewModulesList()
	modules.AddModule(provider)
	balancer.Bind(modules)

	start := time.Now()
	timeoutBackend := balancer.GetBackend(true)
	if timeoutBackend != nil {
		t.Errorf("Expected nil backend on timeout")
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Errorf("GetBackend(true) did not wait for timeout")
	}

	backend1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend1.Address, Backend: backend1})

	time.Sleep(50 * time.Millisecond)

	retrievedBackend := balancer.GetBackend(true) // Should return immediately now
	if retrievedBackend == nil || retrievedBackend.Address != "127.0.0.1:8080" {
		t.Errorf("Expected 127.0.0.1:8080, got %v", retrievedBackend)
	}

	balancer.mu.RLock()
	if len(balancer.weightedList) != 2 {
		t.Errorf("Expected weightedList to have 2 items, got %d", len(balancer.weightedList))
	}
	balancer.mu.RUnlock()

	// Modify backend1 - adjust weight to 3
	evalCtx.Variables["var_weight"] = cty.NumberIntVal(3)
	backend1Mod := backend1.Clone()
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})
	time.Sleep(50 * time.Millisecond)

	balancer.mu.RLock()
	if len(balancer.weightedList) != 3 {
		t.Errorf("Expected weightedList to have 3 items, got %d", len(balancer.weightedList))
	}
	balancer.mu.RUnlock()

	// Modify backend1 - keeping the same weight (3)
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})
	time.Sleep(50 * time.Millisecond)

	balancer.mu.RLock()
	if len(balancer.weightedList) != 3 {
		t.Errorf("Expected weightedList to have 3 items, got %d", len(balancer.weightedList))
	}
	balancer.mu.RUnlock()

	// Modify backend1 - introduce error in evaluating expression
	delete(evalCtx.Variables, "var_weight") // HCL resolving will fail without this
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: backend1Mod.Address, Backend: backend1Mod})
	time.Sleep(50 * time.Millisecond)

	balancer.mu.RLock()
	if len(balancer.weightedList) != 0 {
		t.Errorf("Expected weightedList to have 0 items after error, got %d", len(balancer.weightedList))
	}
	balancer.mu.RUnlock()

	// Add backend2 - with active evaluation error
	backend2 := &backend.Backend{Address: "127.0.0.1:8081", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend2.Address, Backend: backend2})
	time.Sleep(50 * time.Millisecond)

	balancer.mu.RLock()
	if !balancer.backends.Has(backend2.Address) {
		t.Errorf("Expected backend 2 to be added despite eval error")
	}
	balancer.mu.RUnlock()

	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend1.Address})
	time.Sleep(50 * time.Millisecond)

	balancer.mu.RLock()
	if balancer.backends.Has(backend1.Address) {
		t.Errorf("Expected backend 1 to be removed")
	}
	balancer.mu.RUnlock()

	// Restore var_weight to successfully add a final backend
	evalCtx.Variables["var_weight"] = cty.NumberIntVal(2)
	backend3 := &backend.Backend{Address: "127.0.0.1:8082", Meta: backend.NewEmptyMetaMap(0)}
	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: backend3.Address, Backend: backend3})
	time.Sleep(50 * time.Millisecond)

	provider.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: backend3.Address})
	time.Sleep(50 * time.Millisecond)

	// Cancel context to stop main loop
	cancel()
	wg.Wait()

	balancer.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
}
