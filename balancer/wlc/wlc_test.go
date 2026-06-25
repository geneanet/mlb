package wlc

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

func TestWLCBalancer_LeastConnections(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(1)}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wlc", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWLCBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WLCBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	provider.ProvideUpdates(balancer)

	beA := &backend.Backend{Address: "A", Meta: backend.NewEmptyMetaMap(0)}
	beB := &backend.Backend{Address: "B", Meta: backend.NewEmptyMetaMap(0)}

	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "A", Backend: beA})
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "B", Backend: beB})

	testutil.Eventually(t, func() bool {
		return len(balancer.GetBackendList()) == 2
	}, 1*time.Second, 10*time.Millisecond)

	// Initially, both have 0 connections. Pick one.
	res1, rel1 := balancer.GetBackend(false)
	if res1 == nil {
		t.Fatal("Expected a backend")
	}
	addr1 := res1.Address

	// Now addr1 has 1 connection, the other has 0.
	// WLC should pick the other one.
	res2, rel2 := balancer.GetBackend(false)
	if res2 == nil {
		t.Fatal("Expected a backend")
	}
	addr2 := res2.Address

	if addr1 == addr2 {
		t.Errorf("Expected different backends, got %s for both", addr1)
	}

	// Now both have 1 connection.
	// If we release rel1, addr1 has 0 and addr2 has 1.
	rel1()
	
	res3, rel3 := balancer.GetBackend(false)
	if res3 == nil {
		t.Fatal("Expected a backend")
	}
	if res3.Address != addr1 {
		t.Errorf("Expected %s, got %s", addr1, res3.Address)
	}
	rel2()
	rel3()
}

func TestWLCBalancer_WeightedLeastConnections(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.ScopeTraversalExpr{
				Traversal: hcl.Traversal{
					hcl.TraverseRoot{Name: "backend"},
					hcl.TraverseAttr{Name: "meta"},
					hcl.TraverseAttr{Name: "wlc"},
					hcl.TraverseAttr{Name: "weight"},
				},
			}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wlc", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWLCBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WLCBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	provider.ProvideUpdates(balancer)

	// A: weight 2, B: weight 1
	beA := &backend.Backend{Address: "A", Meta: backend.NewEmptyMetaMap(0)}
	beA.Meta.Set("wlc", "weight", cty.NumberIntVal(2))
	beB := &backend.Backend{Address: "B", Meta: backend.NewEmptyMetaMap(0)}
	beB.Meta.Set("wlc", "weight", cty.NumberIntVal(1))

	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "A", Backend: beA})
	provider.SendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: "B", Backend: beB})

	testutil.Eventually(t, func() bool {
		return len(balancer.GetBackendList()) == 2
	}, 1*time.Second, 10*time.Millisecond)

	// Both 0 conn.
	// Pick A (usually first in alphabetical if weights tie, or just one of them)
	_, rel1 := balancer.GetBackend(false)
	
	// If we pick A, A has 1 conn, weight 2 -> ratio 0.5
	// B has 0 conn, weight 1 -> ratio 0
	// WLC should pick B next.
	_, rel2 := balancer.GetBackend(false)
	
	// Now A has 1/2, B has 1/1.
	// WLC should pick A next because 1/2 < 1/1.
	res3, rel3 := balancer.GetBackend(false)
	if res3 == nil || res3.Address != "A" {
		t.Errorf("Expected A, got %v (A: 1/2, B: 1/1)", res3)
	}
	
	rel1()
	rel2()
	rel3()
}

func TestWLCBalancer_ValidateConfig(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wlc", Config: body, Ctx: &hcl.EvalContext{}}
	diags := validateWLCBalancerConfig(cfg)
	if diags.HasErrors() {
		t.Errorf("Unexpected diags: %s", diags.Error())
	}
}

func TestWLCBalancer_WaitBackend(t *testing.T) {
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":  {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight":  {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(1)}},
			"timeout": {Name: "timeout", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("1s")}},
		},
	}
	cfg := &module.Config{Category: "balancer", Name: "test", Type: "wlc", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := newWLCBalancer(cfg, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WLCBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	provider.ProvideUpdates(balancer)

	backendChan := make(chan *backend.Backend)
	go func() {
		be, rel := balancer.GetBackend(true)
		defer rel()
		backendChan <- be
	}()

	time.Sleep(50 * time.Millisecond)

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

func TestWLCBalancer_RegistryIntegration(t *testing.T) {
	src := `
balancer "wlc" "test" {
	source = "src1"
	weight = 2
}
`
	block := testutil.ParseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := module.DecodeConfigBlock(block, ctx, "balancer")
	if diags.HasErrors() {
		t.Fatalf("DecodeConfigBlock failed: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod, err := module.New(cfg, wg, bgCtx, "balancer")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	balancer := mod.(*WLCBalancer)

	provider := &testutil.DummyProvider{ID: "src1", Backends: backend.NewRegistry()}
	modules := make(module.ModulesRegistry)
	modules.AddModule("src1", provider)

	if err := balancer.Bind(modules); err != nil {
		t.Fatalf("Bind failed: %v", err)
	}
}

