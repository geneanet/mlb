package balancer

import (
	"context"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

func TestBalancer(t *testing.T) {
	blockUnknown := &hcl.Block{
		Type:   "balancer",
		Labels: []string{"unknown_type", "test"},
		Body:   &hclsyntax.Body{},
		LabelRanges: []hcl.Range{
			{}, {},
		},
	}
	ctx := &hcl.EvalContext{}
	cfg, diags := DecodeConfigBlock(blockUnknown, ctx)
	if !diags.HasErrors() {
		t.Errorf("Expected error for unknown balancer type")
	}
	if cfg != nil {
		t.Errorf("Expected nil config for unknown balancer type")
	}

	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("src1")}},
			"weight": {Name: "weight", Expr: &hclsyntax.LiteralValueExpr{Val: cty.NumberIntVal(2)}},
		},
	}
	blockKnown := &hcl.Block{
		Type:   "balancer",
		Labels: []string{"wrr", "test"},
		Body:   body,
		LabelRanges: []hcl.Range{
			{}, {},
		},
	}
	cfg, diags = DecodeConfigBlock(blockKnown, ctx)
	if diags.HasErrors() {
		t.Errorf("Unexpected diags: %s", diags.Error())
	}
	if cfg == nil {
		t.Fatalf("Expected valid config")
	}
	if cfg.Type != "wrr" {
		t.Errorf("Expected wrr type")
	}
	if cfg.Name != "test" {
		t.Errorf("Expected test name")
	}

	wg := &sync.WaitGroup{}
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, bgCtx)
	if mod == nil {
		t.Errorf("Expected module to be created")
	}

	diags = ValidateConfig(cfg)
	if diags.HasErrors() {
		t.Errorf("Unexpected diags from ValidateConfig: %s", diags.Error())
	}
}
