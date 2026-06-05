package balancer

import (
	"context"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// TestBalancer verifies the decoding, validation, and initialization of balancer modules.
// It checks both unknown and known (wrr) balancer types.
func TestBalancer(t *testing.T) {
	// Case 1: Unknown balancer type should return an error during decoding.
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

	// Case 2: Known balancer type (wrr) should decode successfully.
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

	// Case 3: Create a new module instance from the decoded config.
	wg := &sync.WaitGroup{}
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, bgCtx)
	if mod == nil {
		t.Errorf("Expected module to be created")
	}

	// Case 4: Validate the decoded configuration.
	diags = ValidateConfig(cfg)
	if diags.HasErrors() {
		t.Errorf("Unexpected diags from ValidateConfig: %s", diags.Error())
	}
}
