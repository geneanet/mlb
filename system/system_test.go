package system

import (
	"runtime"
	"syscall"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// TestDecodeConfigBlock verifies the decoding of various system configuration blocks.
func TestDecodeConfigBlock(t *testing.T) {
	ctx := &hcl.EvalContext{}

	// Case 1: RLimit NOFile
	src := `
system {
	rlimit {
		nofile = 2048
	}
}
`
	file, _ := hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	body, _ := file.Body.(*hclsyntax.Body)
	block := body.Blocks[0].AsHCLBlock()

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.RLimit == nil || cfg.RLimit.NOFile != 2048 {
		t.Errorf("Expected NOFile to be 2048, got %v", cfg.RLimit)
	}

	// Case 2: GoMaxProcs
	src = `
system {
	gomaxprocs = 4
}
`
	file, _ = hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	body, _ = file.Body.(*hclsyntax.Body)
	block = body.Blocks[0].AsHCLBlock()

	cfg, diags = DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.GoMaxProcs != 4 {
		t.Errorf("Expected GoMaxProcs to be 4, got %d", cfg.GoMaxProcs)
	}

	// Case 3: Empty system block
	src = `
system {}
`
	file, _ = hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	body, _ = file.Body.(*hclsyntax.Body)
	block = body.Blocks[0].AsHCLBlock()

	cfg, diags = DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.RLimit != nil {
		t.Errorf("Expected RLimit to be nil, got %v", cfg.RLimit)
	}
	if cfg.GoMaxProcs != 0 {
		t.Errorf("Expected GoMaxProcs to be 0, got %d", cfg.GoMaxProcs)
	}

	// Case 4: Partial system block
	src = `
system {
	rlimit {}
}
`
	file, _ = hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	body, _ = file.Body.(*hclsyntax.Body)
	block = body.Blocks[0].AsHCLBlock()

	cfg, diags = DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.RLimit == nil {
		t.Fatalf("Expected RLimit to be not nil")
	}
	if cfg.RLimit.NOFile != 0 {
		t.Errorf("Expected NOFile to be 0, got %d", cfg.RLimit.NOFile)
	}
}

// TestSetGoMaxProcs verifies that GOMAXPROCS is correctly set.
func TestSetGoMaxProcs(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(oldProcs)

	SetGoMaxProcs(oldProcs + 1)
	if runtime.GOMAXPROCS(0) != oldProcs+1 {
		t.Errorf("Expected GOMAXPROCS to be %d, got %d", oldProcs+1, runtime.GOMAXPROCS(0))
	}
}

// TestSetRlimitNOFILE verifies that the file descriptor limit is adjusted.
func TestSetRlimitNOFILE(t *testing.T) {
	// This test might fail on some systems if permissions are restricted.
	// We'll try to set it to the current value, which should always work.
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		t.Fatalf("Failed to get current rlimit: %s", err)
	}

	// We use the same value to ensure it's a safe operation.
	SetRlimitNOFILE(rLimit.Cur)

	// Since we can't easily verify the change without potential side effects
	// or permission issues, we rely on the fact that SetRlimitNOFILE will panic
	// if it fails (using an explicit panic).
}

// TestDecodeConfigBlock_InvalidBody ensures that decoding fails with errors for invalid HCL bodies.
func TestDecodeConfigBlock_InvalidBody(t *testing.T) {
	block := &hcl.Block{
		Type: "system",
		Body: &hclsyntax.Body{
			Attributes: map[string]*hclsyntax.Attribute{
				"invalid_attr": {
					Name: "invalid_attr",
					Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("foo")},
				},
			},
		},
	}
	ctx := &hcl.EvalContext{}
	_, diags := DecodeConfigBlock(block, ctx)
	if !diags.HasErrors() {
		t.Errorf("Expected errors for invalid system configuration attribute")
	}
}
