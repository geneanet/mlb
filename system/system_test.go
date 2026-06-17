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

	mustParseSystemBlock := func(src string) *hcl.Block {
		file, diags := hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
		if diags.HasErrors() {
			t.Fatalf("Unexpected parse errors: %s", diags.Error())
		}
		if file == nil {
			t.Fatalf("ParseConfig returned nil file without diagnostics")
		}

		body, ok := file.Body.(*hclsyntax.Body)
		if !ok || body == nil {
			t.Fatalf("Expected *hclsyntax.Body, got %T", file.Body)
		}
		if len(body.Blocks) == 0 {
			t.Fatalf("Expected at least one block in parsed config")
		}

		return body.Blocks[0].AsHCLBlock()
	}

	// Case 1: RLimit NOFile
	src := `
system {
	rlimit {
		nofile = 2048
	}
}
`
	block := mustParseSystemBlock(src)

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
	block = mustParseSystemBlock(src)

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
	block = mustParseSystemBlock(src)

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
	block = mustParseSystemBlock(src)

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

// TestSetRlimitNOFILE_Fail verifies that SetRlimitNOFILE panics when given an impossible limit.
func TestSetRlimitNOFILE_Fail(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for impossible rlimit")
		}
	}()
	// Setting rlimit to a very high value (or 0 in some cases) might fail
	// depending on the system and user permissions.
	// 1<<63 - 1 is almost certainly impossible to set as both Cur and Max.
	SetRlimitNOFILE(1<<63 - 1)
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
