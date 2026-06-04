package system

import (
	"syscall"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

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
	return body.Blocks[0].AsHCLBlock()
}

func TestDecodeConfigBlock(t *testing.T) {
	src := `
system {
	rlimit {
		nofile = 2048
	}
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.RLimit.NOFile != 2048 {
		t.Errorf("Expected NOFile to be 2048, got %d", cfg.RLimit.NOFile)
	}
}

func TestSetRlimitNOFILE(t *testing.T) {
	var initialLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &initialLimit)
	if err != nil {
		t.Fatalf("Failed to get current rlimit: %v", err)
	}

	testVal := initialLimit.Cur

	defer func() {
		// Restore after test
		err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &initialLimit)
		if err != nil {
			t.Logf("Failed to restore rlimit: %v", err)
		}
	}()

	SetRlimitNOFILE(testVal)

	var newLimit syscall.Rlimit
	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &newLimit)
	if err != nil {
		t.Fatalf("Failed to get new rlimit: %v", err)
	}

	if newLimit.Cur != testVal {
		t.Errorf("Expected Cur to be %d, got %d", testVal, newLimit.Cur)
	}
	if newLimit.Max != testVal {
		t.Errorf("Expected Max to be %d, got %d", testVal, newLimit.Max)
	}
}
