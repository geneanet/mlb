// Package system provides functionality for managing system-level configurations and resources.
package system

import (
	"runtime"
	"syscall"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// parseHCL is a test helper that parses an HCL string into an hcl.Block.
// It fails the test immediately if parsing errors occur or if no blocks are found.
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

// TestDecodeConfigBlock verifies the decoding of the 'system' configuration block from HCL.
// It ensures that the rlimit settings (specifically nofile) are correctly parsed and
// mapped to the Config struct.
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
	if cfg.RLimit == nil || cfg.RLimit.NOFile != 2048 {
		t.Errorf("Expected NOFile to be 2048, got %v", cfg.RLimit)
	}
}

// TestDecodeConfigBlockOptional verifies that system block works without rlimit.
func TestDecodeConfigBlockOptional(t *testing.T) {
	src := `
system {
	gomaxprocs = 4
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.RLimit != nil {
		t.Errorf("Expected RLimit to be nil, got %v", cfg.RLimit)
	}
	if cfg.GoMaxProcs != 4 {
		t.Errorf("Expected GoMaxProcs to be 4, got %d", cfg.GoMaxProcs)
	}
}

// TestDecodeConfigBlockWithGoMaxProcs verifies decoding of system block with gomaxprocs.
func TestDecodeConfigBlockWithGoMaxProcs(t *testing.T) {
	src := `
system {
	rlimit {
		nofile = 2048
	}
	gomaxprocs = 4
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.GoMaxProcs != 4 {
		t.Errorf("Expected GoMaxProcs to be 4, got %d", cfg.GoMaxProcs)
	}
}

// TestDecodeConfigBlockNoFileOptional verifies that nofile is optional inside rlimit block.
func TestDecodeConfigBlockNoFileOptional(t *testing.T) {
	src := `
system {
	rlimit {
	}
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
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

// TestSetGoMaxProcs verifies the SetGoMaxProcs function.
func TestSetGoMaxProcs(t *testing.T) {
	initial := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(initial)

	SetGoMaxProcs(2)
	if current := runtime.GOMAXPROCS(0); current != 2 {
		t.Errorf("Expected GOMAXPROCS to be 2, got %d", current)
	}
}

// TestSetRlimitNOFILE verifies the SetRlimitNOFILE function's ability to modify
// the RLIMIT_NOFILE process resource limit.
// It tests:
// 1. Getting the current process limit.
// 2. Setting a new limit (re-using the current value for safety during tests).
// 3. Verifying that the limit was correctly applied.
// 4. Properly restoring the original limit after the test completion.
func TestSetRlimitNOFILE(t *testing.T) {
	var initialLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &initialLimit)
	if err != nil {
		t.Fatalf("Failed to get current rlimit: %v", err)
	}

	var testVal uint64
	switch {
	case initialLimit.Cur > 0:
		// Prefer lowering by 1 since reducing soft limit is generally permitted.
		testVal = initialLimit.Cur - 1
	case initialLimit.Cur < initialLimit.Max:
		// If we cannot lower (already 0), try increasing within the hard limit.
		testVal = initialLimit.Cur + 1
	default:
		t.Skipf("No alternate RLIMIT_NOFILE value available (cur=%d, max=%d)", initialLimit.Cur, initialLimit.Max)
	}

	defer func() {
		// Restore after test to avoid affecting the environment or subsequent tests
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
	// If running as root, the maximum limit (hard limit) should also be updated.
	if syscall.Geteuid() == 0 && newLimit.Max != testVal {
		t.Errorf("Expected Max to be %d, got %d", testVal, newLimit.Max)
	}
}

// TestSetRlimitNOFILEPanicOnSetError verifies that SetRlimitNOFILE panics when
// syscall.Setrlimit fails (via misc.PanicIfErr in the implementation).
func TestSetRlimitNOFILEPanicOnSetError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("RLIMIT_NOFILE is not supported on windows")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Expected SetRlimitNOFILE to panic on syscall.Setrlimit error")
		}
	}()

	// Use an invalidly large value to provoke syscall.Setrlimit failure.
	SetRlimitNOFILE(^uint64(0))
}
