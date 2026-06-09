package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// parseHCL is a test helper that parses an HCL string into an hcl.Block.
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

// TestDecodeConfigBlock verifies the decoding of the 'metrics' configuration block from HCL.
// It ensures that the metrics server address is correctly parsed.
func TestDecodeConfigBlock(t *testing.T) {
	src := `
metrics {
	address = "127.0.0.1:9090"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}
	if cfg.Address != "127.0.0.1:9090" {
		t.Errorf("Expected address to be 127.0.0.1:9090, got %s", cfg.Address)
	}
}

// TestHttpLogWrapper verifies the logging middleware for HTTP requests.
// It ensures that:
// 1. The wrapped handler is correctly invoked.
// 2. The HTTP response status code is preserved.
func TestHttpLogWrapper(t *testing.T) {
	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	wrapped := HttpLogWrapper(handler)

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if !called {
		t.Errorf("Expected original handler to be called")
	}
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", w.Code)
	}
}

// TestNewHTTPServer verifies the creation and graceful shutdown of the metrics HTTP server.
// It tests:
// 1. Starting the server on an ephemeral port.
// 2. Shutting down the server using a context cancellation.
// 3. Ensuring all background goroutines finish (via sync.WaitGroup).
func TestNewHTTPServer(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	// Start HTTP server on an available port (127.0.0.1:0)
	NewHTTPServer("127.0.0.1:0", wg, ctx)

	// Trigger shutdown
	cancel()

	// Wait for the server to stop completely
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success: server shut down cleanly
	case <-time.After(1 * time.Second):
		t.Fatalf("Server shutdown timed out")
	}
}
