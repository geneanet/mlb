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

// TestDecodeConfigBlock verifies the decoding of metrics configuration blocks.
func TestDecodeConfigBlock(t *testing.T) {
	t.Run("Default (no enable_pprof)", func(t *testing.T) {
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
		if cfg.EnablePprof {
			t.Errorf("Expected EnablePprof to be false by default, got true")
		}
	})

	t.Run("enable_pprof = true", func(t *testing.T) {
		src := `
metrics {
	address = "127.0.0.1:9090"
	enable_pprof = true
}
`
		block := parseHCL(t, src)
		ctx := &hcl.EvalContext{}

		cfg, diags := DecodeConfigBlock(block, ctx)
		if diags.HasErrors() {
			t.Fatalf("Unexpected errors: %s", diags.Error())
		}
		if !cfg.EnablePprof {
			t.Errorf("Expected EnablePprof to be true, got false")
		}
	})

	t.Run("enable_pprof = false", func(t *testing.T) {
		src := `
metrics {
	address = "127.0.0.1:9090"
	enable_pprof = false
}
`
		block := parseHCL(t, src)
		ctx := &hcl.EvalContext{}

		cfg, diags := DecodeConfigBlock(block, ctx)
		if diags.HasErrors() {
			t.Fatalf("Unexpected errors: %s", diags.Error())
		}
		if cfg.EnablePprof {
			t.Errorf("Expected EnablePprof to be false, got true")
		}
	})
}

// TestRegisterPprof verifies that RegisterPprof registers pprof endpoints on the mux.
func TestRegisterPprof(t *testing.T) {
	mux := http.NewServeMux()
	RegisterPprof(mux)

	endpoints := []string{
		"/debug/pprof/",
		"/debug/pprof/cmdline",
		"/debug/pprof/symbol",
		"/debug/pprof/heap",
		"/debug/pprof/goroutine",
	}

	for _, endpoint := range endpoints {
		req := httptest.NewRequest("GET", endpoint, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200 for %s, got %d", endpoint, w.Code)
		}
	}
}

// TestHTTPServer_Lifecycle verifies that the metrics HTTP server starts and stops correctly.
func TestHTTPServer_Lifecycle(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	mux := http.NewServeMux()
	// Use port 0 to let the OS assign a free port
	err := NewHTTPServer("127.0.0.1:0", mux, wg, ctx)
	if err != nil {
		t.Fatalf("Failed to start server: %s", err)
	}

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Shutdown the server
	cancel()

	// Wait for the server goroutine to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for server shutdown")
	}
}

// TestHTTPServer_StartError verifies that NewHTTPServer returns an error if it cannot bind to the address.
func TestHTTPServer_StartError(t *testing.T) {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use an invalid address (missing port)
	err := NewHTTPServer("invalid-address", nil, wg, ctx)
	if err == nil {
		t.Errorf("Expected error for invalid address")
	}
}

// TestHttpLogWrapper verifies that the HTTP log wrapper executes the wrapped handler.
func TestHttpLogWrapper(t *testing.T) {
	handlerCalled := false
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	wrapped := HttpLogWrapper(mockHandler)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	wrapped.ServeHTTP(w, req)

	if !handlerCalled {
		t.Errorf("Wrapped handler was not called")
	}
	if w.Code != http.StatusOK {
		t.Errorf("Expected status OK, got %d", w.Code)
	}
}
