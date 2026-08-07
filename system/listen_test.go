package system

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListen_Unix(t *testing.T) {
	// Ensure upgrader is nil
	upgrader = nil

	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")
	address := "unix:" + socketPath

	// 1. Test normal listen
	l, err := Listen("tcp", address)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	if l.Addr().Network() != "unix" {
		t.Errorf("Expected network 'unix', got %q", l.Addr().Network())
	}
	if l.Addr().String() != socketPath {
		t.Errorf("Expected address %q, got %q", socketPath, l.Addr().String())
	}
	l.Close()
	if _, err := os.Stat(socketPath); err == nil {
		t.Errorf("Expected socket file %q to be removed after close", socketPath)
	}

	// 2. Test stale socket cleanup
	// Create a dummy file at the socket path
	if err := os.WriteFile(socketPath, []byte("stale"), 0644); err != nil {
		t.Fatalf("Failed to create stale socket file: %v", err)
	}

	l2, err := Listen("tcp", address)
	if err != nil {
		t.Fatalf("Listen with stale socket failed: %v", err)
	}
	l2.Close()
}

func TestListen_Fallback(t *testing.T) {
	// Ensure upgrader is nil
	upgrader = nil

	l, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	defer l.Close()

	if l.Addr() == nil {
		t.Error("Expected listener address to be non-nil")
	}
}

func TestInitTableflip(t *testing.T) {
	tmpDir := t.TempDir()
	pidFile := filepath.Join(tmpDir, "test.pid")

	upg, err := InitTableflip(pidFile)
	if err != nil {
		t.Fatalf("InitTableflip failed: %v", err)
	}
	defer upg.Stop()

	if upg == nil {
		t.Fatal("Expected non-nil upgrader")
	}

	if upgrader != upg {
		t.Error("Global upgrader not set correctly")
	}

	// Verify that Listen now uses the upgrader (it should still work)
	l, err := Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen with upgrader failed: %v", err)
	}
	defer l.Close()

	// Clean up for other tests
	upgrader = nil
}
