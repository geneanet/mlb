package system

import (
	"path/filepath"
	"testing"
)

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
