package system

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

type testListener struct{}

func (testListener) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func TestListen_Unix(t *testing.T) {
	upg := testListener{}

	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")
	address := "unix:" + socketPath

	// 1. Test normal listen
	l, err := Listen(upg, "tcp", address)
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	if l.Addr().Network() != "unix" {
		t.Errorf("Expected network 'unix', got %q", l.Addr().Network())
	}
	if l.Addr().String() != socketPath {
		t.Errorf("Expected address %q, got %q", socketPath, l.Addr().String())
	}
	_ = l.Close()
	if _, err := os.Stat(socketPath); err == nil {
		t.Errorf("Expected socket file %q to be removed after close", socketPath)
	}

	// 2. Test stale socket cleanup
	// Create a dummy file at the socket path
	if err := os.WriteFile(socketPath, []byte("stale"), 0644); err != nil {
		t.Fatalf("Failed to create stale socket file: %v", err)
	}

	l2, err := Listen(upg, "tcp", address)
	if err != nil {
		t.Fatalf("Listen with stale socket failed: %v", err)
	}
	_ = l2.Close()
}

func TestListen_RequiresUpgrader(t *testing.T) {
	if _, err := Listen(nil, "tcp", "127.0.0.1:0"); err == nil {
		t.Fatal("expected an error without a listener manager")
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

	// Verify that Listen now uses the upgrader (it should still work)
	l, err := Listen(upg, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen with upgrader failed: %v", err)
	}
	defer func() { _ = l.Close() }()

	socketPath := filepath.Join(tmpDir, "stale.sock")
	if err := os.WriteFile(socketPath, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	unixListener, err := Listen(upg, "tcp", "unix:"+socketPath)
	if err != nil {
		t.Fatalf("Listen with stale socket failed: %v", err)
	}
	_ = unixListener.Close()
}
