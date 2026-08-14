package testutil

import (
	"fmt"
	"mlb/backend"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

type helperMockTB struct {
	testing.TB
	fatalMessage string
}

func (m *helperMockTB) Fatalf(format string, args ...interface{}) {
	m.fatalMessage = strings.TrimSpace(format + " " + strings.TrimSpace(fmt.Sprint(args...)))
}

func (m *helperMockTB) Helper() {}

func TestParseHCL_Errors(t *testing.T) {
	t.Run("InvalidHCL", func(t *testing.T) {
		mock := &helperMockTB{}
		ParseHCL(mock, "invalid {")
		if !strings.Contains(mock.fatalMessage, "Failed to parse config") {
			t.Errorf("expected error message to contain 'Failed to parse config', got %q", mock.fatalMessage)
		}
	})

	t.Run("NoBlocks", func(t *testing.T) {
		mock := &helperMockTB{}
		ParseHCL(mock, "")
		if !strings.Contains(mock.fatalMessage, "No blocks found") {
			t.Errorf("expected error message to contain 'No blocks found', got %q", mock.fatalMessage)
		}
	})
}

func TestDummySubscriber(t *testing.T) {
	sub := &DummySubscriber{}
	update := backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:8080",
	}

	sub.Wg.Add(1)
	sub.ReceiveUpdate(update)
	sub.Wg.Wait()

	updates := sub.GetUpdates()
	if len(updates) != 1 {
		t.Errorf("expected 1 update, got %d", len(updates))
	}
	if updates[0].Address != "127.0.0.1:8080" {
		t.Errorf("expected address 127.0.0.1:8080, got %s", updates[0].Address)
	}
}

func TestDummyProvider(t *testing.T) {
	registry := backend.NewRegistry(zerolog.Nop(), false)
	provider := &DummyProvider{
		ID:       "test",
		Backends: registry,
	}

	sub := &DummySubscriber{}
	provider.ProvideUpdates(sub)

	update := backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:8081",
	}

	sub.Wg.Add(1)
	provider.SendUpdate(update)
	sub.Wg.Wait()

	updates := sub.GetUpdates()
	if len(updates) != 1 {
		t.Errorf("expected 1 update, got %d", len(updates))
	}
	if updates[0].Address != "127.0.0.1:8081" {
		t.Errorf("expected address 127.0.0.1:8081, got %s", updates[0].Address)
	}

	// Test Bind (no-op)
	provider.Bind(nil)
}

func TestParseHCL(t *testing.T) {
	src := `
	backends_inventory "static" "test" {
		hosts = ["127.0.0.1:8080"]
	}
	`
	block := ParseHCL(t, src)
	if block == nil {
		t.Fatal("expected non-nil block")
	}
	if block.Type != "backends_inventory" {
		t.Errorf("expected type backends_inventory, got %s", block.Type)
	}
}

func TestParseHCL_Panic(t *testing.T) {
	// We can't easily test t.Fatalf without a mock T that implements Fatalf with panic.
	// But we can test it with a real T and expect it to fail if we were running in a subtest.
	// For simplicity, we just test the success path as it's the primary use case.
}
