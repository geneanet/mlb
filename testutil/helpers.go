package testutil

import (
	"errors"
	"mlb/backend"
	"mlb/module"
	"net"
	"sync"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

// MockFailingListener is a net.Listener that fails Accept() a fixed number of times.
type MockFailingListener struct {
	net.Listener
	failCount int
	MaxFails  int
	mu        sync.Mutex
}

// NewMockFailingListener wraps a net.Listener to fail Accept() maxFails times.
func NewMockFailingListener(ln net.Listener, maxFails int) *MockFailingListener {
	return &MockFailingListener{
		Listener: ln,
		MaxFails: maxFails,
	}
}

// Accept returns an error until MaxFails is reached, then delegates to the underlying listener.
func (m *MockFailingListener) Accept() (net.Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failCount < m.MaxFails {
		m.failCount++
		return nil, errors.New("simulated temporary accept error")
	}
	return m.Listener.Accept()
}

// FailCount returns the number of failed accept calls so far.
func (m *MockFailingListener) FailCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failCount
}

// DummySubscriber implements backend.BackendUpdateSubscriber for testing.
type DummySubscriber struct {
	Updates []backend.BackendUpdate
	Wg      sync.WaitGroup
	mu      sync.Mutex
}

// ReceiveUpdate records an update and decrements the internal WaitGroup.
func (d *DummySubscriber) ReceiveUpdate(u backend.BackendUpdate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Updates = append(d.Updates, u)
	d.Wg.Done()
}

// GetUpdates returns a copy of the updates in a thread-safe manner.
func (d *DummySubscriber) GetUpdates() []backend.BackendUpdate {
	d.mu.Lock()
	defer d.mu.Unlock()
	res := make([]backend.BackendUpdate, len(d.Updates))
	copy(res, d.Updates)
	return res
}

// DummyProvider implements backend.BackendUpdateProvider for testing.
type DummyProvider struct {
	ID       string
	Backends *backend.Registry
}

// Bind is a no-op implementation of the module.Module interface.
func (d *DummyProvider) Bind(modules module.ModulesRegistry) {}

// ProvideUpdates registers a subscriber.
func (d *DummyProvider) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	d.Backends.Subscribe(s)
}

// SendUpdate broadcasts an update to all registered subscribers.
func (d *DummyProvider) SendUpdate(u backend.BackendUpdate) {
	d.Backends.Publish(u)
}

// ParseHCL is a helper that parses a HCL string into an hcl.Block.
func ParseHCL(t testing.TB, src string) *hcl.Block {
	t.Helper()
	file, diags := hclsyntax.ParseConfig([]byte(src), "test.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("Failed to parse config: %s", diags.Error())
		return nil
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		t.Fatalf("Failed to get body")
		return nil
	}
	if len(body.Blocks) == 0 {
		t.Fatalf("No blocks found")
		return nil
	}
	// Convert hclsyntax.Block to hcl.Block
	b := body.Blocks[0]
	return b.AsHCLBlock()
}
