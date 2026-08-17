package main

import (
	"context"
	"errors"
	"mlb/module"
	"testing"
	"time"
)

type mockReadyReporter struct {
	ready chan struct{}
}

func (m *mockReadyReporter) Ready() <-chan struct{} {
	return m.ready
}
func (m *mockReadyReporter) Bind(modules module.ModulesRegistry) error {
	return nil
}

type mockUpgrader struct {
	readyCalled bool
	errToReturn error
}

func (m *mockUpgrader) Ready() error {
	m.readyCalled = true
	return m.errToReturn
}

func TestSignalReadiness(t *testing.T) {
	t.Run("AllReady", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		r1 := &mockReadyReporter{ready: make(chan struct{})}
		ml := make(module.ModulesRegistry)
		ml.AddModule("mod1", r1)

		upg := &mockUpgrader{}

		done := make(chan struct{})
		go func() {
			signalReadiness(ctx, ml, upg)
			close(done)
		}()

		// Should block
		select {
		case <-done:
			t.Fatal("signalReadiness returned before ready")
		case <-time.After(10 * time.Millisecond):
		}

		close(r1.ready)

		select {
		case <-done:
			if !upg.readyCalled {
				t.Error("Expected upg.Ready() to be called")
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("signalReadiness timed out")
		}
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		r1 := &mockReadyReporter{ready: make(chan struct{})}
		ml := make(module.ModulesRegistry)
		ml.AddModule("mod1", r1)

		upg := &mockUpgrader{}

		done := make(chan struct{})
		go func() {
			signalReadiness(ctx, ml, upg)
			close(done)
		}()

		cancel()

		select {
		case <-done:
			if upg.readyCalled {
				t.Error("Expected upg.Ready() NOT to be called when context is cancelled")
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("signalReadiness timed out on context cancel")
		}
	})

	t.Run("UpgReadyError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ml := make(module.ModulesRegistry)
		upg := &mockUpgrader{errToReturn: errors.New("upg ready err")}

		// Should not panic or block
		signalReadiness(ctx, ml, upg)

		if !upg.readyCalled {
			t.Error("Expected upg.Ready() to be called")
		}
	})
}
