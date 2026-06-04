package misc

import (
	"context"
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	defaultPeriod := 10 * time.Millisecond
	maxPeriod := 50 * time.Millisecond
	backoffFactor := 2.0

	eb := NewExponentialBackoff(defaultPeriod, maxPeriod, backoffFactor)
	if eb == nil {
		t.Fatalf("Expected non-nil ExponentialBackoff")
	}

	// Get 1 (returns 10, sets to 20)
	dur := eb.Get()
	if dur != 10*time.Millisecond {
		t.Errorf("Expected 10ms, got %v", dur)
	}

	// Get 2 (returns 20, sets to 40)
	dur = eb.Get()
	if dur != 20*time.Millisecond {
		t.Errorf("Expected 20ms, got %v", dur)
	}

	// Get 3 (returns 40, sets to 80 -> caps to 50)
	dur = eb.Get()
	if dur != 40*time.Millisecond {
		t.Errorf("Expected 40ms, got %v", dur)
	}

	// Get 4 (caps at max)
	dur = eb.Get()
	if dur != 50*time.Millisecond {
		t.Errorf("Expected 50ms, got %v", dur)
	}

	// Reset
	eb.Reset()
	dur = eb.Get()
	if dur != 10*time.Millisecond {
		t.Errorf("Expected 10ms after reset, got %v", dur)
	}
}

func TestExponentialBackoff_Sleep(t *testing.T) {
	eb := NewExponentialBackoff(10*time.Millisecond, 50*time.Millisecond, 2.0)

	// Test natural sleep (Wait for timer.C)
	eb.Sleep(context.Background()) // Should take roughly 10ms

	// Test context cancellation (Wait for ctx.Done())
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Since it's cancelled, Sleep shouldn't take the full backoff (20ms) duration
	eb.Sleep(ctx)
}
