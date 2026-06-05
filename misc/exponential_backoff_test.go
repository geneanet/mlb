package misc

import (
	"context"
	"testing"
	"time"
)

// TestExponentialBackoff verifies the calculation logic of the ExponentialBackoff helper.
// It ensures that:
// 1. Subsequent calls to Get() return exponentially increasing durations.
// 2. The returned duration is capped by the configured maxPeriod.
// 3. Reset() correctly restores the backoff state to the initial default period.
func TestExponentialBackoff(t *testing.T) {
	defaultPeriod := 10 * time.Millisecond
	maxPeriod := 50 * time.Millisecond
	backoffFactor := 2.0

	eb := NewExponentialBackoff(defaultPeriod, maxPeriod, backoffFactor)
	if eb == nil {
		t.Fatalf("Expected non-nil ExponentialBackoff")
	}

	// 1st Get: returns 10ms, next will be 20ms
	dur := eb.Get()
	if dur != 10*time.Millisecond {
		t.Errorf("Expected 10ms, got %v", dur)
	}

	// 2nd Get: returns 20ms, next will be 40ms
	dur = eb.Get()
	if dur != 20*time.Millisecond {
		t.Errorf("Expected 20ms, got %v", dur)
	}

	// 3rd Get: returns 40ms, next will be 80ms (capped to 50ms)
	dur = eb.Get()
	if dur != 40*time.Millisecond {
		t.Errorf("Expected 40ms, got %v", dur)
	}

	// 4th Get: returns 50ms (maxPeriod cap reached)
	dur = eb.Get()
	if dur != 50*time.Millisecond {
		t.Errorf("Expected 50ms, got %v", dur)
	}

	// Verify Reset behavior
	eb.Reset()
	dur = eb.Get()
	if dur != 10*time.Millisecond {
		t.Errorf("Expected 10ms after reset, got %v", dur)
	}
}

// TestExponentialBackoff_Sleep verifies the Sleep method of ExponentialBackoff.
// It tests:
// 1. Normal operation: Sleep blocks for the current backoff duration.
// 2. Context cancellation: Sleep returns immediately if the provided context is cancelled.
func TestExponentialBackoff_Sleep(t *testing.T) {
	eb := NewExponentialBackoff(10*time.Millisecond, 50*time.Millisecond, 2.0)

	// Scenario 1: Natural sleep using the current backoff period (10ms)
	start := time.Now()
	eb.Sleep(context.Background())
	duration := time.Since(start)
	if duration < 10*time.Millisecond {
		t.Errorf("Sleep was too short, expected ~10ms, got %v", duration)
	}

	// Scenario 2: Context cancellation during sleep
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel the context immediately before calling Sleep

	start = time.Now()
	// Sleep should detect the cancelled context and return immediately without waiting for the backoff
	eb.Sleep(ctx)
	duration = time.Since(start)
	if duration > 5*time.Millisecond {
		t.Errorf("Sleep did not return immediately on cancelled context, took %v", duration)
	}
}
