package misc

import (
	"testing"
	"time"
)

// TestExponentialBackoffTicker verifies the functionality of the ExponentialBackoffTicker.
// It tests:
//  1. Successful initialization.
//  2. Ticker channel (C) receives initial ticks.
//  3. ApplyBackoff: Ensures the ticker interval correctly doubles (based on backoffFactor)
//     until it hits the configured maxPeriod cap.
//  4. Reset: Ensures the ticker interval can be reset back to the default initial period.
//  5. State tracking: Verifies that ApplyBackoff and Reset return the correct 'updated' boolean status.
func TestExponentialBackoffTicker(t *testing.T) {
	defaultPeriod := 10 * time.Millisecond
	maxPeriod := 50 * time.Millisecond
	backoffFactor := 2.0

	ticker := NewExponentialBackoffTicker(defaultPeriod, maxPeriod, backoffFactor)
	if ticker == nil {
		t.Fatalf("Expected ticker to not be nil")
	}
	defer ticker.Stop()

	// Wait for the first tick to ensure the background goroutine and channel are working
	select {
	case <-ticker.C:
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Timeout waiting for initial ticker tick")
	}

	// Verify Reset behavior when already at default period (should return updated=false)
	period, updated := ticker.Reset()
	if updated {
		t.Errorf("Expected updated to be false since period is already default")
	}
	if period != defaultPeriod {
		t.Errorf("Expected period to be %v (default), got %v", defaultPeriod, period)
	}

	// Scenario 1: Apply first backoff (10ms * 2.0 = 20ms)
	period, updated = ticker.ApplyBackoff()
	if !updated {
		t.Errorf("Expected updated to be true for first backoff")
	}
	if period != 20*time.Millisecond {
		t.Errorf("Expected period to be 20ms, got %v", period)
	}

	// Scenario 2: Apply second backoff (20ms * 2.0 = 40ms)
	period, updated = ticker.ApplyBackoff()
	if !updated {
		t.Errorf("Expected updated to be true for second backoff")
	}
	if period != 40*time.Millisecond {
		t.Errorf("Expected period to be 40ms, got %v", period)
	}

	// Scenario 3: Apply third backoff which should hit the maxPeriod (50ms) cap
	period, updated = ticker.ApplyBackoff()
	if !updated {
		t.Errorf("Expected updated to be true for backoff hitting max cap")
	}
	if period != maxPeriod {
		t.Errorf("Expected period to be %v (max), got %v", maxPeriod, period)
	}

	// Scenario 4: Apply backoff again while already at max (should return updated=false)
	period, updated = ticker.ApplyBackoff()
	if updated {
		t.Errorf("Expected updated to be false when already at maxPeriod")
	}
	if period != maxPeriod {
		t.Errorf("Expected period to remain %v, got %v", maxPeriod, period)
	}

	// Scenario 5: Reset ticker back to its initial default period
	period, updated = ticker.Reset()
	if !updated {
		t.Errorf("Expected updated to be true for Reset")
	}
	if period != defaultPeriod {
		t.Errorf("Expected period to be %v (default), got %v", defaultPeriod, period)
	}
}
