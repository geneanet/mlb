package misc

import (
	"testing"
	"time"
)

func TestExponentialBackoffTicker(t *testing.T) {
	defaultPeriod := 10 * time.Millisecond
	maxPeriod := 50 * time.Millisecond
	backoffFactor := 2.0

	ticker := NewExponentialBackoffTicker(defaultPeriod, maxPeriod, backoffFactor)
	if ticker == nil {
		t.Fatalf("Expected ticker to not be nil")
	}
	defer ticker.Stop()

	// Wait for the first tick to ensure the channel works
	select {
	case <-ticker.C:
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("Timeout waiting for ticker")
	}

	// Missing Coverage: Call Reset while period is already at default
	period, updated := ticker.Reset()
	if updated {
		t.Errorf("Expected updated to be false since period is already default")
	}
	if period != defaultPeriod {
		t.Errorf("Expected period to be %v (default), got %v", defaultPeriod, period)
	}

	// Apply backoff (10ms * 2.0 = 20ms)
	period, updated = ticker.ApplyBackoff()
	if !updated {
		t.Errorf("Expected updated to be true")
	}
	if period != 20*time.Millisecond {
		t.Errorf("Expected period to be 20ms, got %v", period)
	}

	// Apply backoff again (20ms * 2.0 = 40ms)
	period, updated = ticker.ApplyBackoff()
	if !updated {
		t.Errorf("Expected updated to be true")
	}
	if period != 40*time.Millisecond {
		t.Errorf("Expected period to be 40ms, got %v", period)
	}

	// Apply backoff hitting max period cap
	period, updated = ticker.ApplyBackoff()
	if !updated {
		t.Errorf("Expected updated to be true")
	}
	if period != maxPeriod {
		t.Errorf("Expected period to be %v (max), got %v", maxPeriod, period)
	}

	// Apply backoff again, should not update since it's already capped
	period, updated = ticker.ApplyBackoff()
	if updated {
		t.Errorf("Expected updated to be false")
	}
	if period != maxPeriod {
		t.Errorf("Expected period to be %v, got %v", maxPeriod, period)
	}

	// Reset back to default period
	period, updated = ticker.Reset()
	if !updated {
		t.Errorf("Expected updated to be true")
	}
	if period != defaultPeriod {
		t.Errorf("Expected period to be %v (default), got %v", defaultPeriod, period)
	}
}
