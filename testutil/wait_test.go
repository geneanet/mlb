package testutil

import (
	"testing"
	"time"
)

func TestEventually(t *testing.T) {
	// Success case
	count := 0
	Eventually(t, func() bool {
		count++
		return count >= 3
	}, 100*time.Millisecond, 10*time.Millisecond)

	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}

	// Timeout case (using a mock T would be better, but we can just check it doesn't crash)
	mockT := &testing.T{}
	Eventually(mockT, func() bool {
		return false
	}, 50*time.Millisecond, 10*time.Millisecond, "custom message")
	
	if !mockT.Failed() {
		t.Errorf("expected Eventually to fail on timeout")
	}

	// Timeout case without custom message
	mockT2 := &testing.T{}
	Eventually(mockT2, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond)
	if !mockT2.Failed() {
		t.Errorf("expected Eventually to fail on timeout without message")
	}
}

func TestConsistently(t *testing.T) {
	// Success case
	count := 0
	Consistently(t, func() bool {
		count++
		return true
	}, 50*time.Millisecond, 10*time.Millisecond)

	if count < 4 {
		t.Errorf("expected count at least 4, got %d", count)
	}

	// Failure case
	mockT := &testing.T{}
	Consistently(mockT, func() bool {
		count++
		return count < 10
	}, 200*time.Millisecond, 10*time.Millisecond)

	if !mockT.Failed() {
		t.Errorf("expected Consistently to fail when condition returns false")
	}

	// Failure case without custom message
	mockT2 := &testing.T{}
	Consistently(mockT2, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond)
	if !mockT2.Failed() {
		t.Errorf("expected Consistently to fail without message")
	}
}
