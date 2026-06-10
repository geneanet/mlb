package testutil

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

type mockTB struct {
	testing.TB
	failed      bool
	lastMessage string
}

func (m *mockTB) Errorf(format string, args ...interface{}) {
	m.failed = true
	m.lastMessage = fmt.Sprintf(format, args...)
}

func (m *mockTB) Helper() {}

func (m *mockTB) Failed() bool {
	return m.failed
}

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

	// Timeout case with custom message
	mock := &mockTB{}
	Eventually(mock, func() bool {
		return false
	}, 50*time.Millisecond, 10*time.Millisecond, "custom message", 123)

	if !mock.Failed() {
		t.Errorf("expected Eventually to fail on timeout")
	}
	if !strings.Contains(mock.lastMessage, "custom message 123") {
		t.Errorf("expected error message to contain 'custom message 123', got %q", mock.lastMessage)
	}
	if strings.Contains(mock.lastMessage, "[") {
		t.Errorf("expected error message NOT to contain brackets from slice formatting, got %q", mock.lastMessage)
	}

	// Sprintf-style formatting
	mockSprintf := &mockTB{}
	msg := "formatted %s: %d"
	Eventually(mockSprintf, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond, msg, "value", 456)
	if !strings.Contains(mockSprintf.lastMessage, "formatted value: 456") {
		t.Errorf("expected Sprintf-style formatting to work, got %q", mockSprintf.lastMessage)
	}

	// Timeout case without custom message
	mock2 := &mockTB{}
	Eventually(mock2, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond)
	if !mock2.Failed() {
		t.Errorf("expected Eventually to fail on timeout without message")
	}

	// Final check success
	mock3 := &mockTB{}
	start := time.Now()
	timeout := 50 * time.Millisecond
	Eventually(mock3, func() bool {
		// Only return true when we are at or past the timeout
		return time.Since(start) >= timeout
	}, timeout, 10*time.Millisecond)
	if mock3.Failed() {
		t.Errorf("expected Eventually to succeed on final check")
	}

	// One argument message
	mock4 := &mockTB{}
	Eventually(mock4, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond, "single message")
	if !strings.Contains(mock4.lastMessage, "single message") {
		t.Errorf("expected error message to contain 'single message', got %q", mock4.lastMessage)
	}

	// Non-string first argument
	mock5 := &mockTB{}
	Eventually(mock5, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond, 123, 456)
	if !strings.Contains(mock5.lastMessage, "123 456") {
		t.Errorf("expected error message to contain '123 456', got %q", mock5.lastMessage)
	}
}

func TestConsistently(t *testing.T) {
	// Success case
	count := 0
	Consistently(t, func() bool {
		count++
		return true
	}, 50*time.Millisecond, 10*time.Millisecond)

	if count == 0 {
		t.Errorf("expected condition to be evaluated at least once, got %d", count)
	}

	// Failure case with custom message
	mock := &mockTB{}
	count = 0
	Consistently(mock, func() bool {
		count++
		return count < 3
	}, 200*time.Millisecond, 10*time.Millisecond, "failed on count", 3)

	if !mock.Failed() {
		t.Errorf("expected Consistently to fail when condition returns false")
	}
	if !strings.Contains(mock.lastMessage, "failed on count 3") {
		t.Errorf("expected error message to contain 'failed on count 3', got %q", mock.lastMessage)
	}

	// Failure case without custom message
	mock2 := &mockTB{}
	Consistently(mock2, func() bool {
		return false
	}, 10*time.Millisecond, 5*time.Millisecond)
	if !mock2.Failed() {
		t.Errorf("expected Consistently to fail without message")
	}

	// Final check failure
	mock3 := &mockTB{}
	start := time.Now()
	duration := 50 * time.Millisecond
	Consistently(mock3, func() bool {
		// Return true during the duration, but false at/after the boundary
		return time.Since(start) < duration
	}, duration, 10*time.Millisecond)
	if !mock3.Failed() {
		t.Errorf("expected Consistently to fail on final check")
	}

	// Final check failure with message
	mock4 := &mockTB{}
	start = time.Now()
	Consistently(mock4, func() bool {
		return time.Since(start) < duration
	}, duration, 10*time.Millisecond, "final check failure message")
	if !mock4.Failed() || !strings.Contains(mock4.lastMessage, "final check failure message") {
		t.Errorf("expected Consistently to fail on final check with message")
	}
}

func TestFormatMsgAndArgs(t *testing.T) {
	if got := formatMsgAndArgs(); got != "" {
		t.Errorf("expected empty string for no args, got %q", got)
	}
	if got := formatMsgAndArgs("one arg"); got != "one arg" {
		t.Errorf("expected 'one arg', got %q", got)
	}
	fmtStr := "format %s"
	if got := formatMsgAndArgs(fmtStr, "arg"); got != "format arg" {
		t.Errorf("expected 'format arg', got %q", got)
	}
	if got := formatMsgAndArgs("no format", "arg"); got != "no format arg" {
		t.Errorf("expected 'no format arg', got %q", got)
	}
}
