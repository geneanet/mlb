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
}
