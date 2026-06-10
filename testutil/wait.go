package testutil

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// Eventually polls the condition function every tick until it returns true or timeout is reached.
// If the timeout is reached, it fails the test with the given message.
func Eventually(t *testing.T, condition func() bool, timeout time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if condition() {
			return
		}
		time.Sleep(tick)
	}

	if len(msgAndArgs) > 0 {
		var msg string
		if len(msgAndArgs) == 1 {
			msg = fmt.Sprint(msgAndArgs[0])
		} else if format, ok := msgAndArgs[0].(string); ok {
			msg = fmt.Sprintf(format, msgAndArgs[1:]...)
		} else {
			msg = fmt.Sprint(msgAndArgs...)
		}
		t.Errorf("Condition not met within %v: %s", timeout, msg)
	} else {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("%s:%d: Condition not met within %v", file, line, timeout)
	}
}

// Consistently ensures the condition function returns true for the entire duration by checking every tick.
// If the condition returns false at any point, it fails the test with the given message.
func Consistently(t *testing.T, condition func() bool, duration time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	end := time.Now().Add(duration)
	for time.Now().Before(end) {
		if !condition() {
			if len(msgAndArgs) > 0 {
				if format, ok := msgAndArgs[0].(string); ok {
					t.Errorf("Condition failed within %v: "+format, append([]interface{}{duration}, msgAndArgs[1:]...)...)
				} else {
					t.Errorf("Condition failed within %v: %v", duration, msgAndArgs...)
				}
			} else {
				_, file, line, _ := runtime.Caller(1)
				t.Errorf("%s:%d: Condition failed within %v", file, line, duration)
			}
			return
		}
		time.Sleep(tick)
	}
}
