package testutil

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Eventually polls the condition function every tick until it returns true or timeout is reached.
// If the timeout is reached, it fails the test with the given message.
func Eventually(t testing.TB, condition func() bool, timeout time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	end := time.Now().Add(timeout)
	for time.Now().Before(end) {
		if condition() {
			return
		}
		time.Sleep(tick)
	}

	if len(msgAndArgs) > 0 {
		t.Errorf("Condition not met within %v: %v", timeout, formatMsgAndArgs(msgAndArgs...))
	} else {
		t.Errorf("Condition not met within %v", timeout)
	}
}

// Consistently ensures the condition function returns true for the entire duration by checking every tick.
// If the condition returns false at any point, it fails the test with the given message.
func Consistently(t testing.TB, condition func() bool, duration time.Duration, tick time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	end := time.Now().Add(duration)
	for time.Now().Before(end) {
		if !condition() {
			if len(msgAndArgs) > 0 {
				t.Errorf("Condition failed within %v: %v", duration, formatMsgAndArgs(msgAndArgs...))
			} else {
				t.Errorf("Condition failed within %v", duration)
			}
			return
		}
		time.Sleep(tick)
	}
}

func formatMsgAndArgs(msgAndArgs ...interface{}) string {
	if len(msgAndArgs) == 0 {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return fmt.Sprint(msgAndArgs[0])
	}
	if s, ok := msgAndArgs[0].(string); ok && strings.Contains(s, "%") {
		return fmt.Sprintf(s, msgAndArgs[1:]...)
	}
	line := fmt.Sprintln(msgAndArgs...)
	return line[0 : len(line)-1] // Get rid of the \n added by Sprintln
}
