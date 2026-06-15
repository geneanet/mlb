package misc

import (
	"errors"
	"testing"
)

// TestEnsureError verifies the EnsureError utility function which converts various types into errors.
// It tests conversion for:
// 1. Existing error types (should return as is).
// 2. String types (should be wrapped in a new error).
// 3. Other types (e.g., int, should be converted to a string representation and wrapped).
func TestEnsureError(t *testing.T) {
	// Scenario 1: Input is already an error object
	err := errors.New("test error")
	if e := EnsureError(err); e != err {
		t.Errorf("Expected same error instance %v, got %v", err, e)
	}

	// Scenario 2: Input is a string (frequent case for recovered panics)
	str := "string error"
	if e := EnsureError(str); e.Error() != str {
		t.Errorf("Expected error string '%s', got '%s'", str, e.Error())
	}

	// Scenario 3: Input is an arbitrary type (e.g. integer)
	num := 42
	if e := EnsureError(num); e == nil {
		t.Errorf("Expected an error object, got nil for input %v", num)
	}
}
