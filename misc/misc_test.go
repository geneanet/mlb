package misc

import (
	"errors"
	"testing"
)

// TestPanicIfErr verifies that the PanicIfErr function correctly triggers a panic
// when a non-nil error is passed, and does nothing when the error is nil.
func TestPanicIfErr(t *testing.T) {
	// Case 1: Should NOT panic when err is nil
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Expected no panic for nil error, but got %v", r)
			}
		}()
		PanicIfErr(nil)
	}()

	// Case 2: Should panic when a non-nil error is provided
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic for non-nil error, but got none")
			}
		}()
		PanicIfErr(errors.New("test error"))
	}()
}

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
