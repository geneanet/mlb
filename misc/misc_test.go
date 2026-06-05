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

// TestMapValues verifies the MapValues utility function.
// It ensures that all values from a map are correctly extracted into a slice,
// regardless of their order (since maps are unordered in Go).
func TestMapValues(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	vals := MapValues(m)
	if len(vals) != 2 {
		t.Fatalf("Expected slice of length 2, got %d", len(vals))
	}

	// Verify that both expected values (1 and 2) are present in the resulting slice
	var has1, has2 bool
	for _, v := range vals {
		if v == 1 {
			has1 = true
		}
		if v == 2 {
			has2 = true
		}
	}
	if !has1 || !has2 {
		t.Errorf("One or more values missing from extracted slice: %v", vals)
	}
}
