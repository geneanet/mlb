package misc

import (
	"errors"
	"testing"
)

func TestPanicIfErr(t *testing.T) {
	// Should not panic when err is nil
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Expected no panic, got %v", r)
			}
		}()
		PanicIfErr(nil)
	}()

	// Should panic when err is not nil
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic, got none")
			}
		}()
		PanicIfErr(errors.New("test error"))
	}()
}

func TestEnsureError(t *testing.T) {
	// Test with a standard error
	err := errors.New("test error")
	if e := EnsureError(err); e != err {
		t.Errorf("Expected %v, got %v", err, e)
	}

	// Test with a string (commonly recovered from panic)
	str := "string error"
	if e := EnsureError(str); e.Error() != str {
		t.Errorf("Expected %v, got %v", str, e.Error())
	}

	// Test with a different type
	num := 42
	if e := EnsureError(num); e == nil {
		t.Errorf("Expected an error, got nil")
	}
}

func TestMapValues(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2}
	vals := MapValues(m)
	if len(vals) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(vals))
	}

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
		t.Errorf("Missing values in %v", vals)
	}
}
