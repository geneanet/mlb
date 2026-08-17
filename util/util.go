package util

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
)

// ParseSize parses a decimal integer from a byte slice without performing heap allocations.
// It supports negative numbers and validates that all characters are decimal digits.
func ParseSize(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty integer")
	}

	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
		if len(b) == 0 {
			return 0, fmt.Errorf("invalid integer: \"-\"")
		}
	}

	res := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		res = res*10 + int(c-'0')
	}

	if neg {
		return -res, nil
	}
	return res, nil
}

// ParseByteSize parses a human-readable byte size string (e.g. "10mb", "64kb") into an int64.
// It supports k, kb, m, mb, g, gb, t, tb suffixes (case-insensitive, power of 1024).
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("empty byte size")
	}

	var multiplier int64 = 1
	var unit string
	var valStr string

	// Find the first non-digit character
	idx := strings.IndexFunc(s, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.' && r != '-'
	})

	if idx == -1 {
		valStr = s
	} else {
		valStr = strings.TrimSpace(s[:idx])
		unit = strings.TrimSpace(s[idx:])
	}

	switch unit {
	case "", "b", "byte", "bytes":
		multiplier = 1
	case "k", "kb", "kib":
		multiplier = 1024
	case "m", "mb", "mib":
		multiplier = 1024 * 1024
	case "g", "gb", "gib":
		multiplier = 1024 * 1024 * 1024
	case "t", "tb", "tib":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("invalid unit %q", unit)
	}

	if valStr == "" {
		return 0, fmt.Errorf("missing value in byte size %q", s)
	}

	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number %q: %w", valStr, err)
	}

	return int64(val * float64(multiplier)), nil
}

// FromCtyByteSize converts a cty.Value to an int representing a byte size.
// It handles both cty.Number and cty.String (with optional human-readable suffixes).
func FromCtyByteSize(v cty.Value) (int, error) {
	if v.IsNull() || !v.IsKnown() {
		return 0, nil
	}

	switch v.Type() {
	case cty.Number:
		var i int
		err := gocty.FromCtyValue(v, &i)
		return i, err
	case cty.String:
		val, err := ParseByteSize(v.AsString())
		if err != nil {
			return 0, err
		}
		return int(val), nil
	default:
		return 0, fmt.Errorf("unexpected type %s for byte size", v.Type().FriendlyName())
	}
}
