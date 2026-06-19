package util

import "fmt"

// parseSize parses a decimal integer from a byte slice without performing heap allocations.
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
