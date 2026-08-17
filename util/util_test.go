package util

import (
	"testing"

	"github.com/zclconf/go-cty/cty"
)

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		{"1024", 1024, false},
		{"1k", 1024, false},
		{"1kb", 1024, false},
		{"1KB", 1024, false},
		{"1mb", 1024 * 1024, false},
		{"1.5mb", 1.5 * 1024 * 1024, false},
		{"1gb", 1024 * 1024 * 1024, false},
		{"1tb", 1024 * 1024 * 1024 * 1024, false},
		{" 64 kb ", 64 * 1024, false},
		{"0", 0, false},
		{"-1k", -1024, false},
		{"", 0, true},
		{"invalid", 0, true},
		{"1xb", 0, true},
	}

	for _, tt := range tests {
		got, err := ParseByteSize(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseByteSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.expected {
			t.Errorf("ParseByteSize(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestFromCtyByteSize(t *testing.T) {
	tests := []struct {
		input    cty.Value
		expected int
		wantErr  bool
	}{
		{cty.NumberIntVal(1024), 1024, false},
		{cty.StringVal("1kb"), 1024, false},
		{cty.StringVal("64kb"), 64 * 1024, false},
		{cty.NullVal(cty.Number), 0, false},
		{cty.UnknownVal(cty.Number), 0, false},
		{cty.StringVal("invalid"), 0, true},
		{cty.BoolVal(true), 0, true},
	}

	for _, tt := range tests {
		got, err := FromCtyByteSize(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("FromCtyByteSize(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.expected {
			t.Errorf("FromCtyByteSize(%v) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}
