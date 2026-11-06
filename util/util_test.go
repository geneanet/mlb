package util

import (
	"fmt"
	"math"
	"testing"

	"github.com/zclconf/go-cty/cty"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
		wantErr  bool
	}{
		{"zero", "0", 0, false},
		{"positive", "12345", 12345, false},
		{"negative", "-12345", -12345, false},
		{"maxInt", fmt.Sprintf("%d", math.MaxInt), math.MaxInt, false},
		{"minInt", fmt.Sprintf("%d", math.MinInt), math.MinInt, false},
		{"empty", "", 0, true},
		{"justDash", "-", 0, true},
		{"invalidChars", "123a45", 0, true},
		{"spaces", " 123 ", 0, true},
		{"overflowPositive", "9999999999999999999999999999999999999999", 0, true},
		{"overflowPositiveNext", fmt.Sprintf("%d0", math.MaxInt), 0, true},
		{"overflowNegative", "-9999999999999999999999999999999999999999", 0, true},
		{"overflowNegativeNext", fmt.Sprintf("%d0", math.MinInt), 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSize([]byte(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSize(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("ParseSize(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

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
		if !tt.wantErr && got != tt.expected {
			t.Errorf("FromCtyByteSize(%v) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestRedactHCL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name: "mysql_password",
			input: `backends_processor "mysql" "main" {
		user     = "root"
		password = "secret_password"
		}`,
			expected: `backends_processor "mysql" "main" {
		user     = "root"
		password = "******"
		}`,
		},
		{
			name: "redis_password_no_space",
			input: `backends_processor "redis" "main" {
		password="mypassword"
		}`,
			expected: `backends_processor "redis" "main" {
		password="******"
		}`,
		},
		{
			name: "consul_token_with_comment",
			input: `backends_inventory "consul" "dc1" {
		token = "consul-token-xyz" # secret token
		}`,
			expected: `backends_inventory "consul" "dc1" {
		token = "******" # secret token
		}`,
		},
		{
			name: "consul_key_not_redacted",
			input: `backends_processor "consul_kv" "kv" {
		consul_key = "services/app/config"
		}`,
			expected: `backends_processor "consul_kv" "kv" {
		consul_key = "services/app/config"
		}`,
		},
		{
			name: "comments_not_redacted",
			input: `# password = "commented"
		// token = "commented"
		/* password = "block" */`,
			expected: `# password = "commented"
		// token = "commented"
		/* password = "block" */`,
		},
		{
			name:     "escaped_quotes",
			input:    `password = "foo\"bar"`,
			expected: `password = "******"`,
		},
		{
			name:     "unquoted_expression",
			input:    `password = var.my_secret`,
			expected: `password = "******"`,
		},
		{
			name:     "array_token",
			input:    `token = ["tok1", "tok2"]`,
			expected: `token = "******"`,
		},
		{
			name: "prefixed_attributes",
			input: `client_secret = "secret1"
		auth_token = "tok1"
		api_key = "key1"
		db_password = "pwd1"`,
			expected: `client_secret = "******"
		auth_token = "******"
		api_key = "******"
		db_password = "******"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactHCL(tt.input)
			if got != tt.expected {
				t.Errorf("RedactHCL() =\n%s\nwant:\n%s", got, tt.expected)
			}
		})
	}
}
