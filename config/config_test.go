// Package config handles the loading and parsing of the MLB configuration file.
package config

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfig verifies the end-to-end loading and parsing of HCL configuration files.
// It covers multiple scenarios:
//  1. Valid and exhaustive config: Ensures all block types (backends, processors, balancers, proxies, metrics, system)
//     are correctly recognized and parsed.
//  2. Missing file: Verifies that an appropriate error is returned when the config file does not exist.
//  3. Invalid blocks: Verifies that unknown or malformed blocks trigger diagnostics errors.
func TestLoadConfig(t *testing.T) {
	// Capture stdout to prevent RenderConfigDiag from polluting the test output during deliberate failure cases.
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	go io.Copy(io.Discard, r)
	defer func() {
		os.Stdout = oldStdout
		w.Close()
	}()

	// Subtest 1: Successful parsing of a comprehensive, valid configuration.
	t.Run("Valid and exhaustive config", func(t *testing.T) {
		validConfigContent := `
backends_inventory "static" "inv1" {
	hosts = ["127.0.0.1:8080"]
}
backends_inventory "static" "inv2" {
	hosts = ["127.0.0.1:8081"]
}
backends_processor "simple_filter" "proc1" {
	source = "foo"
	condition = true
}
balancer "wrr" "bal1" {
	source = "foo"
	weight = 1
}
proxy "tcp" "prox1" {
	source = "foo"
	addresses = ["127.0.0.1:80"]
}
proxy "redis" "prox2" {
	source = "foo"
	addresses = ["127.0.0.1:6379"]
}
metrics {
	address = "127.0.0.1:9090"
}
system {
	rlimit {
		nofile = 1024
	}
}
`
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "config.hcl")
		err := os.WriteFile(filePath, []byte(validConfigContent), 0644)
		if err != nil {
			t.Fatalf("Failed to write test config file: %v", err)
		}

		cfg, diags := LoadConfig(filePath)
		if diags.HasErrors() {
			t.Errorf("Unexpected diagnostics errors: %s", diags.Error())
		}
		if cfg == nil {
			t.Errorf("Expected non-nil config object")
		}
	})

	// Subtest 2: Handling of non-existent configuration file.
	t.Run("Invalid config missing file", func(t *testing.T) {
		cfg, diags := LoadConfig("non_existent_file.hcl")
		if !diags.HasErrors() {
			t.Errorf("Expected errors for missing file, but got none")
		}
		if cfg == nil {
			t.Errorf("Expected config structure to be initialized even on file read error")
		}
	})

	// Subtest 3: Handling of unknown block types or invalid attributes.
	t.Run("Invalid blocks to trigger nil configs", func(t *testing.T) {
		invalidConfigContent := `
backends_inventory "unknown" "inv1" {
}
backends_processor "unknown" "proc1" {
	source = "foo"
}
balancer "unknown" "bal1" {
	source = "foo"
}
proxy "unknown" "prox1" {
	source = "foo"
}
metrics {
	invalid_attr = true
}
system {
	invalid_attr = true
}
`
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "invalid_config.hcl")
		err := os.WriteFile(filePath, []byte(invalidConfigContent), 0644)
		if err != nil {
			t.Fatalf("Failed to write invalid test config file: %v", err)
		}

		_, diags := LoadConfig(filePath)
		if !diags.HasErrors() {
			t.Errorf("Expected diagnostics errors for unknown block types, but got none")
		}
	})
}
