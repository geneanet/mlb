package config

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Capture stdout to prevent RenderConfigDiag from polluting the test output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	go io.Copy(io.Discard, r)
	defer func() {
		os.Stdout = oldStdout
		w.Close()
	}()

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
	bind = "127.0.0.1:80"
}
proxy "redis" "prox2" {
	bind = "127.0.0.1:6379"
}
metrics {
	address = "127.0.0.1:9090"
}
system {
	rlimit_nofile = 1024
}
`
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "config.hcl")
		err := os.WriteFile(filePath, []byte(validConfigContent), 0644)
		if err != nil {
			t.Fatalf("Failed to write config file: %v", err)
		}

		cfg, _ := LoadConfig(filePath)
		if cfg == nil {
			t.Errorf("Expected config to not be nil")
		}
	})

	t.Run("Invalid config missing file", func(t *testing.T) {
		cfg, diags := LoadConfig("non_existent_file.hcl")
		if !diags.HasErrors() {
			t.Errorf("Expected errors for missing file")
		}
		if cfg == nil {
			t.Errorf("Expected config structure to be initialized even on error")
		}
	})

	t.Run("Invalid blocks to trigger nil configs", func(t *testing.T) {
		invalidConfigContent := `
backends_inventory "unknown" "inv1" {
}
backends_processor "unknown" "proc1" {
}
balancer "unknown" "bal1" {
}
proxy "unknown" "prox1" {
}
metrics {
	invalid = true
}
system {
	invalid = true
}
`
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "invalid_config.hcl")
		err := os.WriteFile(filePath, []byte(invalidConfigContent), 0644)
		if err != nil {
			t.Fatalf("Failed to write config error file: %v", err)
		}

		_, diags := LoadConfig(filePath)
		if !diags.HasErrors() {
			t.Errorf("Expected errors for invalid config")
		}
	})
}
