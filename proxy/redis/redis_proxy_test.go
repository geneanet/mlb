package redis

import (
	"context"
	"mlb/backend"
	"mlb/module"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/zclconf/go-cty/cty"
)

func TestRedisProxyConfig(t *testing.T) {
	t.Run("Parse valid config", func(t *testing.T) {
		hcl := `
			source = "backends_inventory.static.test"
			addresses = [":6379"]
			preconnect = 5
			idle_timeout = "10m"
			healthcheck = true
		`
		parser := hclparse.NewParser()
		f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
		tc := &module.Config{
			Config: f.Body,
		}
		
		config := parseRedisProxyConfig(tc)
		if config.Preconnect != 5 {
			t.Errorf("expected 5, got %d", config.Preconnect)
		}
		if config.IdleTimeout != "10m" {
			t.Errorf("expected 10m, got %s", config.IdleTimeout)
		}
		if !config.Healthcheck {
			t.Error("expected true")
		}
	})

	t.Run("Validate valid config", func(t *testing.T) {
		hcl := `
			source = "backends_inventory.static.test"
			addresses = [":6379"]
		`
		parser := hclparse.NewParser()
		f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
		tc := &module.Config{
			Config: f.Body,
		}
		
		diags := validateRedisProxyConfig(tc)
		if diags.HasErrors() {
			t.Error("expected no errors")
		}
	})

	t.Run("Validate invalid duration", func(t *testing.T) {
		hcl := `
			source = "backends_inventory.static.test"
			idle_timeout = "invalid"
		`
		parser := hclparse.NewParser()
		f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
		tc := &module.Config{
			Config: f.Body,
		}
		
		diags := validateRedisProxyConfig(tc)
		if !diags.HasErrors() {
			t.Error("expected errors")
		}
	})
}

func TestNewRedisProxy(t *testing.T) {
	hcl := `
		source = "backends_inventory.static.test"
		addresses = [":6379"]
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Config: f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Error("expected p to be not nil")
	}
	
	rp := p.(*RedisProxy)
	if rp.bufferSize != 16384 {
		t.Errorf("expected 16384, got %d", rp.bufferSize)
	}
	if rp.idleTimeout != 5*time.Minute {
		t.Errorf("expected 5m, got %v", rp.idleTimeout)
	}
}

type mockBackendProvider struct {
	provided bool
}

func (m *mockBackendProvider) ProvideUpdates(receiver backend.BackendUpdateSubscriber) {
	m.provided = true
}

func TestRedisProxy_BindAndReceiveUpdate(t *testing.T) {
	hcl := `
		source = "backends_inventory.static.test"
		addresses = ["127.0.0.1:0"] // 0 to pick a random port
	`
	parser := hclparse.NewParser()
	f, _ := parser.ParseHCL([]byte(hcl), "test.hcl")
	tc := &module.Config{
		Category: "proxy",
		Type:     "redis",
		Name:     "test",
		Config:   f.Body,
	}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p, err := newRedisProxy(tc, wg, ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rp := p.(*RedisProxy)

	mockProvider := &mockBackendProvider{}
	registry := make(module.ModulesRegistry)
	registry.AddModule("backends_inventory.static.test", mockProvider)

	err = rp.Bind(registry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockProvider.provided {
		t.Error("expected mockProvider.provided to be true")
	}

	// Test ReceiveUpdate
	be := &backend.Backend{Address: "127.0.0.1:6379", Meta: backend.NewEmptyMetaMap(0)}
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Backend: be,
	})

	// Wait a bit for the async update to process
	time.Sleep(10 * time.Millisecond)
	if !rp.backends.Has(be.Address) {
		t.Errorf("expected backend %s to be present", be.Address)
	}

	// Test Modified
	be.Meta.Set("default", "foo", cty.StringVal("bar"))
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Backend: be,
	})
	time.Sleep(10 * time.Millisecond)
	val, ok := rp.backends.GetSortedList()[0].Meta.Get("default", "foo")
	if !ok {
		t.Error("expected meta value to be present")
	}
	if val.AsString() != "bar" {
		t.Errorf("expected bar, got %s", val.AsString())
	}

	// Test Removed
	rp.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: be.Address,
	})
	time.Sleep(10 * time.Millisecond)
	if rp.backends.Has(be.Address) {
		t.Errorf("expected backend %s to be removed", be.Address)
	}
}
