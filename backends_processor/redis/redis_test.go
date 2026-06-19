package redis

import (
	"context"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
)

func TestRedisCheckerConfig(t *testing.T) {
	hclBlock := testutil.ParseHCL(t, `
		backends_processor "redis" "test" {
			source = "inventory.static.test"
			period = "500ms"
			max_period = "2s"
			connect_timeout = "2s"
		}
	`)
	ctx := &hcl.EvalContext{}
	config, diags := module.DecodeConfigBlock(hclBlock, ctx, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("unexpected errors: %s", diags.Error())
	}
	if config.Type != "redis" {
		t.Errorf("expected type redis, got %s", config.Type)
	}
}

func TestRedisChecker_ValidateConfig(t *testing.T) {
	hclBlock := testutil.ParseHCL(t, `
		backends_processor "redis" "test" {
			source = "inventory.static.test"
			period = "invalid"
		}
	`)
	ctx := &hcl.EvalContext{}
	config, _ := module.DecodeConfigBlock(hclBlock, ctx, "backends_processor")
	diags := module.ValidateConfig(config, "backends_processor")
	if !diags.HasErrors() {
		t.Error("expected error for invalid period")
	}

	hclBlock = testutil.ParseHCL(t, `
		backends_processor "redis" "test" {
			source = "inventory.static.test"
			max_period = "invalid"
		}
	`)
	config, _ = module.DecodeConfigBlock(hclBlock, ctx, "backends_processor")
	diags = module.ValidateConfig(config, "backends_processor")
	if !diags.HasErrors() {
		t.Error("expected error for invalid max_period")
	}

	hclBlock = testutil.ParseHCL(t, `
		backends_processor "redis" "test" {
			source = "inventory.static.test"
			connect_timeout = "invalid"
		}
	`)
	config, _ = module.DecodeConfigBlock(hclBlock, ctx, "backends_processor")
	diags = module.ValidateConfig(config, "backends_processor")
	if !diags.HasErrors() {
		t.Error("expected error for invalid connect_timeout")
	}
}

type mockSubscriber struct{}

func (s *mockSubscriber) ReceiveUpdate(upd backend.BackendUpdate) {}

func TestRedisChecker_Integration(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("REDIS_ADDR not set")
	}

	src := `
backends_processor "redis" "test" {
	source = "foo"
	period = "100ms"
}
`
	hclBlock := testutil.ParseHCL(t, src)
	ctx := &hcl.EvalContext{}
	config, _ := module.DecodeConfigBlock(hclBlock, ctx, "backends_processor")

	wg := &sync.WaitGroup{}
	bgCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisChecker := module.New(config, wg, bgCtx, "backends_processor").(*RedisChecker)

	// Add backend
	b := &backend.Backend{
		Address: redisAddr,
		Meta:    backend.NewEmptyMetaMap(0),
	}
	redisChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: redisAddr,
		Backend: b,
	})

	// Wait for check
	time.Sleep(200 * time.Millisecond)

	list := redisChecker.GetBackendList()
	if len(list) != 1 {
		t.Errorf("expected 1 backend, got %d", len(list))
	}

	// Modify backend
	redisChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Address: redisAddr,
		Backend: b,
	})

	// GetBackendList & ProvideUpdates
	redisChecker.GetBackendList()
	redisChecker.ProvideUpdates(&mockSubscriber{})

	// Remove backend
	redisChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: redisAddr,
	})

	// Lifecycle tests
	check := NewRedisCheck(b.Clone(), "", time.Millisecond, time.Millisecond, 1.0, time.Second, time.Second, time.Second, make(chan *backend.Backend, 1))
	check.StartPolling()
	check.StartPolling() // already running
	time.Sleep(10 * time.Millisecond)
	check.StopPolling()
	check.StopPolling() // already stopped

	cancel()
	wg.Wait()
}

func TestRedisChecker_ModuleMethods(t *testing.T) {
	registry := backend.NewRegistry()
	c := &RedisChecker{
		id:       "test-id",
		source:   "test-source",
		backends: registry,
		updChan:  make(chan backend.BackendUpdate, 1),
	}

	if len(c.GetBackendList()) != 0 {
		t.Errorf("expected empty backend list")
	}

	prov := &testutil.DummyProvider{Backends: registry}
	prov.ProvideUpdates(c)

	modules := make(module.ModulesRegistry)
	provider := &testutil.DummyProvider{ID: "test-source", Backends: registry}
	modules.AddModule("test-source", provider)
	c.Bind(modules)
}
