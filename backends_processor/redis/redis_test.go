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

	mod, err := module.New(config, wg, bgCtx, "backends_processor")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	redisChecker := mod.(*RedisChecker)

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

func TestParseResponse(t *testing.T) {
	t.Run("parseRoleResponse", func(t *testing.T) {
		// Master with 2 slaves
		roleMaster := []interface{}{
			"master",
			int64(12345),
			[]interface{}{
				[]interface{}{"127.0.0.1", "6380", "12345"},
				[]interface{}{"127.0.0.1", "6381", "12345"},
			},
		}
		role, readonly, slaves, err := parseRoleResponse(roleMaster)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if role.AsString() != "master" {
			t.Errorf("expected master, got %s", role.AsString())
		}
		if readonly.True() {
			t.Error("expected readonly false")
		}
		s, _ := slaves.AsBigFloat().Int64()
		if s != 2 {
			t.Errorf("expected 2 slaves, got %d", s)
		}

		// Slave
		roleSlave := []interface{}{
			"slave",
			"127.0.0.1",
			int64(6379),
			"connected",
			int64(12345),
		}
		role, readonly, slaves, err = parseRoleResponse(roleSlave)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if role.AsString() != "slave" {
			t.Errorf("expected slave, got %s", role.AsString())
		}
		if !readonly.True() {
			t.Error("expected readonly true")
		}
		s, _ = slaves.AsBigFloat().Int64()
		if s != 0 {
			t.Errorf("expected 0 slaves, got %d", s)
		}
	})

	t.Run("parseInfoResponse", func(t *testing.T) {
		// Master with 1 slave
		infoMaster := "# Replication\nrole:master\nconnected_slaves:1\nslave0:ip=127.0.0.1,port=6380,state=online,offset=123,lag=0\n"
		role, readonly, slaves := parseInfoResponse(infoMaster)
		if role.AsString() != "master" {
			t.Errorf("expected master, got %s", role.AsString())
		}
		if readonly.True() {
			t.Error("expected readonly false")
		}
		s, _ := slaves.AsBigFloat().Int64()
		if s != 1 {
			t.Errorf("expected 1 slave, got %d", s)
		}

		// Slave
		infoSlave := "# Replication\nrole:slave\nmaster_host:127.0.0.1\nmaster_port:6379\nmaster_link_status:up\nconnected_slaves:0\n"
		role, readonly, slaves = parseInfoResponse(infoSlave)
		if role.AsString() != "slave" {
			t.Errorf("expected slave, got %s", role.AsString())
		}
		if !readonly.True() {
			t.Error("expected readonly true")
		}
		s, _ = slaves.AsBigFloat().Int64()
		if s != 0 {
			t.Errorf("expected 0 slaves, got %d", s)
		}
	})
}
