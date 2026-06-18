package backends_processor

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
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

func TestRedisCheckerConfig(t *testing.T) {
	hclBlock := parseHCL(t, `
		redis "test" {
			source = "inventory.static.test"
			period = "500ms"
			max_period = "2s"
			connect_timeout = "2s"
		}
	`)

	tc := &module.Config{
		Type:   "redis",
		Name:   "test",
		Config: hclBlock.Body,
		Ctx:    nil,
	}

	config := parseRedisCheckerConfig(tc)
	if config.Source != "inventory.static.test" {
		t.Errorf("expected source inventory.static.test, got %s", config.Source)
	}
	if config.Period != "500ms" {
		t.Errorf("expected period 500ms, got %s", config.Period)
	}
	if config.ConnectTimeout != "2s" {
		t.Errorf("expected connect_timeout 2s, got %s", config.ConnectTimeout)
	}
}

func TestRedisCheck_Integration(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("REDIS_ADDR not set, skipping integration test")
	}

	b := &backend.Backend{
		Address: redisAddr,
		Meta:    backend.NewEmptyMetaMap(0),
	}
	statusChan := make(chan *backend.Backend, 1)

	check := NewRedisCheck(
		b,
		"",
		100*time.Millisecond,
		500*time.Millisecond,
		1.5,
		1*time.Second,
		1*time.Second,
		1*time.Second,
		statusChan,
	)

	err := check.StartPolling()
	if err != nil {
		t.Fatalf("failed to start polling: %v", err)
	}
	defer check.StopPolling()

	// Wait for status update
	select {
	case updatedBackend := <-statusChan:
		status, _ := updatedBackend.Meta.Get("redis", "status")
		if status.AsString() != "ok" {
			t.Errorf("expected status ok, got %v", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for status update")
	}
}

func TestRedisCheck_Parsing(t *testing.T) {
	// Master via ROLE
	role, readonly, err := parseRoleResponse([]interface{}{"master", int64(0), []interface{}{}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if role.AsString() != "master" {
		t.Errorf("expected master, got %s", role.AsString())
	}
	if readonly.True() {
		t.Error("expected readonly false")
	}

	// Slave via ROLE
	role, readonly, err = parseRoleResponse([]interface{}{"slave", "127.0.0.1", int64(6379), "connected", int64(0)})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if role.AsString() != "slave" {
		t.Errorf("expected slave, got %s", role.AsString())
	}
	if !readonly.True() {
		t.Error("expected readonly true")
	}

	// Master via INFO
	role, readonly = parseInfoResponse("# Replication\r\nrole:master\r\n")
	if role.AsString() != "master" {
		t.Errorf("expected master, got %s", role.AsString())
	}
	if readonly.True() {
		t.Error("expected readonly false")
	}

	// Slave via INFO
	role, readonly = parseInfoResponse("# Replication\r\nrole:slave\r\n")
	if role.AsString() != "slave" {
		t.Errorf("expected slave, got %s", role.AsString())
	}
	if !readonly.True() {
		t.Error("expected readonly true")
	}

	// Unknown via INFO
	role, readonly = parseInfoResponse("# Replication\r\nrole:sentinel\r\n")
	if role.AsString() != "unknown" {
		t.Errorf("expected unknown, got %s", role.AsString())
	}
	if readonly.True() {
		t.Error("expected readonly false")
	}

	// Format Error ROLE
	_, _, err = parseRoleResponse("not an array")
	if err == nil {
		t.Error("expected error for invalid ROLE format")
	}
}

func TestRedisChecker_ValidateConfig(t *testing.T) {
	hclBlock := parseHCL(t, `
		redis "test" {
			source = "inventory.static.test"
			period = "invalid"
		}
	`)

	tc := &module.Config{
		Type:   "redis",
		Name:   "test",
		Config: hclBlock.Body,
		Ctx:    &hcl.EvalContext{},
	}

	diags := validateRedisCheckerConfig(tc)
	if !diags.HasErrors() {
		t.Error("expected errors for invalid duration")
	}
}

func TestRedis_Coverage(t *testing.T) {
	factory := module.GetFactory("backends_processor", "redis")

	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.LiteralValueExpr{Val: cty.StringVal("test_cov")}},
		},
	}
	config := &module.Config{Name: "test_cov", Type: "redis", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	mod := factory.New(config, wg, ctx)
	redisChecker := mod.(*RedisChecker)

	// Add backend
	b := &backend.Backend{Address: "127.0.0.1:6379", Meta: backend.NewEmptyMetaMap(0)}
	redisChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:6379",
		Backend: b,
	})

	testutil.Eventually(t, func() bool {
		redisChecker.checksMtex.RLock()
		defer redisChecker.checksMtex.RUnlock()
		_, ok := redisChecker.checks["127.0.0.1:6379"]
		return ok
	}, 1*time.Second, 10*time.Millisecond)

	// Modified backend
	redisChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Address: "127.0.0.1:6379",
		Backend: b,
	})

	// GetBackendList & ProvideUpdates
	redisChecker.GetBackendList()
	redisChecker.ProvideUpdates(&mockSubscriber{})
	redisChecker.GetID()

	// Remove backend
	redisChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "127.0.0.1:6379",
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

	if c.GetID() != "test-id" {
		t.Errorf("expected test-id, got %s", c.GetID())
	}
	if len(c.GetBackendList()) != 0 {
		t.Errorf("expected empty backend list")
	}

	prov := &dummyProvider{backends: registry}
	prov.ProvideUpdates(c)

	modules := module.NewModulesRegistry()
	provider := &dummyProvider{id: "test-source", backends: registry}
	modules.AddModule(provider)

	c.Bind(modules)
}
