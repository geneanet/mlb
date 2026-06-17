package backends_processor

import (
	"context"
	"errors"
	"mlb/backend"
	"mlb/misc"
	"mlb/module"
	"mlb/testutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/redis/go-redis/v9"
	"github.com/zclconf/go-cty/cty"
)

type mockRedisClient struct {
	roleFunc func(ctx context.Context, args ...interface{}) *redis.Cmd
	infoFunc func(ctx context.Context, sections ...string) *redis.StringCmd
}

func (m *mockRedisClient) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	if m.roleFunc != nil && args[0] == "ROLE" {
		return m.roleFunc(ctx, args...)
	}
	return redis.NewCmd(ctx)
}

func (m *mockRedisClient) Info(ctx context.Context, sections ...string) *redis.StringCmd {
	if m.infoFunc != nil {
		return m.infoFunc(ctx, sections...)
	}
	return redis.NewStringCmd(ctx)
}

func (m *mockRedisClient) Close() error {
	return nil
}

func TestRedisCheckerConfig(t *testing.T) {
	factory := &RedisCheckerFactory{}
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

	config := factory.parseConfig(tc)
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

func TestRedisCheck_Mock(t *testing.T) {
	b := &backend.Backend{
		Address: "127.0.0.1:6379",
		Meta:    backend.NewEmptyMetaMap(0),
	}
	statusChan := make(chan *backend.Backend, 10)

	runTest := func(name string, roleFunc func(context.Context, ...interface{}) *redis.Cmd, infoFunc func(context.Context, ...string) *redis.StringCmd, expectedRole string, expectedReadonly bool) {
		t.Run(name, func(t *testing.T) {
			check := NewRedisCheck(b.Clone(), "", time.Millisecond, time.Millisecond, 1.0, time.Second, time.Second, time.Second, statusChan)
			check.ctx = context.Background()
			check.ticker = misc.NewExponentialBackoffTicker(time.Millisecond, time.Millisecond, 1.0)
			check.client = &mockRedisClient{roleFunc: roleFunc, infoFunc: infoFunc}

			status, role, readonly, err := check.fetchStatus()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if status.AsString() != "ok" {
				t.Errorf("expected status ok, got %v", status)
			}
			if role.AsString() != expectedRole {
				t.Errorf("expected role %s, got %s", expectedRole, role.AsString())
			}
			if readonly.True() != expectedReadonly {
				t.Errorf("expected readonly %v, got %v", expectedReadonly, readonly.True())
			}
		})
	}

	// Master via ROLE
	runTest("MasterROLE", func(ctx context.Context, args ...interface{}) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetVal([]interface{}{"master", int64(0), []interface{}{}})
		return cmd
	}, nil, "master", false)

	// Slave via ROLE
	runTest("SlaveROLE", func(ctx context.Context, args ...interface{}) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetVal([]interface{}{"slave", "127.0.0.1", int64(6379), "connected", int64(0)})
		return cmd
	}, nil, "slave", true)

	// Master via INFO (fallback)
	runTest("MasterINFO", func(ctx context.Context, args ...interface{}) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errors.New("ROLE not implemented"))
		return cmd
	}, func(ctx context.Context, sections ...string) *redis.StringCmd {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetVal("# Replication\r\nrole:master\r\n")
		return cmd
	}, "master", false)

	// Slave via INFO (fallback)
	runTest("SlaveINFO", func(ctx context.Context, args ...interface{}) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errors.New("ROLE not implemented"))
		return cmd
	}, func(ctx context.Context, sections ...string) *redis.StringCmd {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetVal("# Replication\r\nrole:slave\r\n")
		return cmd
	}, "slave", true)

	// Unknown via INFO (fallback)
	runTest("UnknownINFO", func(ctx context.Context, args ...interface{}) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(errors.New("ROLE not implemented"))
		return cmd
	}, func(ctx context.Context, sections ...string) *redis.StringCmd {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetVal("# Replication\r\nrole:sentinel\r\n")
		return cmd
	}, "unknown", false)
}

func TestRedisChecker_ValidateConfig(t *testing.T) {
	factory := &RedisCheckerFactory{}
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
		Ctx:    nil,
	}

	diags := factory.ValidateConfig(tc)
	if !diags.HasErrors() {
		t.Error("expected diagnostics to have errors for invalid period")
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

	// Error cases in fetchStatus (panic/recovery)
	t.Run("RecoveryPanic", func(t *testing.T) {
		check := NewRedisCheck(b.Clone(), "", time.Millisecond, time.Millisecond, 1.0, time.Second, time.Second, time.Second, make(chan *backend.Backend, 1))
		check.ctx = context.Background()
		check.ticker = misc.NewExponentialBackoffTicker(time.Millisecond, time.Millisecond, 1.0)
		check.client = &mockRedisClient{roleFunc: func(ctx context.Context, args ...interface{}) *redis.Cmd {
			panic("redis panic")
		}}
		status, _, _, err := check.fetchStatus()
		if status.AsString() != "err" {
			t.Errorf("expected status err, got %v", status)
		}
		if err == nil {
			t.Error("expected error, got nil")
		}
	})

	// Fallback INFO error
	t.Run("InfoError", func(t *testing.T) {
		check := NewRedisCheck(b.Clone(), "", time.Millisecond, time.Millisecond, 1.0, time.Second, time.Second, time.Second, make(chan *backend.Backend, 1))
		check.ctx = context.Background()
		check.ticker = misc.NewExponentialBackoffTicker(time.Millisecond, time.Millisecond, 1.0)
		check.client = &mockRedisClient{
			roleFunc: func(ctx context.Context, args ...interface{}) *redis.Cmd {
				cmd := redis.NewCmd(ctx)
				cmd.SetErr(errors.New("ROLE error"))
				return cmd
			},
			infoFunc: func(ctx context.Context, sections ...string) *redis.StringCmd {
				panic(errors.New("INFO panic"))
			},
		}
		status, _, _, err := check.fetchStatus()
		if status.AsString() != "err" {
			t.Errorf("expected status err, got %v", status)
		}
		if err == nil || !strings.Contains(err.Error(), "INFO panic") {
			t.Errorf("expected INFO panic error, got %v", err)
		}
	})

	// Unexpected ROLE format
	t.Run("RoleFormatError", func(t *testing.T) {
		check := NewRedisCheck(b.Clone(), "", time.Millisecond, time.Millisecond, 1.0, time.Second, time.Second, time.Second, make(chan *backend.Backend, 1))
		check.ctx = context.Background()
		check.ticker = misc.NewExponentialBackoffTicker(time.Millisecond, time.Millisecond, 1.0)
		check.client = &mockRedisClient{
			roleFunc: func(ctx context.Context, args ...interface{}) *redis.Cmd {
				cmd := redis.NewCmd(ctx)
				cmd.SetVal("not an array")
				return cmd
			},
		}
		status, _, _, err := check.fetchStatus()
		if status.AsString() != "err" {
			t.Errorf("expected status err, got %v", status)
		}
		if err == nil || !strings.Contains(err.Error(), "unexpected ROLE result format") {
			t.Errorf("expected format error, got %v", err)
		}
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
