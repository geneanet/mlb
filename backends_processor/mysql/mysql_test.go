package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/rs/zerolog"
	"github.com/zclconf/go-cty/cty"
)

type mockDriver struct{}

func (d *mockDriver) Open(name string) (driver.Conn, error) {
	if name == "error" || strings.Contains(name, "error") {
		return nil, errors.New("open error")
	}
	return &mockConn{name: name}, nil
}

type mockConn struct {
	name string
}

func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return &mockStmt{query: query, conn: c}, nil
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) Begin() (driver.Tx, error) {
	return nil, nil
}

type mockStmt struct {
	query string
	conn  *mockConn
}

func (s *mockStmt) Close() error {
	return nil
}

func (s *mockStmt) NumInput() int {
	return 0
}

func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	switch s.query {
	case "SELECT @@read_only":
		if s.conn.name == "panic_readonly" {
			panic("panic readonly")
		}
		if s.conn.name == "err_readonly" {
			return nil, errors.New("err readonly")
		}
		return &mockRows{
			columns: []string{"@@read_only"},
			data:    [][]driver.Value{{int64(0)}},
		}, nil
	case "SHOW REPLICA STATUS":
		if s.conn.name == "panic_replica" {
			panic("panic replica")
		}
		if s.conn.name == "empty_replica" {
			return &mockRows{
				columns: []string{"Seconds_Behind_Source"},
				data:    [][]driver.Value{},
			}, nil
		}
		if s.conn.name == "null_replica" {
			return &mockRows{
				columns: []string{"Seconds_Behind_Source"},
				data:    [][]driver.Value{{nil}},
			}, nil
		}
		if s.conn.name == "no_sbs_replica" {
			return &mockRows{
				columns: []string{"Other"},
				data:    [][]driver.Value{{1}},
			}, nil
		}
		if s.conn.name == "columns_err_replica" {
			return &mockRows{
				columns: []string{"error"},
			}, nil
		}
		if s.conn.name == "err_replica" {
			return nil, errors.New("err replica")
		}
		if s.conn.name == "fallback_slave" {
			return nil, errors.New("Error 1064 (42000): You have an error in your SQL syntax near 'REPLICA STATUS'")
		}
		return &mockRows{
			columns: []string{"Seconds_Behind_Source", "DummyColumn"},
			data:    [][]driver.Value{{int64(5), "dummy"}},
		}, nil
	case "SHOW SLAVE STATUS":
		if s.conn.name == "fallback_slave" {
			return &mockRows{
				columns: []string{"Seconds_Behind_Master", "DummyColumn"},
				data:    [][]driver.Value{{int64(42), "dummy"}},
			}, nil
		}
	}
	return nil, errors.New("unknown query")
}

type mockRows struct {
	columns []string
	data    [][]driver.Value
	pos     int
}

func (r *mockRows) Columns() []string {
	if len(r.columns) == 1 && r.columns[0] == "error" {
		panic(errors.New("columns error"))
	}
	return r.columns
}

func (r *mockRows) Close() error {
	return nil
}

func (r *mockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

func init() {
	sql.Register("mysql_mock", &mockDriver{})
}

type mockSubscriber struct{}

func (m *mockSubscriber) ReceiveUpdate(upd backend.BackendUpdate) {}

// TestMySQL verifies the initialization, configuration validation, and backend update handling
// for the MySQL checker. It also tests various error and panic scenarios during health checks.
func TestMySQL(t *testing.T) {
	setMySQLDriverName("mysql_mock")

	// Create a mock config
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source":          {Name: "source", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("test")}}}},
			"user":            {Name: "user", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("user")}}}},
			"password":        {Name: "password", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("pwd")}}}},
			"period":          {Name: "period", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("50ms")}}}},
			"max_period":      {Name: "max_period", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("50ms")}}}},
			"connect_timeout": {Name: "connect_timeout", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("50ms")}}}},
			"read_timeout":    {Name: "read_timeout", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("50ms")}}}},
			"write_timeout":   {Name: "write_timeout", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("50ms")}}}},
			"check_replica":   {Name: "check_replica", Expr: &hclsyntax.LiteralValueExpr{Val: cty.BoolVal(true)}},
		},
	}

	config := &module.Config{
		Category: "backends_processor",
		Name:     "test",
		Type:     "mysql",
		Config:   body,
		Ctx:      &hcl.EvalContext{},
	}

	diags := module.ValidateConfig(config, "backends_processor")
	if diags.HasErrors() {
		t.Fatal(diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	mod, err := newMySQLChecker(config, wg, ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	mysqlChecker := mod.(*MySQLChecker)

	mysqlChecker.GetBackendList()

	dp := &testutil.DummyProvider{ID: "test", Backends: backend.NewRegistry(zerolog.Nop(), false)}
	modules := make(module.ModulesRegistry)
	modules.AddModule("test", dp)
	_ = mysqlChecker.Bind(modules)

	// Test Ready functionality: should be ready after dp is ready and first check is done
	dp.Backends.MarkReady()
	// Add backend
	b := backend.NewBackend("127.0.0.1:3306", nil)
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:3306",
		Backend: b,
	})

	select {
	case <-mysqlChecker.Ready():
		// OK
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Timeout waiting for mysql_checker readiness")
	}

	subscriber := &mockSubscriber{}
	mysqlChecker.ProvideUpdates(subscriber)

	testutil.Eventually(t, func() bool {
		list := mysqlChecker.GetBackendList()
		return len(list) == 1 && list[0].Address == "127.0.0.1:3306"
	}, 1*time.Second, 10*time.Millisecond)

	// Modified backend
	b.Meta.Set("test", "test", cty.StringVal("test"))
	// Set a custom value in the mysql bucket to verify it's preserved
	// We use "test_preserve" instead of "status" to avoid race conditions with the running checker
	list := mysqlChecker.GetBackendList()
	if len(list) > 0 {
		list[0].Meta.Set("mysql", "test_preserve", cty.StringVal("preserved"))
	}

	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Address: "127.0.0.1:3306",
		Backend: b,
	})

	testutil.Eventually(t, func() bool {
		list := mysqlChecker.GetBackendList()
		if len(list) == 0 {
			return false
		}
		val, ok := list[0].Meta.Get("test", "test")
		if !ok || val.AsString() != "test" {
			return false
		}
		val, ok = list[0].Meta.Get("mysql", "test_preserve")
		return ok && val.AsString() == "preserved"
	}, 1*time.Second, 10*time.Millisecond)

	// Remove backend
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "127.0.0.1:3306",
	})

	testutil.Eventually(t, func() bool {
		return len(mysqlChecker.GetBackendList()) == 0
	}, 1*time.Second, 10*time.Millisecond)

	// Add backend again to verify stopChecks on shutdown
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:3306",
		Backend: b,
	})
	testutil.Eventually(t, func() bool {
		return len(mysqlChecker.GetBackendList()) == 1
	}, 1*time.Second, 10*time.Millisecond)

	// Test directly some checks to cover panic cases and fetch logic
	runTestCheck := func(name string) {
		c := NewMySQLCheck(b, name, time.Millisecond, time.Millisecond, 1.0, time.Millisecond, time.Millisecond, 1.0, 1, time.Minute*5, make(chan *backend.Backend, 1), true)
		c.db, _ = sql.Open("mysql_mock", name)
		defer c.db.Close()
		_, _, _, _ = c.fetchStatus()
	}

	runTestCheck("panic_readonly")
	runTestCheck("panic_replica")
	runTestCheck("empty_replica")
	runTestCheck("null_replica")
	runTestCheck("no_sbs_replica")
	runTestCheck("columns_err_replica")
	runTestCheck("err_readonly")
	runTestCheck("err_replica")

	lifecycleCheck := NewMySQLCheck(b, "error", time.Millisecond, time.Millisecond, 1.0, time.Millisecond, time.Millisecond, 1.0, 1, time.Minute*5, make(chan *backend.Backend, 1), true)
	setMySQLDriverName("invalid_driver")
	_ = lifecycleCheck.StartPolling() // Error opening db
	setMySQLDriverName("mysql_mock")
	lifecycleCheck.StopPolling()  // Try to stop not running
	lifecycleCheck.running = true // Force running to true
	lifecycleCheck.db, _ = sql.Open("mysql_mock", "error")
	lifecycleCheck.StopPolling()

	lifecycleCheck.running = true
	lifecycleCheck.StopPolling() // Hits the closed channel case

	cancel()
	wg.Wait()
}

// TestMySQL_Coverage performs exhaustive testing of edge cases in the MySQL checker,
// including backoff logic, connection errors, and metadata update scenarios.
func TestMySQL_Coverage(t *testing.T) {
	setMySQLDriverName("mysql_mock")

	// 1. Defaults parsing in module.Config
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("test_cov")}}}},
		},
	}
	config := &module.Config{Category: "backends_processor", Name: "test_cov", Type: "mysql", Config: body, Ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	mod, err := module.New(config, wg, ctx, "backends_processor")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	mysqlChecker := mod.(*MySQLChecker)

	// 2. Add an item directly to cover loop execution in GetBackendList, ProvideUpdates, and stopChecks
	b := backend.NewBackend("127.0.0.1:3307", nil)
	statusChan := make(chan *backend.Backend, 100)
	// Drain status channel asynchronously to prevent deadlocks during the test
	go func() {
		for range statusChan {
		}
	}()
	check := NewMySQLCheck(b, "ok", 1*time.Millisecond, 10*time.Millisecond, 2.0, 1*time.Millisecond, 10*time.Millisecond, 2.0, 1, time.Minute*5, statusChan, true)
	check.db, _ = sql.Open("mysql_mock", "ok")

	mysqlChecker.GetBackendList()
	mysqlChecker.ProvideUpdates(&mockSubscriber{})
	// Give a bit of time for goroutines to run
	time.Sleep(2 * time.Millisecond)

	// 3. StartPolling / StopPolling early returns logic (c.running = true/false)
	_ = check.StartPolling()
	_ = check.StartPolling()
	check.StopPolling()
	check.StopPolling()
	testutil.Eventually(t, func() bool {
		check.runningMu.Lock()
		defer check.runningMu.Unlock()
		return !check.running
	}, 1*time.Second, 1*time.Millisecond)

	// 4. UpdateStatus specific log branches: Known status changed
	check.updateStatus()
	check.backend.Meta.Set("mysql", "status", cty.StringVal("err"))
	check.backend.Meta.Set("mysql", "readonly", cty.BoolVal(true))
	check.backend.Meta.Set("mysql", "replica_latency", cty.NumberIntVal(10))
	check.updateStatus() // Update known values with differing state

	check.dsn = "err_replica"
	check.db, _ = sql.Open("mysql_mock", "err_replica")
	check.updateStatus() // Triggers err != nil

	// 5. Panic recovery and repeated fetch failures.
	check.dsn = "error" // sql.Open("mysql_mock", "error") evaluates to an error mock
	check.db, _ = sql.Open("mysql_mock", "panic_readonly")
	_, _, _, _ = check.fetchStatus()

	// A successful cycle resets the backoff.
	check.dsn = "ok"
	check.db, _ = sql.Open("mysql_mock", "ok")
	check.backoff.Get()
	check.updateStatus()

	// Repeated successful fetches remain healthy.
	_, _, _, _ = check.fetchStatus()

	// Hit checkReplica == false in fetchStatus
	check.checkReplica = false
	_, _, _, _ = check.fetchStatus()
	check.checkReplica = true

	// Hit c.db == nil inside recover block of fetchStatus
	check.db = nil
	check.dsn = "panic_readonly"
	_, _, _, _ = check.fetchStatus()

	// Repeated failures do not require reopening the pool.
	check.dsn = "panic_readonly"
	check.db, _ = sql.Open("mysql_mock", "panic_readonly")
	// Advance the backoff to its maximum.
	for i := 0; i < 10; i++ {
		check.backoff.Get()
	}
	_, _, _, _ = check.fetchStatus()

	// 6. Unknown backend removed
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "unknown_address",
	})

	// 7. Add a backend that fails StartPolling due to sql.Open failing
	setMySQLDriverName("invalid_driver")
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "error_address",
		Backend: backend.NewBackend("error_address", nil),
	})
	// Small sleep to ensure the goroutine picks up the update while mysqlDriverName is still invalid.
	time.Sleep(20 * time.Millisecond)
	setMySQLDriverName("mysql_mock")

	// Wait for background go routine to process everything and hit stopChecks
	cancel()
	wg.Wait()

	// 8. Hit c.updChanStop in ReceiveUpdate
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "unknown_address",
	})
}

// TestMySQL_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestMySQL_ParseConfigError(t *testing.T) {
	// Invalid config (source should be a string, not a list)
	src := `
backends_processor "mysql" "test" {
	source = ["a"]
	user = "foo"
}
`
	block := testutil.ParseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &module.Config{
		Category: "backends_processor",
		Type:     "mysql",
		Name:     "test",
		Config:   block.Body,
		Ctx:      ctx,
	}

	// This will trigger log.Error() and still return a config object
	config := parseMySQLCheckerConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}

func TestMySQL_FetchReplicaLatency_FallbackToSlaveStatus(t *testing.T) {
	setMySQLDriverName("mysql_mock")
	b := backend.NewBackend("127.0.0.1:3306", nil)
	c := NewMySQLCheck(b, "fallback_slave", time.Second, time.Second, 1.0, time.Millisecond, time.Millisecond, 1.0, 1, time.Minute*5, make(chan *backend.Backend, 1), true)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	defer c.cancel()
	var err error
	c.db, err = sql.Open("mysql_mock", "fallback_slave")
	if err != nil {
		t.Fatal(err)
	}

	defer c.db.Close()
	val, err := c.fetchReplicaLatency()
	if err != nil {
		t.Fatalf("expected nil error on fallback, got %v", err)
	}
	bf := val.AsBigFloat()
	intVal, _ := bf.Int64()
	if intVal != 42 {
		t.Fatalf("expected replica_latency 42, got %d", intVal)
	}
}

type checkDriver struct {
	query func(context.Context, string) (driver.Rows, error)
}

func (d *checkDriver) Open(string) (driver.Conn, error) {
	return &checkConn{query: d.query}, nil
}

func (d *checkDriver) Connect(context.Context) (driver.Conn, error) {
	return d.Open("")
}

func (d *checkDriver) Driver() driver.Driver { return d }

type checkConn struct {
	mockConn
	query func(context.Context, string) (driver.Rows, error)
}

func (c *checkConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.query(ctx, query)
}

func newTestCheck(t *testing.T, d *checkDriver) *MySQLCheck {
	t.Helper()
	c := NewMySQLCheck(backend.NewBackend("test:3306", nil), "", time.Second, 4*time.Second, 2,
		time.Millisecond, time.Millisecond, 1, 1, time.Minute, make(chan *backend.Backend, 20), true)
	c.db = sql.OpenDB(d)
	c.db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = c.db.Close() })
	return c
}

func TestMySQLFailureThresholdConfig(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "1", "3"} {
		t.Run("threshold="+value, func(t *testing.T) {
			attribute := ""
			if value != "" {
				attribute = "failure_threshold = " + value
			}
			block := testutil.ParseHCL(t, fmt.Sprintf(`
	backends_processor "mysql" "test" {
	  source = "inventory"
	  %s
	}`, attribute))
			cfg := &module.Config{Category: "backends_processor", Type: "mysql", Name: "test", Config: block.Body}
			diags := validateMySQLCheckerConfig(cfg)
			wantError := value == "0" || value == "-1"
			if diags.HasErrors() != wantError {
				t.Fatalf("validation errors = %v, want %v: %s", diags.HasErrors(), wantError, diags.Error())
			}
			if wantError {
				if _, err := newMySQLChecker(cfg, &sync.WaitGroup{}, context.Background()); err == nil {
					t.Fatal("constructor accepted invalid failure_threshold")
				}
			} else {
				want := 1
				if value == "3" {
					want = 3
				}
				if got := *parseMySQLCheckerConfig(cfg).FailureThreshold; got != want {
					t.Fatalf("parsed failure_threshold = %d, want %d", got, want)
				}
			}
		})
	}
}

func TestMySQLReadyAfterRemovingUncheckedBackend(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	d := &checkDriver{query: func(ctx context.Context, _ string) (driver.Rows, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}}
	name := fmt.Sprintf("mysql_initial_%p", d)
	sql.Register(name, d)
	previous := getMySQLDriverName()
	setMySQLDriverName(name)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		setMySQLDriverName(previous)
	})
	block := testutil.ParseHCL(t, `
backends_processor "mysql" "test" {
  source = "inventory"
  period = "10s"
  retry_max_attempts = 1
}`)
	cfg := &module.Config{Category: "backends_processor", Type: "mysql", Name: "test", Config: block.Body}
	mod, err := newMySQLChecker(cfg, &wg, ctx)
	if err != nil {
		t.Fatal(err)
	}
	checker := mod.(*MySQLChecker)
	b := backend.NewBackend("test:3306", nil)
	checker.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b.Address, Backend: b})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial check did not start")
	}
	checker.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdReady})
	select {
	case <-checker.Ready():
		t.Fatal("checker became ready before the initial check completed")
	default:
	}
	checker.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b.Address})
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("removal did not cancel the initial check")
	}
	select {
	case <-checker.Ready():
	case <-time.After(time.Second):
		t.Fatal("checker stayed unready after its last unchecked backend was removed")
	}
	if list := checker.GetBackendList(); len(list) != 0 {
		t.Fatalf("removed backend is still registered: %v", list)
	}
}

func TestMySQLConsecutiveFailures(t *testing.T) {
	for _, threshold := range []int{1, 3} {
		t.Run(fmt.Sprint(threshold), func(t *testing.T) {
			fail := true
			queries := 0
			d := &checkDriver{query: func(_ context.Context, query string) (driver.Rows, error) {
				queries++
				if fail {
					return nil, errors.New("backend unavailable")
				}
				if query == "SELECT @@read_only" {
					return &mockRows{columns: []string{"@@read_only"}, data: [][]driver.Value{{int64(1)}}}, nil
				}
				return &mockRows{columns: []string{"Seconds_Behind_Source"}, data: [][]driver.Value{{int64(5)}}}, nil
			}}
			c := newTestCheck(t, d)
			c.failureThreshold = threshold
			db := c.db
			assertStatus := func(want string) {
				t.Helper()
				got, _ := c.backend.Meta.Get("mysql", "status")
				if !got.RawEquals(cty.StringVal(want)) {
					t.Fatalf("status = %v, want %s", got, want)
				}
			}

			c.updateStatus()
			assertStatus("err")
			if queries != 1 {
				t.Fatalf("failed cycle made %d queries, want one read_only attempt", queries)
			}
			fail = false
			c.updateStatus()
			assertStatus("ok")
			if c.consecutiveFailures != 0 || c.period != c.defaultPeriod {
				t.Fatal("success did not reset failure counter and period")
			}

			fail = true
			for i := 1; i <= threshold; i++ {
				c.updateStatus()
				want := "ok"
				if i == threshold {
					want = "err"
				}
				assertStatus(want)
				wantPeriod := 2 * time.Second
				if i > 1 {
					wantPeriod = 4 * time.Second
				}
				if c.period != wantPeriod {
					t.Fatalf("failure backoff period = %v, want %v", c.period, wantPeriod)
				}
				readonly, _ := c.backend.Meta.Get("mysql", "readonly")
				latency, _ := c.backend.Meta.Get("mysql", "replica_latency")
				running, _ := c.backend.Meta.Get("mysql", "replica_running")
				if !readonly.RawEquals(cty.BoolVal(true)) || !latency.RawEquals(cty.NumberIntVal(5)) || !running.True() {
					t.Fatal("failed reads overwrote valid metadata")
				}
			}
			c.updateStatus()
			assertStatus("err")
			if c.db != db {
				t.Fatal("failed cycle replaced the SQL pool")
			}
			fail = false
			c.updateStatus()
			assertStatus("ok")
			fail = true
			c.updateStatus()
			if c.consecutiveFailures != 1 {
				t.Fatalf("failure counter = %d after recovery, want 1", c.consecutiveFailures)
			}
			c.retryMaxAttempts = 3
			queries = 0
			c.updateStatus()
			if queries != 3 {
				t.Fatalf("legacy retries made %d queries, want 3", queries)
			}
		})
	}
}

func TestMySQLPartialObservationsAndCancellation(t *testing.T) {
	readOnly := false
	failReplica := false
	d := &checkDriver{query: func(_ context.Context, query string) (driver.Rows, error) {
		if query == "SELECT @@read_only" {
			return &mockRows{columns: []string{"@@read_only"}, data: [][]driver.Value{{readOnly}}}, nil
		}
		if failReplica {
			return nil, errors.New("replica status unavailable")
		}
		return &mockRows{columns: []string{"Seconds_Behind_Source"}, data: [][]driver.Value{{int64(5)}}}, nil
	}}
	c := newTestCheck(t, d)
	c.failureThreshold = 3
	c.updateStatus()
	<-c.statusChan
	readOnly, failReplica = true, true
	c.updateStatus()
	select {
	case <-c.statusChan:
	default:
		t.Fatal("confirmed role change was not published")
	}
	status, _ := c.backend.Meta.Get("mysql", "status")
	readonly, _ := c.backend.Meta.Get("mysql", "readonly")
	latency, _ := c.backend.Meta.Get("mysql", "replica_latency")
	if !status.RawEquals(cty.StringVal("ok")) || !readonly.True() || !latency.RawEquals(cty.NumberIntVal(5)) {
		t.Fatal("partial failure masked role change or overwrote replication metadata")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	cancel()
	failures, period := c.consecutiveFailures, c.period
	c.updateStatus()
	if c.consecutiveFailures != failures || c.period != period {
		t.Fatal("shutdown cancellation changed health state")
	}
	select {
	case <-c.statusChan:
		t.Fatal("shutdown cancellation published a health change")
	default:
	}
}

func TestMySQLPollingWaitsAfterCycle(t *testing.T) {
	const period = 40 * time.Millisecond
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan time.Time, 1)
	next := make(chan time.Time, 1)
	var calls atomic.Int64
	d := &checkDriver{query: func(_ context.Context, _ string) (driver.Rows, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-time.After(time.Second):
				return nil, errors.New("test query was not released")
			}
			finished <- time.Now()
		} else {
			select {
			case next <- time.Now():
			default:
			}
		}
		return &mockRows{columns: []string{"@@read_only"}, data: [][]driver.Value{{int64(0)}}}, nil
	}}
	name := fmt.Sprintf("mysql_poll_%p", d)
	sql.Register(name, d)
	previous := getMySQLDriverName()
	setMySQLDriverName(name)
	defer setMySQLDriverName(previous)
	c := NewMySQLCheck(backend.NewBackend("test:3306", nil), "", period, period, 1,
		time.Millisecond, time.Millisecond, 1, 1, time.Minute, make(chan *backend.Backend, 10), false)
	if err := c.StartPolling(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.StopPolling()
		testutil.Eventually(t, func() bool {
			c.runningMu.Lock()
			defer c.runningMu.Unlock()
			return !c.running
		}, time.Second, time.Millisecond)
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first query did not start")
	}
	// Simulate a cycle longer than its polling period without accumulating ticks.
	testutil.Consistently(t, func() bool { return calls.Load() == 1 }, 2*period, time.Millisecond)
	close(release)
	var end time.Time
	select {
	case end = <-finished:
	case <-time.After(time.Second):
		t.Fatal("first query did not finish")
	}
	select {
	case start := <-next:
		if delay := start.Sub(end); delay < period {
			t.Fatalf("next cycle started after %v, want at least %v", delay, period)
		}
	case <-time.After(time.Second):
		t.Fatal("next cycle did not start")
	}
}
