package backends_processor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"mlb/backend"
	"mlb/misc"
	"mlb/module"
	"mlb/testutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
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
	if s.query == "SELECT @@read_only" {
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
	} else if s.query == "SHOW REPLICA STATUS" {
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
		if s.conn.name == "err_replica" {
			return nil, errors.New("err replica")
		}
		return &mockRows{
			columns: []string{"Seconds_Behind_Source", "DummyColumn"},
			data:    [][]driver.Value{{int64(5), "dummy"}},
		}, nil
	}
	return nil, errors.New("unknown query")
}

type mockRows struct {
	columns []string
	data    [][]driver.Value
	pos     int
}

func (r *mockRows) Columns() []string {
	return r.columns
}

func (r *mockRows) Close() error {
	return nil
}

func (r *mockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	for i, v := range r.data[r.pos] {
		dest[i] = v
	}
	r.pos++
	return nil
}

func init() {
	sql.Register("mysql_mock", &mockDriver{})
}

type mockSubscriber struct{}

func (m *mockSubscriber) ReceiveUpdate(upd backend.BackendUpdate)       {}
func (m *mockSubscriber) SubscribeTo(bup backend.BackendUpdateProvider) {}
func (m *mockSubscriber) GetUpdateSource() string                       { return "" }

// TestMySQL verifies the initialization, configuration validation, and backend update handling
// for the MySQL checker. It also tests various error and panic scenarios during health checks.
func TestMySQL(t *testing.T) {
	mysqlDriverName = "mysql_mock"

	factory := factories["mysql"]

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

	config := &Config{
		Name:   "test",
		Type:   "mysql",
		Config: body,
		ctx:    &hcl.EvalContext{},
	}

	diags := factory.ValidateConfig(config)
	if diags.HasErrors() {
		t.Fatal(diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	mod := factory.New(config, wg, ctx)
	mysqlChecker := mod.(*MySQLChecker)

	mysqlChecker.GetID()
	mysqlChecker.GetUpdateSource()
	mysqlChecker.GetBackendList()

	dp := &dummyProvider{id: "test", backends: backend.NewRegistry()}
	modules := module.NewModulesList()
	modules.AddModule(dp)
	mysqlChecker.Bind(modules)

	subscriber := &mockSubscriber{}
	mysqlChecker.ProvideUpdates(subscriber)

	// Add backend
	b := &backend.Backend{Address: "127.0.0.1:3306", Meta: backend.NewEmptyMetaMap(0)}
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "127.0.0.1:3306",
		Backend: b,
	})

	testutil.Eventually(t, func() bool {
		mysqlChecker.checksMtex.RLock()
		defer mysqlChecker.checksMtex.RUnlock()
		_, ok := mysqlChecker.checks["127.0.0.1:3306"]
		return ok
	}, 1*time.Second, 10*time.Millisecond)

	// Modified backend
	b.Meta.Set("test", "test", cty.StringVal("test"))
	// Set a value in the mysql bucket to verify it's preserved
	mysqlChecker.checksMtex.RLock()
	check, _ := mysqlChecker.checks["127.0.0.1:3306"]
	check.backend.Meta.Set("mysql", "status", cty.StringVal("preserved"))
	mysqlChecker.checksMtex.RUnlock()

	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendModified,
		Address: "127.0.0.1:3306",
		Backend: b,
	})

	testutil.Eventually(t, func() bool {
		mysqlChecker.checksMtex.RLock()
		defer mysqlChecker.checksMtex.RUnlock()
		check, ok := mysqlChecker.checks["127.0.0.1:3306"]
		if !ok {
			return false
		}
		val, ok := check.backend.Meta.Get("test", "test")
		if !ok || val.AsString() != "test" {
			return false
		}
		val, ok = check.backend.Meta.Get("mysql", "status")
		return ok && val.AsString() == "preserved"
	}, 1*time.Second, 10*time.Millisecond)

	// Remove backend
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "127.0.0.1:3306",
	})

	testutil.Eventually(t, func() bool {
		mysqlChecker.checksMtex.RLock()
		defer mysqlChecker.checksMtex.RUnlock()
		_, ok := mysqlChecker.checks["127.0.0.1:3306"]
		return !ok
	}, 1*time.Second, 10*time.Millisecond)

	// Test directly some checks to cover panic cases and fetch logic
	runTestCheck := func(name string) {
		c := NewMySQLCheck(b, name, time.Millisecond, time.Millisecond, 1.0, time.Minute*5, make(chan *backend.Backend, 1), true)
		c.ticker = misc.NewExponentialBackoffTicker(time.Millisecond, time.Millisecond, 1.0)
		c.db, _ = sql.Open("mysql_mock", name)
		c.fetchStatus()
	}

	runTestCheck("panic_readonly")
	runTestCheck("panic_replica")
	runTestCheck("empty_replica")
	runTestCheck("null_replica")
	runTestCheck("no_sbs_replica")
	runTestCheck("err_readonly")
	runTestCheck("err_replica")

	lifecycleCheck := NewMySQLCheck(b, "error", time.Millisecond, time.Millisecond, 1.0, time.Minute*5, make(chan *backend.Backend, 1), true)
	mysqlDriverName = "invalid_driver"
	lifecycleCheck.StartPolling() // Error opening db
	mysqlDriverName = "mysql_mock"
	lifecycleCheck.StopPolling()  // Try to stop not running
	lifecycleCheck.running = true // Force running to true
	lifecycleCheck.ticker = misc.NewExponentialBackoffTicker(time.Millisecond, time.Millisecond, 1.0)
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
	mysqlDriverName = "mysql_mock"
	factory := factories["mysql"]

	// 1. Defaults parsing in Config
	body := &hclsyntax.Body{
		Attributes: map[string]*hclsyntax.Attribute{
			"source": {Name: "source", Expr: &hclsyntax.TemplateExpr{Parts: []hclsyntax.Expression{&hclsyntax.LiteralValueExpr{Val: cty.StringVal("test_cov")}}}},
		},
	}
	config := &Config{Name: "test_cov", Type: "mysql", Config: body, ctx: &hcl.EvalContext{}}
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	mod := factory.New(config, wg, ctx)
	mysqlChecker := mod.(*MySQLChecker)

	// 2. Add an item directly to cover loop execution in GetBackendList, ProvideUpdates, and stopChecks
	b := &backend.Backend{Address: "127.0.0.1:3307", Meta: backend.NewEmptyMetaMap(0)}
	statusChan := make(chan *backend.Backend, 100)
	// Drain status channel asynchronously to prevent deadlocks during the test
	go func() {
		for range statusChan {
		}
	}()
	check := NewMySQLCheck(b, "ok", 1*time.Millisecond, 10*time.Millisecond, 2.0, time.Minute*5, statusChan, true)
	check.db, _ = sql.Open("mysql_mock", "ok")
	check.ticker = misc.NewExponentialBackoffTicker(1*time.Millisecond, 10*time.Millisecond, 2.0)
	mysqlChecker.checksMtex.Lock()
	mysqlChecker.checks["127.0.0.1:3307"] = check
	mysqlChecker.checksMtex.Unlock()

	mysqlChecker.GetBackendList()
	mysqlChecker.ProvideUpdates(&mockSubscriber{})
	// Give a bit of time for goroutines to run
	time.Sleep(2 * time.Millisecond)

	// 3. StartPolling / StopPolling early returns logic (c.running = true/false)
	check.StartPolling()
	check.StartPolling()
	time.Sleep(2 * time.Millisecond)
	check.StopPolling()
	check.StopPolling()
	time.Sleep(2 * time.Millisecond) // Let StartPolling goroutine exit and run defer

	// 4. UpdateStatus specific log branches: Known status changed
	check.updateStatus()
	check.backend.Meta.Set("mysql", "status", cty.StringVal("err"))
	check.backend.Meta.Set("mysql", "readonly", cty.BoolVal(true))
	check.backend.Meta.Set("mysql", "replica_latency", cty.NumberIntVal(10))
	check.updateStatus() // Update known values with differing state

	check.dsn = "err_replica"
	check.db, _ = sql.Open("mysql_mock", "err_replica")
	check.updateStatus() // Triggers err != nil

	// 5. fetchStatus reopen error handling branch inside recovery block
	check.dsn = "error" // sql.Open("mysql_mock", "error") evaluates to an error mock
	check.db, _ = sql.Open("mysql_mock", "panic_readonly")
	mysqlDriverName = "invalid_driver"
	check.fetchStatus()
	mysqlDriverName = "mysql_mock"

	// Hit Reset update == true in fetchStatus
	check.dsn = "ok"
	check.db, _ = sql.Open("mysql_mock", "ok")
	check.ticker.ApplyBackoff() // Force it to backoff state
	check.fetchStatus()         // Should succeed and call Reset() -> true

	// Hit Reset update == false in fetchStatus
	check.fetchStatus()

	// Hit checkReplica == false in fetchStatus
	check.checkReplica = false
	check.fetchStatus()
	check.checkReplica = true

	// Hit c.db == nil inside recover block of fetchStatus
	check.db = nil
	check.dsn = "panic_readonly"
	check.fetchStatus()

	// Hit ApplyBackoff update == false in fetchStatus error
	check.dsn = "panic_readonly"
	check.db, _ = sql.Open("mysql_mock", "panic_readonly")
	// Call it many times to reach max backoff, so ApplyBackoff returns false
	for i := 0; i < 10; i++ {
		check.ticker.ApplyBackoff()
	}
	check.fetchStatus()

	// 6. Unknown backend removed
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendRemoved,
		Address: "unknown_address",
	})

	// 7. Add a backend that fails StartPolling due to sql.Open failing
	mysqlDriverName = "invalid_driver"
	mysqlChecker.ReceiveUpdate(backend.BackendUpdate{
		Kind:    backend.UpdBackendAdded,
		Address: "error_address",
		Backend: &backend.Backend{Address: "error_address", Meta: backend.NewEmptyMetaMap(0)},
	})
	// Small sleep to ensure the goroutine picks up the update while mysqlDriverName is still invalid.
	time.Sleep(20 * time.Millisecond)
	mysqlDriverName = "mysql_mock"

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
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &Config{
		Type:   "mysql",
		Name:   "test",
		Config: block.Body,
		ctx:    ctx,
	}

	factory := MySQLCheckerFactory{}
	// This will trigger log.Error() and still return a config object
	config := factory.parseConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}
