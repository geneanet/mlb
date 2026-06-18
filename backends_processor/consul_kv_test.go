package backends_processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mlb/backend"
	"mlb/module"
	"mlb/testutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"
)

// TestConsulKV_Basic tests the main functionality of the ConsulKV processor,
// including initial data fetching, reacting to Consul value changes, and handling backend updates.
func TestConsulKV_Basic(t *testing.T) {
	// Create mock consul server
	var mu sync.Mutex
	callCount := 0
	var consulIndex string = "1"
	var consulValue string = "default"
	var statusToReturn int = 200

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		idx := consulIndex
		val := consulValue
		status := statusToReturn
		mu.Unlock()

		if status == 404 {
			w.WriteHeader(404)
			return
		} else if status != 200 {
			w.WriteHeader(status)
			return
		}

		w.Header().Set("X-Consul-Index", idx)

		res := []consulKVValue{
			{
				Key:   "foo/bar",
				Value: base64.StdEncoding.EncodeToString([]byte(val)),
			},
		}
		b, _ := json.Marshal(res)
		w.Write(b)
	}))
	defer ts.Close()

	src := fmt.Sprintf(`
backends_processor "consul_kv" "test" {
	source = "foo"
	url = "%s"
	period = "10ms"
	max_period = "50ms"
	value "weight" {
		consul_key = "foo/${backend.address}"
		default = "1"
	}
	value "invalid" {
		consul_key = missing_var
		default = "0"
	}
}
`, ts.URL)

	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	diags = module.ValidateConfig(cfg, "backends_processor")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	consulMod := mod.(*ConsulKV)

	dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}
	modules := make(module.ModulesRegistry)
	modules.AddModule("foo", dp)
	consulMod.Bind(modules)

	sub := &dummySubscriber{wg: sync.WaitGroup{}}
	consulMod.ProvideUpdates(sub)

	// Add a backend
	b1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}

	// We expect 1 add/modify from the add, and 1 from watcher updating
	sub.wg.Add(2)

	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b1.Address, Backend: b1})

	waitSub(t, sub, "Wait for backend add and first watch update")

	if len(consulMod.GetBackendList()) != 1 {
		t.Errorf("Expected 1 backend")
	}

	bList := consulMod.GetBackendList()
	b1Mod := bList[0]

	valMeta, ok := b1Mod.Meta.Get("consul_kv", "weight")
	if !ok {
		t.Errorf("Expected consul_kv weight meta")
	} else {
		if valMeta.AsString() != "default" {
			t.Errorf("Expected 'default', got %s", valMeta.AsString())
		}
	}

	// Change value in consul
	sub.wg.Add(1)
	mu.Lock()
	consulValue = "newval"
	consulIndex = "2"
	mu.Unlock()

	waitSub(t, sub, "Wait for watcher to pick up newval")
	valMeta, ok = b1Mod.Meta.Get("consul_kv", "weight")
	if !ok || valMeta.AsString() != "newval" {
		t.Errorf("Expected 'newval', got %v", valMeta)
	}

	// Test Update backend (should recreate watchers)
	sub.wg.Add(1)
	b1Updated := b1Mod.Clone()
	// Add some other metadata to verify it's updated
	b1Updated.Meta.Set("other", "foo", cty.StringVal("bar"))
	// Set a value in consul_kv bucket to see if it's preserved
	b1Mod.Meta.Set("consul_kv", "test", cty.StringVal("preserved"))

	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: b1Updated.Address, Backend: b1Updated})
	waitSub(t, sub, "Wait for backend update to propagate")

	valMeta, ok = b1Mod.Meta.Get("other", "foo")
	if !ok || valMeta.AsString() != "bar" {
		t.Errorf("Expected 'bar', got %v", valMeta)
	}
	valMeta, ok = b1Mod.Meta.Get("consul_kv", "test")
	if !ok || valMeta.AsString() != "preserved" {
		t.Errorf("Expected 'preserved', got %v (metadata loss!)", valMeta)
	}

	// Then watcher picks up again
	sub.wg.Add(1)
	mu.Lock()
	consulValue = "newval2"
	consulIndex = "3"
	mu.Unlock()
	waitSub(t, sub, "Wait for recreated watcher to pick up newval2")

	// Test 404 from consul
	sub.wg.Add(1)
	mu.Lock()
	statusToReturn = 404
	consulIndex = "4"
	mu.Unlock()
	waitSub(t, sub, "Wait for 404 from watcher")

	// Test 500 error from consul to trigger backoff
	mu.Lock()
	statusToReturn = 500
	lastCallCount := callCount
	mu.Unlock()
	testutil.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return callCount > lastCallCount+1
	}, 1*time.Second, 10*time.Millisecond) // Let it fail a few times

	mu.Lock()
	statusToReturn = 200
	consulValue = "after_error"
	mu.Unlock()
	sub.wg.Add(1)
	waitSub(t, sub, "Wait for value after error")

	// Test Remove
	sub.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: b1.Address})
	waitSub(t, sub, "Wait for remove")

	if len(consulMod.GetBackendList()) != 0 {
		t.Errorf("Expected 0 backends after remove")
	}

	// Remove non-existent
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "127.0.0.1:9999"})
}

// TestConsulKV_ProvideUpdatesExisting verifies that new subscribers receive
// updates for backends already present in the ConsulKV processor.
func TestConsulKV_ProvideUpdatesExisting(t *testing.T) {
	src := `
backends_processor "consul_kv" "test" {
	source = "foo"
	url = "http://localhost"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_processor")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	consulMod := mod.(*ConsulKV)

	dp := &dummyProvider{id: "foo", backends: backend.NewRegistry()}
	modules := make(module.ModulesRegistry)
	modules.AddModule("foo", dp)
	consulMod.Bind(modules)

	b1 := &backend.Backend{Address: "127.0.0.1:8080", Meta: backend.NewEmptyMetaMap(0)}
	sub1 := &dummySubscriber{wg: sync.WaitGroup{}}
	consulMod.ProvideUpdates(sub1)
	sub1.wg.Add(1)
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendAdded, Address: b1.Address, Backend: b1})
	waitSub(t, sub1, "Wait for first add")

	sub2 := &dummySubscriber{wg: sync.WaitGroup{}}
	sub2.wg.Add(1)
	consulMod.ProvideUpdates(sub2)
	waitSub(t, sub2, "Wait for sub2 existing update")
}

// TestConsulKV_Defaults verifies that default values are correctly applied
// when not specified in the configuration.
func TestConsulKV_Defaults(t *testing.T) {
	src := `
backends_processor "consul_kv" "test_defaults" {
	source = "foo"
	url = "http://localhost"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_processor")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	consulMod := mod.(*ConsulKV)

	if consulMod.defaultPeriod != 500*time.Millisecond {
		t.Errorf("Expected 500ms, got %v", consulMod.defaultPeriod)
	}
	if consulMod.maxPeriod != 2*time.Second {
		t.Errorf("Expected 2s, got %v", consulMod.maxPeriod)
	}
	if consulMod.backoffFactor != 1.5 {
		t.Errorf("Expected 1.5, got %v", consulMod.backoffFactor)
	}
}

// TestConsulKV_InvalidPeriod verifies that an invalid period configuration causes a panic.
func TestConsulKV_InvalidPeriod(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic due to invalid period")
		}
	}()
	src := `
backends_processor "consul_kv" "test" {
	source = "foo"
	url = "http://localhost"
	period = "invalid"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_processor")
	wg := &sync.WaitGroup{}
	module.New(cfg, wg, context.Background(), "backends_processor")
}

// TestConsulKV_InvalidMaxPeriod verifies that an invalid max_period configuration causes a panic.
func TestConsulKV_InvalidMaxPeriod(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic due to invalid max_period")
		}
	}()
	src := `
backends_processor "consul_kv" "test" {
	source = "foo"
	url = "http://localhost"
	period = "10ms"
	max_period = "invalid"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_processor")
	wg := &sync.WaitGroup{}
	module.New(cfg, wg, context.Background(), "backends_processor")
}

// TestConsulKV_ReceiveUpdateClosed verifies that the processor handles updates
// gracefully after it has been shut down.
func TestConsulKV_ReceiveUpdateClosed(t *testing.T) {
	src := `
backends_processor "consul_kv" "test" {
	source = "foo"
	url = "http://localhost"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_processor")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	mod := module.New(cfg, wg, ctxBG, "backends_processor")
	consulMod := mod.(*ConsulKV)

	cancel()
	wg.Wait()

	// Should hit <-c.updChanStop directly instead of blocking on the main loop
	consulMod.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
}

// TestConsulKV_FetchErrors tests various HTTP and JSON parsing error scenarios
// in the Consul KV watcher.
func TestConsulKV_FetchErrors(t *testing.T) {
	var mu sync.Mutex
	var responseBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		body := responseBody
		mu.Unlock()
		w.Header().Set("X-Consul-Index", "1")
		w.Write([]byte(body))
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Case 1: Invalid JSON response
	mu.Lock()
	responseBody = `invalid json`
	mu.Unlock()
	w := newConsulKVWatcher(&backend.Backend{Address: "foo"}, "id", ts.URL, "key", 10*time.Millisecond, 50*time.Millisecond, 1.5, make(chan *consulKVWatcherMessage, 10), ctx, log.Logger)
	_, err := w.fetch()
	if err == nil {
		t.Errorf("Expected error for invalid json")
	}

	// Case 2: Invalid base64 in JSON response
	mu.Lock()
	responseBody = `[{"Key":"foo","Value":"@@@"}]`
	mu.Unlock()
	_, err = w.fetch()
	if err == nil {
		t.Errorf("Expected error for invalid base64")
	}

	// Case 3: Invalid URL scheme
	w = newConsulKVWatcher(&backend.Backend{Address: "foo"}, "id", "httpxx://invalid", "key", 10*time.Millisecond, 50*time.Millisecond, 1.5, make(chan *consulKVWatcherMessage, 10), ctx, log.Logger)
	_, err = w.fetch()
	if err == nil {
		t.Errorf("Expected error for invalid scheme")
	}
}

// TestConsulKV_WatcherCoverage tests edge cases in the Consul KV watcher's execution loop,
// such as context cancellation during fetch or sleep.
func TestConsulKV_WatcherCoverage(t *testing.T) {
	// Case 1: Cancel context during an active HTTP fetch.
	var callCount1 atomic.Int64
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount1.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer ts1.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ch := make(chan *consulKVWatcherMessage, 10)
	newConsulKVWatcher(&backend.Backend{Address: "foo1"}, "id", ts1.URL, "key", 10*time.Millisecond, 50*time.Millisecond, 1.5, ch, ctx1, log.Logger)

	// Ensure request starts
	testutil.Eventually(t, func() bool {
		return callCount1.Load() > 0
	}, 1*time.Second, 10*time.Millisecond)

	cancel1() // Force context cancellation natively mid-flight

	// Case 2: Cancel context while the watcher is sleeping between requests.
	var callCount2 atomic.Int64
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount2.Add(1)
		w.Header().Set("X-Consul-Index", "1")
		w.Write([]byte(`[{"Key":"foo","Value":"YmFy"}]`))
	}))
	defer ts2.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	newConsulKVWatcher(&backend.Backend{Address: "foo2"}, "id", ts2.URL, "key", 500*time.Millisecond, 1*time.Second, 1.5, ch, ctx2, log.Logger)

	// Give it time to execute its first request and fall into sleep
	testutil.Eventually(t, func() bool {
		return callCount2.Load() > 0
	}, 1*time.Second, 10*time.Millisecond)

	cancel2()
}

// TestConsulKV_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestConsulKV_ParseConfigError(t *testing.T) {
	// Invalid config (backoff_factor should be a number, not a string)
	src := `
backends_processor "consul_kv" "test" {
	source = "foo"
	url = "http://localhost"
	backoff_factor = "invalid"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &module.Config{
		Category: "backends_processor",
		Type:     "consul_kv",
		Name:     "test",
		Config:   block.Body,
		Ctx:      ctx,
	}

	// This will trigger log.Error() and still return a config object
	config := parseConsulKVConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}
