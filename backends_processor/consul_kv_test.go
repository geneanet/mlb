package backends_processor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mlb/backend"
	"mlb/module"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/rs/zerolog/log"
)

// TestConsulKV_Basic tests the main functionality of the ConsulKV processor,
// including initial data fetching, reacting to Consul value changes, and handling backend updates.
func TestConsulKV_Basic(t *testing.T) {
	// Create mock consul server
	var consulIndex string = "1"
	var consulValue string = "default"
	var statusToReturn int = 200

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if statusToReturn == 404 {
			w.WriteHeader(404)
			return
		} else if statusToReturn != 200 {
			w.WriteHeader(statusToReturn)
			return
		}

		w.Header().Set("X-Consul-Index", consulIndex)

		val := []consulKVValue{
			{
				Key:   "foo/bar",
				Value: base64.StdEncoding.EncodeToString([]byte(consulValue)),
			},
		}
		b, _ := json.Marshal(val)
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

	cfg, diags := DecodeConfigBlock(block, ctx)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	diags = ValidateConfig(cfg)
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
	consulMod := mod.(*ConsulKV)

	if consulMod.GetID() != "backends_processor.consul_kv.test" {
		t.Errorf("Unexpected ID: %s", consulMod.GetID())
	}
	if consulMod.GetUpdateSource() != "foo" {
		t.Errorf("Unexpected update source: %s", consulMod.GetUpdateSource())
	}

	dp := &dummyProvider{id: "foo"}
	modules := module.NewModulesList()
	modules.AddModule(dp)
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

	val, ok := b1Mod.Meta.Get("consul_kv", "weight")
	if !ok {
		t.Errorf("Expected consul_kv weight meta")
	} else {
		if val.AsString() != "default" {
			t.Errorf("Expected 'default', got %s", val.AsString())
		}
	}

	// Change value in consul
	sub.wg.Add(1)
	consulValue = "newval"
	consulIndex = "2"

	waitSub(t, sub, "Wait for watcher to pick up newval")
	val, ok = b1Mod.Meta.Get("consul_kv", "weight")
	if !ok || val.AsString() != "newval" {
		t.Errorf("Expected 'newval', got %v", val)
	}

	// Test Update backend (should recreate watchers)
	sub.wg.Add(1)
	b1Updated := b1Mod.Clone()
	dp.sendUpdate(backend.BackendUpdate{Kind: backend.UpdBackendModified, Address: b1Updated.Address, Backend: b1Updated})
	waitSub(t, sub, "Wait for backend update to propagate")

	// Then watcher picks up again
	sub.wg.Add(1)
	consulValue = "newval2"
	consulIndex = "3"
	waitSub(t, sub, "Wait for recreated watcher to pick up newval2")

	// Test 404 from consul
	sub.wg.Add(1)
	statusToReturn = 404
	consulIndex = "4"
	waitSub(t, sub, "Wait for 404 from watcher")

	// Test 500 error from consul to trigger backoff
	statusToReturn = 500
	time.Sleep(100 * time.Millisecond) // Let it fail a few times

	statusToReturn = 200
	consulValue = "after_error"
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
	time.Sleep(10 * time.Millisecond)
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
	consulMod := mod.(*ConsulKV)

	dp := &dummyProvider{id: "foo"}
	modules := module.NewModulesList()
	modules.AddModule(dp)
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
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
	cfg, _ := DecodeConfigBlock(block, ctx)
	wg := &sync.WaitGroup{}
	New(cfg, wg, context.Background())
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
	cfg, _ := DecodeConfigBlock(block, ctx)
	wg := &sync.WaitGroup{}
	New(cfg, wg, context.Background())
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	mod := New(cfg, wg, ctxBG)
	consulMod := mod.(*ConsulKV)

	cancel()
	wg.Wait()

	// Should hit <-c.updChanStop directly instead of blocking on the main loop
	consulMod.ReceiveUpdate(backend.BackendUpdate{Kind: backend.UpdBackendRemoved, Address: "foo"})
}

// TestConsulKV_FetchErrors tests various HTTP and JSON parsing error scenarios
// in the Consul KV watcher.
func TestConsulKV_FetchErrors(t *testing.T) {
	var responseBody string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Consul-Index", "1")
		w.Write([]byte(responseBody))
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Case 1: Invalid JSON response
	responseBody = `invalid json`
	w := newConsulKVWatcher(&backend.Backend{Address: "foo"}, "id", ts.URL, "key", 10*time.Millisecond, 50*time.Millisecond, 1.5, make(chan *consulKVWatcherMessage), ctx, log.Logger)
	_, err := w.fetch()
	if err == nil {
		t.Errorf("Expected error for invalid json")
	}

	// Case 2: Invalid base64 in JSON response
	responseBody = `[{"Key":"foo","Value":"@@@"}]`
	_, err = w.fetch()
	if err == nil {
		t.Errorf("Expected error for invalid base64")
	}

	// Case 3: Invalid URL scheme
	w = newConsulKVWatcher(&backend.Backend{Address: "foo"}, "id", "httpxx://invalid", "key", 10*time.Millisecond, 50*time.Millisecond, 1.5, make(chan *consulKVWatcherMessage), ctx, log.Logger)
	_, err = w.fetch()
	if err == nil {
		t.Errorf("Expected error for invalid scheme")
	}
}

// TestConsulKV_WatcherCoverage tests edge cases in the Consul KV watcher's execution loop,
// such as context cancellation during fetch or sleep.
func TestConsulKV_WatcherCoverage(t *testing.T) {
	// Case 1: Cancel context during an active HTTP fetch.
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer ts1.Close()

	ctx1, cancel1 := context.WithCancel(context.Background())
	ch := make(chan *consulKVWatcherMessage, 10)
	newConsulKVWatcher(&backend.Backend{Address: "foo1"}, "id", ts1.URL, "key", 10*time.Millisecond, 50*time.Millisecond, 1.5, ch, ctx1, log.Logger)

	time.Sleep(20 * time.Millisecond) // Ensure request starts
	cancel1()                         // Force context cancellation natively mid-flight
	time.Sleep(100 * time.Millisecond)

	// Case 2: Cancel context while the watcher is sleeping between requests.
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Consul-Index", "1")
		w.Write([]byte(`[{"Key":"foo","Value":"YmFy"}]`))
	}))
	defer ts2.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	newConsulKVWatcher(&backend.Backend{Address: "foo2"}, "id", ts2.URL, "key", 500*time.Millisecond, 1*time.Second, 1.5, ch, ctx2, log.Logger)

	// Give it time to execute its first request and fall into sleep
	time.Sleep(50 * time.Millisecond)
	cancel2()
	time.Sleep(50 * time.Millisecond)
}
