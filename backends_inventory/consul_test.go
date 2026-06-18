package backends_inventory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mlb/backend"
	"mlb/module"
	"mlb/testutil"

	"github.com/hashicorp/hcl/v2"
)

// consulDummySubscriber implements backend.BackendUpdateSubscriber for testing.
type consulDummySubscriber struct {
	updates []backend.BackendUpdate
	mu      sync.Mutex
}

// ReceiveUpdate records an update in a thread-safe manner.
func (d *consulDummySubscriber) ReceiveUpdate(u backend.BackendUpdate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.updates = append(d.updates, u)
}

// TestConsulBackendsInventory_All tests the full functionality of the Consul backends inventory,
// including service discovery, reacting to changes in Consul, and service removal.
func TestConsulBackendsInventory_All(t *testing.T) {
	// Mock Consul server
	var callCount atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cnt := callCount.Add(1)
		if r.URL.Path != "/v1/health/service/my-service" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("X-Consul-Index", "1")

		var services consulServicesSlice
		if cnt == 1 {
			// First call: service node1 with tag1
			services = consulServicesSlice{
				{
					Node: struct{ Node string }{"node1"},
					Service: struct {
						Tags    []string
						Address string
						Port    int
						Weights struct {
							Passing uint64
							Warning uint64
						}
						ModifyIndex int
					}{
						Tags:    []string{"tag1"},
						Address: "127.0.0.1",
						Port:    8080,
						Weights: struct {
							Passing uint64
							Warning uint64
						}{Passing: 1, Warning: 0},
						ModifyIndex: 1,
					},
				},
			}
		} else if cnt == 2 {
			// Second call: service node1 with changed tag and index
			services = consulServicesSlice{
				{
					Node: struct{ Node string }{"node1"},
					Service: struct {
						Tags    []string
						Address string
						Port    int
						Weights struct {
							Passing uint64
							Warning uint64
						}
						ModifyIndex int
					}{
						Tags:    []string{"tag2"}, // changed tag
						Address: "127.0.0.1",
						Port:    8080,
						Weights: struct {
							Passing uint64
							Warning uint64
						}{Passing: 1, Warning: 0},
						ModifyIndex: 2, // changed index
					},
				},
			}
		} else {
			// Subsequent calls: empty services (service removed)
			services = consulServicesSlice{}
		}

		json.NewEncoder(w).Encode(services)
	}))
	defer ts.Close()

	src := `
backends_inventory "consul" "test" {
	url = "` + ts.URL + `"
	service = "my-service"
	period = "10ms"
	max_period = "10ms"
	backoff_factor = 1.0
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg, diags := module.DecodeConfigBlock(block, ctx, "backends_inventory")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	diags = module.ValidateConfig(cfg, "backends_inventory")
	if diags.HasErrors() {
		t.Fatalf("Unexpected errors: %s", diags.Error())
	}

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_inventory")
	consulMod, ok := mod.(*BackendsInventoryConsul)
	if !ok {
		t.Fatalf("Expected *BackendsInventoryConsul")
	}

	consulMod.Bind(module.ModulesRegistry{})

	sub := &consulDummySubscriber{}
	consulMod.ProvideUpdates(sub)

	// Wait for all expected updates to be received
	testutil.Eventually(t, func() bool {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		hasAdded := false
		hasModified := false
		hasRemoved := false
		for _, u := range sub.updates {
			switch u.Kind {
			case backend.UpdBackendAdded:
				hasAdded = true
			case backend.UpdBackendModified:
				hasModified = true
			case backend.UpdBackendRemoved:
				hasRemoved = true
			}
		}
		return hasAdded && hasModified && hasRemoved
	}, 1*time.Second, 10*time.Millisecond)

	// Call ProvideUpdates again to cover the path where backends list is not empty
	consulMod.ProvideUpdates(&consulDummySubscriber{})

	// Check GetBackendList
	list := consulMod.GetBackendList()
	_ = list // Will be empty after removal

	// Cancel context to stop fetching
	cancel()
	wg.Wait()

	// Verify subscriber received updates
	hasAdded := false
	hasModified := false
	hasRemoved := false
	sub.mu.Lock()
	for _, u := range sub.updates {
		if u.Kind == backend.UpdBackendAdded {
			hasAdded = true
		} else if u.Kind == backend.UpdBackendModified {
			hasModified = true
		} else if u.Kind == backend.UpdBackendRemoved {
			hasRemoved = true
		}
	}
	sub.mu.Unlock()
	if !hasAdded {
		t.Errorf("Expected UpdBackendAdded")
	}
	if !hasModified {
		t.Errorf("Expected UpdBackendModified")
	}
	if !hasRemoved {
		t.Errorf("Expected UpdBackendRemoved")
	}
}

// TestConsulBackendsInventory_Error verifies that the Consul inventory handles
// HTTP 500 errors from the Consul server gracefully.
func TestConsulBackendsInventory_Error(t *testing.T) {
	// Mock Consul server that returns 500
	var callCount atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	src := `
backends_inventory "consul" "test_err" {
	url = "` + ts.URL + `"
	service = "my-service"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_inventory")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	mod := module.New(cfg, wg, ctxBG, "backends_inventory")

	// Wait for at least one fetch attempt
	testutil.Eventually(t, func() bool {
		return callCount.Load() > 0
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
	_ = mod
}

// TestConsulBackendsInventory_Recovery verifies that the Consul inventory
// can recover from temporary server errors.
func TestConsulBackendsInventory_Recovery(t *testing.T) {
	// Mock Consul server that fails first, then succeeds
	var callCount atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cnt := callCount.Add(1)
		if cnt == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("X-Consul-Index", "1")
		services := consulServicesSlice{}
		json.NewEncoder(w).Encode(services)
	}))
	defer ts.Close()

	src := `
backends_inventory "consul" "test_rec" {
	url = "` + ts.URL + `"
	service = "my-service"
	period = "10ms"
	max_period = "20ms"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_inventory")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	module.New(cfg, wg, ctxBG, "backends_inventory")

	// Let it fail and then succeed
	testutil.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

// TestConsulBackendsInventory_ContextCanceled verifies that the Consul inventory
// correctly handles context cancellation during long-polling.
func TestConsulBackendsInventory_ContextCanceled(t *testing.T) {
	// Mock Consul server that blocks until the context is closed,
	// forcing the client to cancel the request.
	var callCount atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		<-r.Context().Done()
	}))
	defer ts.Close()

	src := `
backends_inventory "consul" "test_cancel" {
	url = "` + ts.URL + `"
	service = "my-service"
	period = "10ms"
	max_period = "20ms"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_inventory")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	module.New(cfg, wg, ctxBG, "backends_inventory")

	// Wait for at least one attempt
	testutil.Eventually(t, func() bool {
		return callCount.Load() > 0
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	wg.Wait()
}

// TestConsulServicesDiff tests the diffing logic between two sets of Consul services.
func TestConsulServicesDiff(t *testing.T) {
	// Case 1: diffing nil slices should return empty results.
	added, modified, removed := consulServicesDiff(nil, nil)
	if len(added) != 0 || len(modified) != 0 || len(removed) != 0 {
		t.Errorf("Expected empty maps, got %v, %v, %v", added, modified, removed)
	}
}

// TestConsulBackendsInventory_ProvideUpdates verifies that new subscribers
// receive the current list of discovered backends upon registration.
func TestConsulBackendsInventory_ProvideUpdates(t *testing.T) {
	var callCount atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.Header().Set("X-Consul-Index", "1")
		services := consulServicesSlice{
			{
				Node: struct{ Node string }{"node1"},
				Service: struct {
					Tags    []string
					Address string
					Port    int
					Weights struct {
						Passing uint64
						Warning uint64
					}
					ModifyIndex int
				}{
					Tags:    []string{"tag1"},
					Address: "127.0.0.1",
					Port:    8080,
					Weights: struct {
						Passing uint64
						Warning uint64
					}{Passing: 1, Warning: 0},
					ModifyIndex: 1,
				},
			},
		}
		json.NewEncoder(w).Encode(services)
	}))
	defer ts.Close()

	src := `
backends_inventory "consul" "test_pu" {
	url = "` + ts.URL + `"
	service = "my-service"
	period = "1s"
	max_period = "1s"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}
	cfg, _ := module.DecodeConfigBlock(block, ctx, "backends_inventory")

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := module.New(cfg, wg, ctxBG, "backends_inventory")
	consulMod := mod.(*BackendsInventoryConsul)

	// Wait for the first fetch so backend is populated
	testutil.Eventually(t, func() bool {
		return callCount.Load() > 0
	}, 1*time.Second, 10*time.Millisecond)

	sub := &consulDummySubscriber{}
	consulMod.ProvideUpdates(sub)

	// Wait for ProvideUpdates goroutine to execute
	testutil.Eventually(t, func() bool {
		sub.mu.Lock()
		defer sub.mu.Unlock()
		return len(sub.updates) > 0
	}, 1*time.Second, 10*time.Millisecond)

	sub.mu.Lock()
	count := len(sub.updates)
	sub.mu.Unlock()

	if count == 0 {
		t.Errorf("Expected updates from ProvideUpdates, got 0")
	}
}

// TestConsulBackendsInventory_ParseConfigError verifies that parseConfig handles HCL decoding errors.
func TestConsulBackendsInventory_ParseConfigError(t *testing.T) {
	// Invalid config (backoff_factor should be a number, not a string)
	src := `
backends_inventory "consul" "test" {
	url = "http://localhost:8500"
	service = "test"
	backoff_factor = "invalid"
}
`
	block := parseHCL(t, src)
	ctx := &hcl.EvalContext{}

	cfg := &module.Config{
		Type:   "consul",
		Name:   "test",
		Config: block.Body,
		Ctx:    ctx,
	}

	// This will trigger log.Error() and still return a config object
	config := parseConsulBackendsInventoryConfig(cfg)
	if config == nil {
		t.Fatal("expected config not to be nil even on error")
	}
}
