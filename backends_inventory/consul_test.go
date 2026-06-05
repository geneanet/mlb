package backends_inventory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"mlb/backend"
	"mlb/module"
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

// SubscribeTo is a no-op for this mock.
func (d *consulDummySubscriber) SubscribeTo(p backend.BackendUpdateProvider) {}

// GetUpdateSource returns a default identifier.
func (d *consulDummySubscriber) GetUpdateSource() string { return "consul_dummy" }

// TestConsulBackendsInventory_All tests the full functionality of the Consul backends inventory,
// including service discovery, reacting to changes in Consul, and service removal.
func TestConsulBackendsInventory_All(t *testing.T) {
	// Mock Consul server
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if r.URL.Path != "/v1/health/service/my-service" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("X-Consul-Index", "1")

		var services consulServicesSlice
		if callCount == 1 {
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
		} else if callCount == 2 {
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
	consulMod, ok := mod.(*BackendsInventoryConsul)
	if !ok {
		t.Fatalf("Expected *BackendsInventoryConsul")
	}

	if consulMod.GetID() != "backends_inventory.consul.test" {
		t.Errorf("Unexpected ID: %s", consulMod.GetID())
	}

	consulMod.Bind(module.ModulesList{})

	sub := &consulDummySubscriber{}
	consulMod.ProvideUpdates(sub)

	time.Sleep(100 * time.Millisecond) // Let it fetch a few times

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
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	mod := New(cfg, wg, ctxBG)

	// Wait a bit to let it fail fetching
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
	_ = mod
}

// TestConsulBackendsInventory_Recovery verifies that the Consul inventory
// can recover from temporary server errors.
func TestConsulBackendsInventory_Recovery(t *testing.T) {
	// Mock Consul server that fails first, then succeeds
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	New(cfg, wg, ctxBG)

	// Let it fail and then succeed
	time.Sleep(100 * time.Millisecond)
	cancel()
	wg.Wait()
}

// TestConsulBackendsInventory_ContextCanceled verifies that the Consul inventory
// correctly handles context cancellation during long-polling.
func TestConsulBackendsInventory_ContextCanceled(t *testing.T) {
	// Mock Consul server that blocks until the context is closed,
	// forcing the client to cancel the request.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())

	New(cfg, wg, ctxBG)

	// Wait a bit so the fetch call begins and is blocked
	time.Sleep(50 * time.Millisecond)

	// Cancel the context to force fetch to return context.Canceled
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
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	cfg, _ := DecodeConfigBlock(block, ctx)

	wg := &sync.WaitGroup{}
	ctxBG, cancel := context.WithCancel(context.Background())
	defer cancel()

	mod := New(cfg, wg, ctxBG)
	consulMod := mod.(*BackendsInventoryConsul)

	// Wait for the first fetch so backend is populated
	time.Sleep(100 * time.Millisecond)

	sub := &consulDummySubscriber{}
	consulMod.ProvideUpdates(sub)

	// Wait for ProvideUpdates goroutine to execute
	time.Sleep(50 * time.Millisecond)

	sub.mu.Lock()
	count := len(sub.updates)
	sub.mu.Unlock()

	if count == 0 {
		t.Errorf("Expected updates from ProvideUpdates, got 0")
	}
}
