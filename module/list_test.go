package module

import (
	"mlb/backend"
	"testing"
)

// Mock module structures for testing the ModulesList functionality.

// dummyModule is a basic mock that implements the module.Module interface.
type dummyModule struct {
	id string
}

func (d *dummyModule) GetID() string {
	return d.id
}

func (d *dummyModule) Bind(modules ModulesList) {}

// dummyUpdateProvider implements backend.BackendUpdateProvider.
type dummyUpdateProvider struct {
	dummyModule
}

func (d *dummyUpdateProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {}

// dummyProvider implements backend.BackendProvider.
type dummyProvider struct {
	dummyModule
}

func (d *dummyProvider) GetBackend(wait bool) *backend.Backend {
	return nil
}

// dummyListProvider implements backend.BackendListProvider.
type dummyListProvider struct {
	dummyModule
}

func (d *dummyListProvider) GetBackendList() []*backend.Backend {
	return nil
}

// Tests

// TestNewModulesList verifies the correct initialization of an empty ModulesList.
func TestNewModulesList(t *testing.T) {
	ml := NewModulesList()
	if ml == nil {
		t.Fatal("Expected NewModulesList to return a non-nil object")
	}
	if len(ml) != 0 {
		t.Errorf("Expected fresh ModulesList to be empty, got size %d", len(ml))
	}
}

// TestAddModule verifies that modules can be added to and retrieved from the ModulesList by their ID.
func TestAddModule(t *testing.T) {
	ml := NewModulesList()
	m := &dummyModule{id: "m1"}
	ml.AddModule(m)

	if len(ml) != 1 {
		t.Fatalf("Expected ModulesList size 1, got %d", len(ml))
	}

	if ml["m1"] != m {
		t.Errorf("Retrieved module does not match the added module")
	}
}

// TestGet verifies the retrieval of modules using the generic Get function.
func TestGet(t *testing.T) {
	ml := NewModulesList()
	m := &dummyUpdateProvider{dummyModule: dummyModule{id: "m1"}}
	ml.AddModule(m)

	// Scenario 1: Correct retrieval
	bup := Get[backend.BackendUpdateProvider](ml, "m1")
	if bup == nil {
		t.Errorf("Expected to retrieve a valid BackendUpdateProvider for 'm1'")
	}

	// Scenario 2: Module ID not found (expects panic)
	assertPanic(t, func() { Get[backend.BackendUpdateProvider](ml, "missing") }, "Expected panic for missing module ID")

	// Scenario 3: Module found but interface not implemented (expects panic)
	mWrong := &dummyModule{id: "m2"}
	ml.AddModule(mWrong)
	assertPanic(t, func() { Get[backend.BackendUpdateProvider](ml, "m2") }, "Expected panic for module not implementing BackendUpdateProvider")
}

// TestFilter verifies that Filter correctly filters
// and returns all modules in the list that implement the requested interface.
func TestFilter(t *testing.T) {
	ml := NewModulesList()

	m1 := &dummyListProvider{dummyModule: dummyModule{id: "m1"}}
	m2 := &dummyModule{id: "m2"} // Does not implement BackendListProvider
	m3 := &dummyListProvider{dummyModule: dummyModule{id: "m3"}}

	ml.AddModule(m1)
	ml.AddModule(m2)
	ml.AddModule(m3)

	providers := Filter[backend.BackendListProvider](ml)

	if len(providers) != 2 {
		t.Fatalf("Expected exactly 2 BackendListProviders, got %d", len(providers))
	}

	if _, ok := providers["m1"]; !ok {
		t.Errorf("Expected 'm1' to be in the filtered results")
	}
	if _, ok := providers["m3"]; !ok {
		t.Errorf("Expected 'm3' to be in the filtered results")
	}
}

// assertPanic is a test helper that fails the test if the provided function does not panic.
func assertPanic(t *testing.T, f func(), message string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error(message)
		}
	}()
	f()
}
