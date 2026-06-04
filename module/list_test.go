package module

import (
	"mlb/backend"
	"testing"
)

// Mock module structures

type dummyModule struct {
	id string
}

func (d *dummyModule) GetID() string {
	return d.id
}

func (d *dummyModule) Bind(modules ModulesList) {}

type dummyUpdateProvider struct {
	dummyModule
}

func (d *dummyUpdateProvider) ProvideUpdates(sub backend.BackendUpdateSubscriber) {}

type dummyProvider struct {
	dummyModule
}

func (d *dummyProvider) GetBackend(wait bool) *backend.Backend {
	return nil
}

type dummyListProvider struct {
	dummyModule
}

func (d *dummyListProvider) GetBackendList() []*backend.Backend {
	return nil
}

// Tests

func TestNewModulesList(t *testing.T) {
	ml := NewModulesList()
	if ml == nil {
		t.Fatal("Expected ModulesList to not be nil")
	}
	if len(ml) != 0 {
		t.Errorf("Expected empty ModulesList")
	}
}

func TestAddModule(t *testing.T) {
	ml := NewModulesList()
	m := &dummyModule{id: "m1"}
	ml.AddModule(m)

	if len(ml) != 1 {
		t.Fatalf("Expected 1 module, got %d", len(ml))
	}

	if ml["m1"] != m {
		t.Errorf("Expected added module to be retrievable by its ID")
	}
}

func TestGetBackendUpdateProvider(t *testing.T) {
	ml := NewModulesList()
	m := &dummyUpdateProvider{dummyModule: dummyModule{id: "m1"}}
	ml.AddModule(m)

	bup := ml.GetBackendUpdateProvider("m1")
	if bup == nil {
		t.Errorf("Expected to retrieve a valid BackendUpdateProvider")
	}

	assertPanic(t, func() { ml.GetBackendUpdateProvider("missing") }, "Expected panic for missing module")

	mWrong := &dummyModule{id: "m2"}
	ml.AddModule(mWrong)
	assertPanic(t, func() { ml.GetBackendUpdateProvider("m2") }, "Expected panic for module not implementing interface")
}

func TestGetBackendProvider(t *testing.T) {
	ml := NewModulesList()
	m := &dummyProvider{dummyModule: dummyModule{id: "m1"}}
	ml.AddModule(m)

	bp := ml.GetBackendProvider("m1")
	if bp == nil {
		t.Errorf("Expected to retrieve a valid BackendProvider")
	}

	assertPanic(t, func() { ml.GetBackendProvider("missing") }, "Expected panic for missing module")

	mWrong := &dummyModule{id: "m2"}
	ml.AddModule(mWrong)
	assertPanic(t, func() { ml.GetBackendProvider("m2") }, "Expected panic for module not implementing interface")
}

func TestGetBackendListProvider(t *testing.T) {
	ml := NewModulesList()
	m := &dummyListProvider{dummyModule: dummyModule{id: "m1"}}
	ml.AddModule(m)

	blp := ml.GetBackendListProvider("m1")
	if blp == nil {
		t.Errorf("Expected to retrieve a valid BackendListProvider")
	}

	assertPanic(t, func() { ml.GetBackendListProvider("missing") }, "Expected panic for missing module")

	mWrong := &dummyModule{id: "m2"}
	ml.AddModule(mWrong)
	assertPanic(t, func() { ml.GetBackendListProvider("m2") }, "Expected panic for module not implementing interface")
}

func TestGetBackendListProviders(t *testing.T) {
	ml := NewModulesList()

	m1 := &dummyListProvider{dummyModule: dummyModule{id: "m1"}}
	m2 := &dummyModule{id: "m2"}
	m3 := &dummyListProvider{dummyModule: dummyModule{id: "m3"}}

	ml.AddModule(m1)
	ml.AddModule(m2)
	ml.AddModule(m3)

	providers := ml.GetBackendListProviders()

	if len(providers) != 2 {
		t.Fatalf("Expected exactly 2 providers, got %d", len(providers))
	}

	if _, ok := providers["m1"]; !ok {
		t.Errorf("Expected m1 to be in the list of providers")
	}
	if _, ok := providers["m3"]; !ok {
		t.Errorf("Expected m3 to be in the list of providers")
	}
}

func assertPanic(t *testing.T, f func(), message string) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error(message)
		}
	}()
	f()
}
