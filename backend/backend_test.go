package backend

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// TestBackend_Clone tests the deep copy functionality of the Backend structure.
// It ensures that modifications to a cloned backend do not propagate to the original.
func TestBackend_Clone(t *testing.T) {
	b1 := &Backend{
		Address: "127.0.0.1:8080",
		Meta:    NewEmptyMetaMap(0),
	}
	b1.Meta.Set("test", "foo", cty.StringVal("bar"))

	b2 := b1.Clone()

	// Verify equality of original and clone.
	if !b1.Equal(b2) {
		t.Errorf("expected clone to be equal to original")
	}

	// Verify independence of Address field.
	b2.Address = "127.0.0.1:8081"
	if b1.Equal(b2) {
		t.Errorf("expected clone to be independent")
	}

	// Verify independence of Meta field.
	b2.Address = "127.0.0.1:8080"
	b2.Meta.Set("test", "foo", cty.StringVal("baz"))
	if b1.Equal(b2) {
		t.Errorf("expected clone meta to be independent")
	}
}

// TestBackend_Equal tests the equality logic for Backend objects.
// It verifies comparison by address and metadata.
func TestBackend_Equal(t *testing.T) {
	b1 := &Backend{Address: "127.0.0.1", Meta: NewEmptyMetaMap(0)}
	b2 := &Backend{Address: "127.0.0.1", Meta: NewEmptyMetaMap(0)}
	b3 := &Backend{Address: "127.0.0.2", Meta: NewEmptyMetaMap(0)}

	if !b1.Equal(b2) {
		t.Errorf("expected b1 to equal b2")
	}
	if b1.Equal(b3) {
		t.Errorf("expected b1 not to equal b3")
	}
}

// TestBackend_ResolveExpression tests the evaluation of HCL expressions against backend metadata.
// It covers simple attribute access, nested metadata access, evaluation with custom contexts,
// and error handling for unknown values or type mismatches.
func TestBackend_ResolveExpression(t *testing.T) {
	b := &Backend{
		Address: "127.0.0.1:8080",
		Meta:    NewEmptyMetaMap(0),
	}
	b.Meta.Set("custom", "weight", cty.NumberIntVal(10))

	// Test: Simple expression referencing backend.address
	expr, diags := hclsyntax.ParseExpression([]byte(`backend.address == "127.0.0.1:8080"`), "", hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		t.Fatalf("failed to parse expression: %s", diags.Error())
	}

	var result bool
	known, diags := b.ResolveExpression(expr, nil, &result)
	if diags.HasErrors() {
		t.Fatalf("failed to resolve expression: %s", diags.Error())
	}
	if !known {
		t.Fatalf("expected expression to be known")
	}
	if !result {
		t.Errorf("expected expression to resolve to true")
	}

	// Test: Expression referencing nested backend.meta
	expr2, diags2 := hclsyntax.ParseExpression([]byte(`backend.meta.custom.weight > 5`), "", hcl.Pos{Line: 1, Column: 1})
	if diags2.HasErrors() {
		t.Fatalf("failed to parse expression 2: %s", diags2.Error())
	}

	var result2 bool
	known2, diags2 := b.ResolveExpression(expr2, nil, &result2)
	if diags2.HasErrors() {
		t.Fatalf("failed to resolve expression 2: %s", diags2.Error())
	}
	if !known2 {
		t.Fatalf("expected expression 2 to be known")
	}
	if !result2 {
		t.Errorf("expected expression 2 to resolve to true")
	}

	// Test: Expression evaluation with an existing external context
	ctx := &hcl.EvalContext{
		Variables: map[string]cty.Value{
			"other_var": cty.StringVal("test"),
		},
	}
	var resultCtx bool
	knownCtx, diagsCtx := b.ResolveExpression(expr, ctx, &resultCtx)
	if diagsCtx.HasErrors() {
		t.Fatalf("failed to resolve expression with Ctx: %s", diagsCtx.Error())
	}
	if !knownCtx || !resultCtx {
		t.Errorf("expected expression to resolve to true with ctx")
	}

	// Test: Unknown value handling
	exprUnknown, _ := hclsyntax.ParseExpression([]byte(`backend.meta.custom.unknown`), "", hcl.Pos{Line: 1, Column: 1})
	var resultUnknown bool
	knownUnknown, _ := b.ResolveExpression(exprUnknown, nil, &resultUnknown)
	if knownUnknown {
		t.Errorf("expected expression with unknown value to not be known")
	}

	// Test: Type conversion error handling
	var resultInt int
	exprString, _ := hclsyntax.ParseExpression([]byte(`backend.address`), "", hcl.Pos{Line: 1, Column: 1})
	knownTypeErr, diagsTypeErr := b.ResolveExpression(exprString, nil, &resultInt)
	if !knownTypeErr {
		t.Errorf("expected expression to be known for type err test")
	}
	if !diagsTypeErr.HasErrors() {
		t.Errorf("expected diagnostics errors due to type conversion")
	}
}

// TestRegistry_BasicOperations tests standard map-like operations for Registry.
// It covers adding, getting, existence checking, and removing backends.
func TestRegistry_BasicOperations(t *testing.T) {
	bm := NewRegistry()
	if len(bm.GetList()) != 0 {
		t.Errorf("expected empty map to have size 0, got %d", len(bm.GetList()))
	}

	b1 := &Backend{Address: "10.0.0.1", Meta: NewEmptyMetaMap(0)}
	b2 := &Backend{Address: "10.0.0.2", Meta: NewEmptyMetaMap(0)}

	// Test Add and Size
	bm.Add(b1)
	if !bm.Has("10.0.0.1") {
		t.Errorf("expected map to have 10.0.0.1")
	}
	if len(bm.GetList()) != 1 {
		t.Errorf("expected map to have size 1, got %d", len(bm.GetList()))
	}

	bm.Add(b2)
	if len(bm.GetList()) != 2 {
		t.Errorf("expected map to have size 2, got %d", len(bm.GetList()))
	}

	// Test Get
	gotB1 := bm.Get("10.0.0.1")
	if !reflect.DeepEqual(b1, gotB1) {
		t.Errorf("expected Get to return b1")
	}

	// Test Get for unknown key
	gotNil := bm.Get("unknown")
	if gotNil != nil {
		t.Errorf("expected Get for unknown to return nil")
	}

	// Test Remove
	bm.Remove("10.0.0.1")
	if bm.Has("10.0.0.1") {
		t.Errorf("expected map not to have 10.0.0.1 after remove")
	}
	if len(bm.GetList()) != 1 {
		t.Errorf("expected map to have size 1 after remove, got %d", len(bm.GetList()))
	}
}

// TestRegistry_Lists tests the retrieval of backend lists from Registry.
// It verifies that GetList returns all backends and GetSortedList returns them sorted by address.
func TestRegistry_Lists(t *testing.T) {
	bm := NewRegistry()
	b1 := &Backend{Address: "10.0.0.2", Meta: NewEmptyMetaMap(0)}
	b2 := &Backend{Address: "10.0.0.1", Meta: NewEmptyMetaMap(0)}

	bm.Add(b1)
	bm.Add(b2)

	// Verify unordered list length
	list := bm.GetList()
	if len(list) != 2 {
		t.Errorf("expected list to have 2 items, got %d", len(list))
	}

	// Verify sorted list order
	sorted := bm.GetSortedList()
	if len(sorted) != 2 {
		t.Errorf("expected sorted list to have 2 items, got %d", len(sorted))
	}
	if sorted[0].Address != "10.0.0.1" || sorted[1].Address != "10.0.0.2" {
		t.Errorf("expected sorted list to be sorted by address")
	}
}

// TestRegistry_Update tests the conditional update of backends in the map.
// it verifies that existing backends' metadata is merged and new backends are added.
func TestRegistry_Update(t *testing.T) {
	bm := NewRegistry()

	m1 := NewEmptyMetaMap(0)
	m1.Set("b1", "k1", cty.StringVal("v1"))
	b1 := &Backend{Address: "10.0.0.1", Meta: m1}
	bm.Add(b1)

	m2 := NewEmptyMetaMap(0)
	m2.Set("b1", "k1", cty.StringVal("new_v1"))
	m2.Set("b2", "k2", cty.StringVal("v2"))
	b2 := &Backend{Address: "10.0.0.1", Meta: m2}

	// Update existing backend 10.0.0.1 while preserving bucket "b1"
	bm.Update(b2, "b1")

	updatedB1 := bm.Get("10.0.0.1")
	if val, ok := updatedB1.Meta.Get("b1", "k1"); !ok || val.AsString() != "v1" {
		t.Errorf("expected b1.k1 to be preserved as v1, got %v", val)
	}
	if val, ok := updatedB1.Meta.Get("b2", "k2"); !ok || val.AsString() != "v2" {
		t.Errorf("expected b2.k2 to be added as v2")
	}

	// Test updating a non-existent backend adds it to the map
	b3 := &Backend{Address: "10.0.0.2", Meta: NewEmptyMetaMap(0)}
	bm.Update(b3)
	if !bm.Has("10.0.0.2") {
		t.Errorf("expected b3 to be added via Update")
	}

	// Test updating from itself (should be a no-op and not deadlock)
	bm.Update(b3)
}

type testSubscriber struct {
	updates []BackendUpdate
}

func (s *testSubscriber) ReceiveUpdate(u BackendUpdate) {
	s.updates = append(s.updates, u)
}
func (s *testSubscriber) SubscribeTo(p BackendUpdateProvider) { p.ProvideUpdates(s) }
func (s *testSubscriber) GetUpdateSource() string              { return "test" }

// TestRegistry_PublishSubscribe tests the Observer pattern implementation in Registry.
func TestRegistry_PublishSubscribe(t *testing.T) {
	reg := NewRegistry()
	b1 := &Backend{Address: "10.0.0.1", Meta: NewEmptyMetaMap(0)}
	reg.Add(b1)

	sub := &testSubscriber{}

	// Test ProvideUpdates: subscriber should receive current state
	reg.ProvideUpdates(sub)
	if len(sub.updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(sub.updates))
	}
	if sub.updates[0].Kind != UpdBackendAdded || sub.updates[0].Address != "10.0.0.1" {
		t.Errorf("unexpected initial update: %v", sub.updates[0])
	}

	// Test Publish: subscriber should receive new updates
	sub.updates = nil
	u := BackendUpdate{Kind: UpdBackendRemoved, Address: "10.0.0.1"}
	reg.Publish(u)
	if len(sub.updates) != 1 {
		t.Fatalf("expected 1 update after publish, got %d", len(sub.updates))
	}
	if sub.updates[0].Kind != UpdBackendRemoved || sub.updates[0].Address != "10.0.0.1" {
		t.Errorf("unexpected published update: %v", sub.updates[0])
	}

	// Test Subscribe (without initial state)
	sub2 := &testSubscriber{}
	reg.Subscribe(sub2)
	reg.Publish(u)
	if len(sub2.updates) != 1 {
		t.Errorf("sub2 should have received 1 update")
	}
}

// TestRegistry_Wait tests the blocking behavior of Wait method in Registry.
func TestRegistry_Wait(t *testing.T) {
	reg := NewRegistry()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// 1. Test timeout when empty
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	err := reg.Wait(timeoutCtx)
	timeoutCancel()
	if err == nil || err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	// 2. Test unblocking when adding a backend
	unblockChan := make(chan error, 1)
	go func() {
		unblockChan <- reg.Wait(ctx)
	}()

	time.Sleep(20 * time.Millisecond) // Ensure Wait is blocking
	reg.Add(&Backend{Address: "127.0.0.1", Meta: NewEmptyMetaMap(0)})

	select {
	case err := <-unblockChan:
		if err != nil {
			t.Errorf("expected nil error, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Wait did not unblock after Add")
	}

	// 3. Test immediate return when NOT empty
	err = reg.Wait(ctx)
	if err != nil {
		t.Errorf("expected immediate return (nil error) when not empty, got %v", err)
	}

	// 4. Test re-blocking after removal
	reg.Remove("127.0.0.1")
	timeoutCtx2, timeoutCancel2 := context.WithTimeout(ctx, 50*time.Millisecond)
	err = reg.Wait(timeoutCtx2)
	timeoutCancel2()
	if err == nil || err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded after removal, got %v", err)
	}
}

// TestMetaMap_Deadlocks tests that MetaMap operations do not deadlock when called with same object.
func TestMetaMap_Deadlocks(t *testing.T) {
	m1 := NewEmptyMetaMap(0)
	m1.Set("b1", "k1", cty.StringVal("v1"))

	// Test self-update
	m1.Update(m1)

	// Test self-equality
	if !m1.Equal(m1) {
		t.Errorf("expected m1 to equal itself")
	}
}

// TestBackendsList_Addresses tests the Addresses helper of BackendsList.
func TestBackendsList_Addresses(t *testing.T) {
	list := BackendsList{
		{Address: "10.0.0.1"},
		{Address: "10.0.0.2"},
	}
	addr := list.Addresses()
	expected := []string{"10.0.0.1", "10.0.0.2"}
	if !reflect.DeepEqual(addr, expected) {
		t.Errorf("expected %v, got %v", expected, addr)
	}
}
