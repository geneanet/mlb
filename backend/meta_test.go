package backend

import (
	"encoding/json"
	"testing"

	"github.com/zclconf/go-cty/cty"
)

// TestMetaMap_New tests the creation of MetaMap objects.
// It verifies that both NewEmptyMetaMap and NewMetaMap return valid, initialized structures.
func TestMetaMap_New(t *testing.T) {
	// Test creating an empty meta map with a specific capacity.
	m := NewEmptyMetaMap(10)
	if m == nil {
		t.Fatal("expected NewEmptyMetaMap to return a non-nil object")
	}

	// Test creating a meta map with initial data.
	data := map[string]MetaBucket{
		"default": {
			"key1": cty.StringVal("value1"),
		},
	}
	m2 := NewMetaMap(data)
	if m2 == nil {
		t.Fatal("expected NewMetaMap to return a non-nil object")
	}
	if val, ok := m2.Get("default", "key1"); !ok || val.AsString() != "value1" {
		t.Errorf("expected to find key1=value1 in default bucket")
	}
}

// TestMetaMap_SetGet tests the basic Get and Set operations for a MetaMap.
// It ensures that values can be stored and retrieved from specific buckets.
func TestMetaMap_SetGet(t *testing.T) {
	m := NewEmptyMetaMap(0)

	// Set a key-value pair in a specific bucket.
	m.Set("test", "foo", cty.StringVal("bar"))

	// Retrieve the value and verify it matches.
	val, ok := m.Get("test", "foo")
	if !ok {
		t.Errorf("expected to find foo in test bucket")
	}
	if val.AsString() != "bar" {
		t.Errorf("expected foo=bar, got %v", val.AsString())
	}

	// Verify that retrieving from an unknown bucket returns false.
	_, ok = m.Get("unknown", "foo")
	if ok {
		t.Errorf("expected not to find foo in unknown bucket")
	}

	// Verify that retrieving an unknown key from an existing bucket returns false.
	_, ok = m.Get("test", "unknown")
	if ok {
		t.Errorf("expected not to find unknown key in test bucket")
	}
}

// TestMetaMap_Update tests the conditional update of a MetaMap.
// It verifies that data from another MetaMap can be merged while excluding specific buckets.
func TestMetaMap_Update(t *testing.T) {
	m := NewEmptyMetaMap(0)
	m.Set("b1", "k1", cty.StringVal("v1"))
	m.Set("b2", "k2", cty.StringVal("v2"))

	m2 := NewEmptyMetaMap(0)
	m2.Set("b1", "k1", cty.StringVal("new_v1"))
	m2.Set("b3", "k3", cty.StringVal("v3"))

	// Update m with values from m2, preserving bucket "b2".
	m.Update(m2, "b2")

	// Check if "b1.k1" was updated.
	if val, ok := m.Get("b1", "k1"); !ok || val.AsString() != "new_v1" {
		t.Errorf("expected b1.k1 to be updated to new_v1")
	}
	// Check if "b2.k2" was preserved as requested.
	if val, ok := m.Get("b2", "k2"); !ok || val.AsString() != "v2" {
		t.Errorf("expected b2.k2 to be preserved as v2")
	}
	// Check if "b3.k3" was added.
	if val, ok := m.Get("b3", "k3"); !ok || val.AsString() != "v3" {
		t.Errorf("expected b3.k3 to be added as v3")
	}
}

// TestMetaMap_Equal tests the equality check between two MetaMap objects.
// It covers cases with matching values, mismatched values, and different lengths.
func TestMetaMap_Equal(t *testing.T) {
	m1 := NewEmptyMetaMap(0)
	m1.Set("b1", "k1", cty.StringVal("v1"))

	m2 := NewEmptyMetaMap(0)
	m2.Set("b1", "k1", cty.StringVal("v1"))

	// Equal maps.
	if !m1.Equal(m2) {
		t.Errorf("expected m1 to equal m2")
	}

	// Maps with different values in the same key.
	m2.Set("b1", "k2", cty.StringVal("v2"))
	if m1.Equal(m2) {
		t.Errorf("expected m1 not to equal m2")
	}

	// Maps with different bucket names.
	m3 := NewEmptyMetaMap(0)
	m3.Set("b2", "k1", cty.StringVal("v1"))
	if m1.Equal(m3) {
		t.Errorf("expected m1 not to equal m3")
	}

	// Maps with mismatched keys within buckets.
	m4 := NewEmptyMetaMap(0)
	m4.Set("b1", "k2", cty.StringVal("v1"))
	if m1.Equal(m4) {
		t.Errorf("expected m1 not to equal m4 due to missing bucket key")
	}

	// Maps with different numbers of buckets.
	m5 := NewEmptyMetaMap(0)
	m5.Set("b1", "k1", cty.StringVal("v1"))
	m5.Set("b2", "k2", cty.StringVal("v2"))
	if m1.Equal(m5) {
		t.Errorf("expected m1 not to equal m5 due to different lengths")
	}
}

// TestMetaMap_Clone tests the deep copy functionality of a MetaMap.
// It ensures that modifying a clone does not affect the original object.
func TestMetaMap_Clone(t *testing.T) {
	m1 := NewEmptyMetaMap(0)
	m1.Set("b1", "k1", cty.StringVal("v1"))

	m2 := m1.Clone()
	if !m1.Equal(m2) {
		t.Errorf("expected clone to be equal to original")
	}

	// Verify independence by modifying the clone.
	m2.Set("b1", "k2", cty.StringVal("v2"))
	if m1.Equal(m2) {
		t.Errorf("expected clone to be independent")
	}
}

// TestMetaMap_MarshalJSON tests the JSON serialization of a MetaMap.
// It ensures that the MetaMap correctly implements the json.Marshaler interface.
func TestMetaMap_MarshalJSON(t *testing.T) {
	m := NewEmptyMetaMap(0)
	m.Set("b1", "k1", cty.StringVal("v1"))

	data, err := m.MarshalJSON()
	if err != nil {
		t.Fatalf("unexpected error marshaling JSON: %v", err)
	}

	var unmarshaled map[string]interface{}
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("unexpected error unmarshaling JSON: %v", err)
	}

	if unmarshaled["b1"] == nil {
		t.Fatalf("expected b1 in JSON")
	}
}

// TestMetaBucket_Equal tests equality between individual MetaBucket objects.
// It covers matching, mismatched values, and different lengths.
func TestMetaBucket_Equal(t *testing.T) {
	mb1 := MetaBucket{"k1": cty.StringVal("v1")}
	mb2 := MetaBucket{"k1": cty.StringVal("v1")}
	mb3 := MetaBucket{"k1": cty.StringVal("v2")}
	mb4 := MetaBucket{"k1": cty.StringVal("v1"), "k2": cty.StringVal("v2")}

	if !mb1.Equal(mb2) {
		t.Errorf("expected mb1 to equal mb2")
	}
	if mb1.Equal(mb3) {
		t.Errorf("expected mb1 not to equal mb3")
	}
	if mb1.Equal(mb4) {
		t.Errorf("expected mb1 not to equal mb4")
	}
}

// TestMetaBucket_Clone tests the deep copy functionality of a MetaBucket.
// It ensures independence between the original and the clone.
func TestMetaBucket_Clone(t *testing.T) {
	mb1 := MetaBucket{"k1": cty.StringVal("v1")}
	mb2 := mb1.Clone()

	if !mb1.Equal(mb2) {
		t.Errorf("expected clone to be equal to original")
	}

	mb2["k2"] = cty.StringVal("v2")
	if mb1.Equal(mb2) {
		t.Errorf("expected clone to be independent")
	}
}
