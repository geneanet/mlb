package backend

import (
	"encoding/json"
	"testing"

	"github.com/zclconf/go-cty/cty"
)

func TestMetaMap_New(t *testing.T) {
	m := NewEmptyMetaMap(10)
	if m == nil {
		t.Fatal("expected NewEmptyMetaMap to return a non-nil object")
	}

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

func TestMetaMap_SetGet(t *testing.T) {
	m := NewEmptyMetaMap(0)
	
	m.Set("test", "foo", cty.StringVal("bar"))
	
	val, ok := m.Get("test", "foo")
	if !ok {
		t.Errorf("expected to find foo in test bucket")
	}
	if val.AsString() != "bar" {
		t.Errorf("expected foo=bar, got %v", val.AsString())
	}
	
	_, ok = m.Get("unknown", "foo")
	if ok {
		t.Errorf("expected not to find foo in unknown bucket")
	}
	
	_, ok = m.Get("test", "unknown")
	if ok {
		t.Errorf("expected not to find unknown key in test bucket")
	}
}

func TestMetaMap_Update(t *testing.T) {
	m := NewEmptyMetaMap(0)
	m.Set("b1", "k1", cty.StringVal("v1"))
	m.Set("b2", "k2", cty.StringVal("v2"))
	
	m2 := NewEmptyMetaMap(0)
	m2.Set("b1", "k1", cty.StringVal("new_v1"))
	m2.Set("b3", "k3", cty.StringVal("v3"))
	
	m.Update(m2, "b2") // update with m2, but preserve b2 from m
	
	if val, ok := m.Get("b1", "k1"); !ok || val.AsString() != "new_v1" {
		t.Errorf("expected b1.k1 to be updated to new_v1")
	}
	if val, ok := m.Get("b2", "k2"); !ok || val.AsString() != "v2" {
		t.Errorf("expected b2.k2 to be preserved as v2")
	}
	if val, ok := m.Get("b3", "k3"); !ok || val.AsString() != "v3" {
		t.Errorf("expected b3.k3 to be added as v3")
	}
}

func TestMetaMap_Equal(t *testing.T) {
	m1 := NewEmptyMetaMap(0)
	m1.Set("b1", "k1", cty.StringVal("v1"))
	
	m2 := NewEmptyMetaMap(0)
	m2.Set("b1", "k1", cty.StringVal("v1"))
	
	if !m1.Equal(m2) {
		t.Errorf("expected m1 to equal m2")
	}
	
	m2.Set("b1", "k2", cty.StringVal("v2"))
	if m1.Equal(m2) {
		t.Errorf("expected m1 not to equal m2")
	}
	
	m3 := NewEmptyMetaMap(0)
	m3.Set("b2", "k1", cty.StringVal("v1"))
	if m1.Equal(m3) {
		t.Errorf("expected m1 not to equal m3")
	}

	m4 := NewEmptyMetaMap(0)
	m4.Set("b1", "k2", cty.StringVal("v1"))
	if m1.Equal(m4) {
		t.Errorf("expected m1 not to equal m4 due to missing bucket key")
	}

	m5 := NewEmptyMetaMap(0)
	m5.Set("b1", "k1", cty.StringVal("v1"))
	m5.Set("b2", "k2", cty.StringVal("v2"))
	if m1.Equal(m5) {
		t.Errorf("expected m1 not to equal m5 due to different lengths")
	}
}

func TestMetaMap_Clone(t *testing.T) {
	m1 := NewEmptyMetaMap(0)
	m1.Set("b1", "k1", cty.StringVal("v1"))
	
	m2 := m1.Clone()
	if !m1.Equal(m2) {
		t.Errorf("expected clone to be equal to original")
	}
	
	m2.Set("b1", "k2", cty.StringVal("v2"))
	if m1.Equal(m2) {
		t.Errorf("expected clone to be independent")
	}
}

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
