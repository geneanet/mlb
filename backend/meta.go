package backend

import (
	"encoding/json"

	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// Map
type MetaMap map[string]MetaBucket
type MetaBucket map[string]cty.Value

func (m MetaMap) Set(bucket string, key string, value cty.Value) {
	if _, ok := m[bucket]; !ok {
		m[bucket] = MetaBucket{}
	}
	m[bucket][key] = value
}

func (m MetaMap) Get(bucket string, key string) (cty.Value, bool) {
	if _, ok := m[bucket]; !ok {
		return cty.UnknownVal(cty.DynamicPseudoType), false
	}
	if _, ok := m[bucket][key]; !ok {
		return cty.UnknownVal(cty.DynamicPseudoType), false
	}
	return m[bucket][key], true
}

func (m MetaMap) Equal(other MetaMap) bool {
	if len(m) != len(other) {
		return false
	}

	for k, v := range m {
		other_v, ok := other[k]
		if !ok || !v.Equal(other_v) {
			return false
		}
	}

	return true
}

func (m MetaMap) Clone() MetaMap {
	new := make(MetaMap, len(m))
	for k, v := range m {
		new[k] = v.Clone()
	}
	return new
}

func (m MetaBucket) Equal(other MetaBucket) bool {
	if len(m) != len(other) {
		return false
	}

	for k, v := range m {
		other_v, ok := other[k]
		if !ok || v.Equals(other_v).False() {
			return false
		}
	}

	return true
}

func (m MetaBucket) Clone() MetaBucket {
	new := make(MetaBucket, len(m))
	for k, v := range m {
		new[k] = v
	}
	return new
}

func (m MetaBucket) MarshalJSON() ([]byte, error) {
	b := map[string]ctyjson.SimpleJSONValue{}

	for k, v := range m {
		b[k] = ctyjson.SimpleJSONValue{Value: v}
	}

	return json.Marshal(b)
}
