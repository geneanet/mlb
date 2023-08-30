package backend

import (
	"encoding/json"
	"sync"

	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// Map

type MetaMap struct {
	data  map[string]MetaBucket
	mutex sync.RWMutex
}

func NewEmptyMetaMap(size int) *MetaMap {
	return &MetaMap{
		data: make(map[string]MetaBucket, size),
	}
}

func NewMetaMap(data map[string]MetaBucket) *MetaMap {
	m := NewEmptyMetaMap(len(data))

	for k, v := range data {
		m.data[k] = v.Clone()
	}

	return m
}

func (m *MetaMap) Set(bucket string, key string, value cty.Value) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.data[bucket]; !ok {
		m.data[bucket] = MetaBucket{}
	}
	m.data[bucket][key] = value
}

// Replace all the metadata with the provided ones, except for the specified bucket that is preserved
func (m *MetaMap) Update(source *MetaMap, except ...string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	new := source.Clone()

	for _, k := range except {
		if v, ok := m.data[k]; ok {
			new.data[k] = v.Clone()
		}
	}

	m.data = new.data
}

func (m *MetaMap) Get(bucket string, key string) (cty.Value, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if _, ok := m.data[bucket]; !ok {
		return cty.UnknownVal(cty.DynamicPseudoType), false
	}
	if _, ok := m.data[bucket][key]; !ok {
		return cty.UnknownVal(cty.DynamicPseudoType), false
	}
	return m.data[bucket][key], true
}

func (m *MetaMap) ToCtyObject() cty.Value {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	buckets := map[string]cty.Value{}

	for k, v := range m.data {
		buckets[k] = cty.ObjectVal(v)
	}

	return cty.ObjectVal(buckets)
}

func (m *MetaMap) Equal(other *MetaMap) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	if len(m.data) != len(other.data) {
		return false
	}

	for k, v := range m.data {
		other_v, ok := other.data[k]
		if !ok || !v.Equal(other_v) {
			return false
		}
	}

	return true
}

func (m *MetaMap) Clone() *MetaMap {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	new := NewEmptyMetaMap(len(m.data))
	for k, v := range m.data {
		new.data[k] = v.Clone()
	}
	return new
}

// Bucket

type MetaBucket map[string]cty.Value

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
