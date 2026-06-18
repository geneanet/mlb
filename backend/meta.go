package backend

import (
	"encoding/json"
	"sync"

	"github.com/zclconf/go-cty/cty"
	ctyjson "github.com/zclconf/go-cty/cty/json"
)

// MetaMap is a thread-safe map of metadata buckets, where each bucket is a map of cty.Value.
type MetaMap struct {
	data  map[string]MetaBucket
	mutex sync.RWMutex
}

// NewEmptyMetaMap creates a new empty MetaMap with the specified initial capacity.
func NewEmptyMetaMap(size int) *MetaMap {
	return &MetaMap{
		data: make(map[string]MetaBucket, size),
	}
}

// NewMetaMap creates a new MetaMap from the provided data.
func NewMetaMap(data map[string]MetaBucket) *MetaMap {
	m := NewEmptyMetaMap(len(data))

	for k, v := range data {
		m.data[k] = v.Clone()
	}

	return m
}

// Set sets a value in a specific bucket and key.
func (m *MetaMap) Set(bucket string, key string, value cty.Value) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.data[bucket]; !ok {
		m.data[bucket] = MetaBucket{}
	}
	m.data[bucket][key] = value
}

// Update replaces all metadata with the provided ones, except for the specified buckets that are preserved.
func (m *MetaMap) Update(source *MetaMap, except ...string) {
	if m == source {
		return
	}
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

// Get retrieves a value from a specific bucket and key.
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

// ToCtyObject converts the MetaMap to a cty.Value object.
func (m *MetaMap) ToCtyObject() cty.Value {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	buckets := map[string]cty.Value{}

	for k, v := range m.data {
		buckets[k] = cty.ObjectVal(v)
	}

	return cty.ObjectVal(buckets)
}

// Equal checks if two MetaMaps are equal.
func (m *MetaMap) Equal(other *MetaMap) bool {
	if m == other {
		return true
	}
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	other.mutex.RLock()
	defer other.mutex.RUnlock()

	if len(m.data) != len(other.data) {
		return false
	}

	for k, v := range m.data {
		otherValue, ok := other.data[k]
		if !ok || !v.Equal(otherValue) {
			return false
		}
	}

	return true
}

// Clone creates a deep copy of the MetaMap.
func (m *MetaMap) Clone() *MetaMap {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	new := NewEmptyMetaMap(len(m.data))
	for k, v := range m.data {
		new.data[k] = v.Clone()
	}
	return new
}

// MarshalJSON implements the json.Marshaler interface.
func (m *MetaMap) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.data)
}

// MetaBucket is a map of cty.Value.
type MetaBucket map[string]cty.Value

// Equal checks if two MetaBuckets are equal.
func (m MetaBucket) Equal(other MetaBucket) bool {
	if len(m) != len(other) {
		return false
	}

	for k, v := range m {
		otherValue, ok := other[k]
		if !ok || v.Equals(otherValue).False() {
			return false
		}
	}

	return true
}

// Clone creates a deep copy of the MetaBucket.
func (m MetaBucket) Clone() MetaBucket {
	new := make(MetaBucket, len(m))
	for k, v := range m {
		new[k] = v
	}
	return new
}

// MarshalJSON implements the json.Marshaler interface.
func (m MetaBucket) MarshalJSON() ([]byte, error) {
	b := map[string]ctyjson.SimpleJSONValue{}

	for k, v := range m {
		b[k] = ctyjson.SimpleJSONValue{Value: v}
	}

	return json.Marshal(b)
}
