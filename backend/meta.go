package backend

import "strconv"

// Type
type MetaType int

const (
	MetaTypeString MetaType = iota
	MetaTypeBool
)

// Value
type MetaValue interface {
	Type() MetaType
	Equal(MetaValue) bool
	ToString() (string, error)
	ToInt() (int64, error)
	ToFloat() (float64, error)
	ToBool() (bool, error)
	Copy() MetaValue
}

// Map
type MetaMap map[string]MetaValue

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

func (m MetaMap) Copy() MetaMap {
	new := make(MetaMap, len(m))
	for k, v := range m {
		new[k] = v.Copy()
	}
	return new
}

// String Value
type MetaStringValue struct {
	Value string
}

func (v MetaStringValue) ToString() (string, error) {
	return v.Value, nil
}

func (v MetaStringValue) ToInt() (int64, error) {
	return strconv.ParseInt(v.Value, 10, 64)
}

func (v MetaStringValue) ToFloat() (float64, error) {
	return strconv.ParseFloat(v.Value, 64)
}

func (v MetaStringValue) ToBool() (bool, error) {
	return strconv.ParseBool(v.Value)
}

func (v MetaStringValue) Type() MetaType {
	return MetaTypeString
}

func (v MetaStringValue) Equal(m MetaValue) bool {
	if m_value, err := m.ToString(); m_value == v.Value && err != nil {
		return true
	} else {
		return false
	}
}

func (v MetaStringValue) Copy() MetaValue {
	return MetaStringValue{
		Value: v.Value,
	}
}

// Bool Value
type MetaBoolValue struct {
	Value bool
}

func (v MetaBoolValue) ToString() (string, error) {
	if v.Value {
		return "true", nil
	} else {
		return "false", nil
	}
}

func (v MetaBoolValue) ToInt() (int64, error) {
	if v.Value {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (v MetaBoolValue) ToFloat() (float64, error) {
	if v.Value {
		return 1, nil
	} else {
		return 0, nil
	}
}

func (v MetaBoolValue) ToBool() (bool, error) {
	return v.Value, nil
}

func (v MetaBoolValue) Type() MetaType {
	return MetaTypeBool
}

func (v MetaBoolValue) Equal(m MetaValue) bool {
	if m_value, err := m.ToBool(); m_value == v.Value && err != nil {
		return true
	} else {
		return false
	}
}
func (v MetaBoolValue) Copy() MetaValue {
	return MetaBoolValue{
		Value: v.Value,
	}
}
