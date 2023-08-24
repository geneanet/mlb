package backend

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
	"github.com/zclconf/go-cty/cty/gocty"
)

// Backend
type Backend struct {
	Address string
	Meta    MetaMap
}

func (b *Backend) Clone() *Backend {
	new := &Backend{
		Address: b.Address,
		Meta:    b.Meta.Clone(),
	}
	return new
}

func (b *Backend) Equal(other *Backend) bool {
	return b.Address == other.Address && b.Meta.Equal(other.Meta)
}

// Replace all the metadata with the provided ones, except for the specified bucket that is preserved
func (b *Backend) UpdateMeta(new_meta MetaMap, except ...string) {
	new := new_meta.Clone()
	for _, k := range except {
		if v, ok := b.Meta[k]; ok {
			new[k] = v
		}
	}
	b.Meta = new
}

func (b *Backend) ResolveExpression(expression hcl.Expression, target interface{}) hcl.Diagnostics {
	meta_ctx := hcl.EvalContext{
		Variables: map[string]cty.Value{
			"backend": cty.ObjectVal(map[string]cty.Value{
				"meta":    b.Meta.ToCtyObject(),
				"address": cty.StringVal(b.Address),
			}),
		},
		Functions: map[string]function.Function{
			"contains": stdlib.ContainsFunc,
		},
	}

	w, diags := expression.Value(&meta_ctx)

	err := gocty.FromCtyValue(w, target)
	if err != nil {
		diags2 := hcl.Diagnostics{
			{
				Severity: hcl.DiagError,
				Summary:  "Type conversion error",
				Detail:   err.Error(),
				Subject:  expression.Range().Ptr(),
			},
		}
		diags = append(diags, diags2...)
	}

	return diags
}

// Map
type BackendsMap map[string]*Backend

// List
type BackendsList []*Backend

// Messages
type BackendUpdate struct {
	Kind    BackendUpdateKind
	Address string
	Backend *Backend
}

type BackendUpdateKind int

const (
	UpdBackendAdded BackendUpdateKind = iota
	UpdBackendModified
	UpdBackendRemoved
)

// Interfaces
type BackendUpdateProvider interface {
	ProvideUpdates(chan BackendUpdate)
}

type BackendUpdateSubscriber interface {
	SubscribeTo(BackendUpdateProvider)
	GetUpdateSource() string
}

type BackendProvider interface {
	GetBackend() *Backend
}

type BackendListProvider interface {
	GetBackendList() []*Backend
}
