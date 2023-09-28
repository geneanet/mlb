package backend

import (
	"mlb/misc"
	"sort"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
)

// Backend
type Backend struct {
	Address string   `json:"address"`
	Meta    *MetaMap `json:"meta"`
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

func (b *Backend) ResolveExpression(expression hcl.Expression, ctx *hcl.EvalContext, target interface{}) (bool, hcl.Diagnostics) {
	var meta_ctx *hcl.EvalContext

	if ctx != nil {
		meta_ctx = ctx.NewChild()
	} else {
		meta_ctx = &hcl.EvalContext{}
	}

	meta_ctx.Variables = map[string]cty.Value{
		"backend": cty.ObjectVal(map[string]cty.Value{
			"meta":    b.Meta.ToCtyObject(),
			"address": cty.StringVal(b.Address),
		}),
	}

	w, diags := expression.Value(meta_ctx)

	if !w.IsKnown() {
		return false, diags
	}

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

	return true, diags
}

// Map
type BackendsMap struct {
	backends map[string]*Backend
	lock     sync.RWMutex
}

func NewBackendsMap() *BackendsMap {
	return &BackendsMap{
		backends: make(map[string]*Backend),
		lock:     sync.RWMutex{},
	}
}

func (bm *BackendsMap) Get(address string) *Backend {
	bm.lock.RLock()
	defer bm.lock.RUnlock()

	if b, ok := bm.backends[address]; ok {
		return b
	} else {
		return nil
	}
}

func (bm *BackendsMap) GetList() BackendsList {
	bm.lock.RLock()
	defer bm.lock.RUnlock()

	return misc.MapValues(bm.backends)
}

func (bm *BackendsMap) GetSortedList() BackendsList {
	backends := bm.GetList()
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].Address < backends[j].Address
	})
	return backends
}

func (bm *BackendsMap) Add(backend *Backend) {
	bm.lock.Lock()
	defer bm.lock.Unlock()

	bm.backends[backend.Address] = backend
}

func (bm *BackendsMap) Update(backend *Backend, except_meta ...string) {
	bm.lock.Lock()
	defer bm.lock.Unlock()

	if b, ok := bm.backends[backend.Address]; ok {
		b.Meta.Update(backend.Meta, except_meta...)
	} else {
		bm.backends[backend.Address] = backend
	}
}

func (bm *BackendsMap) Remove(address string) {
	bm.lock.Lock()
	defer bm.lock.Unlock()

	delete(bm.backends, address)
}

func (bm *BackendsMap) Has(address string) bool {
	bm.lock.RLock()
	defer bm.lock.RUnlock()

	_, ok := bm.backends[address]
	return ok
}

func (bm *BackendsMap) Size() int {
	bm.lock.RLock()
	defer bm.lock.RUnlock()

	return len(bm.backends)
}

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
	ProvideUpdates(BackendUpdateSubscriber)
}

type BackendUpdateSubscriber interface {
	SubscribeTo(BackendUpdateProvider)
	GetUpdateSource() string
	ReceiveUpdate(BackendUpdate)
}

type BackendProvider interface {
	GetBackend(wait bool) *Backend
}

type BackendListProvider interface {
	GetBackendList() []*Backend
}
