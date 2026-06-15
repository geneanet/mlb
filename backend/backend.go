package backend

import (
	"context"
	"maps"
	"slices"
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
	var metaCtx *hcl.EvalContext

	if ctx != nil {
		metaCtx = ctx.NewChild()
	} else {
		metaCtx = &hcl.EvalContext{}
	}

	metaCtx.Variables = map[string]cty.Value{
		"backend": cty.ObjectVal(map[string]cty.Value{
			"meta":    b.Meta.ToCtyObject(),
			"address": cty.StringVal(b.Address),
		}),
	}

	w, diags := expression.Value(metaCtx)

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

// Registry implements a thread-safe store for backends and a publisher for updates.
type Registry struct {
	backends    map[string]*Backend
	subscribers []BackendUpdateSubscriber
	mu          sync.RWMutex
	waitChan    chan struct{}
	isBlocked   bool
}

func NewRegistry() *Registry {
	return &Registry{
		backends:  make(map[string]*Backend),
		waitChan:  make(chan struct{}),
		isBlocked: true,
	}
}

func (r *Registry) updateWaitState() {
	if len(r.backends) > 0 && r.isBlocked {
		close(r.waitChan)
		r.isBlocked = false
	} else if len(r.backends) == 0 && !r.isBlocked {
		r.waitChan = make(chan struct{})
		r.isBlocked = true
	}
}

func (r *Registry) Wait(ctx context.Context) error {
	r.mu.RLock()
	if !r.isBlocked {
		r.mu.RUnlock()
		return nil
	}
	waitChan := r.waitChan
	r.mu.RUnlock()

	select {
	case <-waitChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Registry) Get(address string) *Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backends[address]
}

func (r *Registry) GetList() BackendsList {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Collect(maps.Values(r.backends))
}

func (r *Registry) GetSortedList() BackendsList {
	backends := r.GetList()
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].Address < backends[j].Address
	})
	return backends
}

func (r *Registry) Add(b *Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backends[b.Address] = b
	r.updateWaitState()
}

func (r *Registry) Update(b *Backend, exceptMeta ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.backends[b.Address]; ok {
		if existing == b {
			return
		}
		existing.Meta.Update(b.Meta, exceptMeta...)
	} else {
		r.backends[b.Address] = b
	}
	r.updateWaitState()
}

func (r *Registry) Remove(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.backends, address)
	r.updateWaitState()
}

func (r *Registry) Has(address string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.backends[address]
	return ok
}

func (r *Registry) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.backends)
}

func (r *Registry) Subscribe(s BackendUpdateSubscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscribers = append(r.subscribers, s)
}

func (r *Registry) ProvideUpdates(s BackendUpdateSubscriber) {
	r.mu.Lock()
	r.subscribers = append(r.subscribers, s)
	list := slices.Collect(maps.Values(r.backends))
	r.mu.Unlock()

	for _, b := range list {
		s.ReceiveUpdate(BackendUpdate{
			Kind:    UpdBackendAdded,
			Address: b.Address,
			Backend: b,
		})
	}
}

func (r *Registry) Publish(u BackendUpdate) {
	r.mu.RLock()
	subs := slices.Clone(r.subscribers)
	r.mu.RUnlock()

	for _, s := range subs {
		s.ReceiveUpdate(u)
	}
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
