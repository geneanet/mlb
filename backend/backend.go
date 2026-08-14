package backend

import (
	"context"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/rs/zerolog"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"
)

// Backend represents a single backend server with its metadata and lifecycle.
type Backend struct {
	Address string             `json:"address"`
	Meta    *MetaMap           `json:"meta"`
	Ctx     context.Context    `json:"-"`
	Cancel  context.CancelFunc `json:"-"`
}

// Clone creates a deep copy of the Backend.
func (b *Backend) Clone() *Backend {
	new := &Backend{
		Address: b.Address,
		Meta:    b.Meta.Clone(),
		Ctx:     b.Ctx,
		Cancel:  b.Cancel,
	}
	return new
}

// Equal checks if two backends are equal based on address and metadata.
func (b *Backend) Equal(other *Backend) bool {
	return b.Address == other.Address && b.Meta.Equal(other.Meta)
}

// ResolveExpression evaluates an HCL expression in the context of this backend.
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
	log         zerolog.Logger
	logUpdates  bool
}

// NewRegistry creates a new empty Registry.
func NewRegistry(log zerolog.Logger, logUpdates bool) *Registry {
	return &Registry{
		backends:   make(map[string]*Backend),
		waitChan:   make(chan struct{}),
		log:        log,
		logUpdates: logUpdates,
	}
}

// updateWaitState updates the wait channel state based on whether backends are present.
func (r *Registry) updateWaitState() {
	needsWait := len(r.backends) == 0
	if needsWait == (r.waitChan != nil) {
		return
	}
	if needsWait {
		r.waitChan = make(chan struct{})
	} else {
		close(r.waitChan)
		r.waitChan = nil
	}
}

// Wait blocks until at least one backend is available or the context is cancelled.
func (r *Registry) Wait(ctx context.Context) error {
	r.mu.RLock()
	ch := r.waitChan
	r.mu.RUnlock()
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Get retrieves a backend by its address.
func (r *Registry) Get(address string) *Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backends[address]
}

// GetList returns a slice containing all current backends.
func (r *Registry) GetList() BackendsList {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Collect(maps.Values(r.backends))
}

// GetSortedList returns a slice containing all current backends sorted by address.
func (r *Registry) GetSortedList() BackendsList {
	backends := r.GetList()
	sort.Slice(backends, func(i, j int) bool {
		return backends[i].Address < backends[j].Address
	})
	return backends
}

// Add adds a backend to the registry.
func (r *Registry) Add(b *Backend) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.log.Debug().Str("address", b.Address).Msg("Backend added to registry")

	if _, ok := r.backends[b.Address]; !ok && r.logUpdates {
		r.log.Info().Str("address", b.Address).Msg("Backend added")
	}

	r.backends[b.Address] = b
	r.updateWaitState()
}

// Update updates an existing backend or adds it if it doesn't exist.
func (r *Registry) Update(b *Backend, exceptMeta ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.log.Debug().Str("address", b.Address).Msg("Backend updated in registry")

	if existing, ok := r.backends[b.Address]; ok {
		if existing == b {
			return
		}
		existing.Meta.Update(b.Meta, exceptMeta...)
	} else {
		if r.logUpdates {
			r.log.Info().Str("address", b.Address).Msg("Backend added")
		}
		r.backends[b.Address] = b
	}
	r.updateWaitState()
}

// Remove removes a backend from the registry by its address.
func (r *Registry) Remove(address string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.log.Debug().Str("address", address).Msg("Backend removed from registry")

	if _, ok := r.backends[address]; ok {
		if r.logUpdates {
			r.log.Info().Str("address", address).Msg("Backend removed")
		}
		delete(r.backends, address)
		r.updateWaitState()
	}
}

// Has checks if a backend with the given address exists in the registry.
func (r *Registry) Has(address string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.backends[address]
	return ok
}

// Subscribe registers a new subscriber for backend updates.
func (r *Registry) Subscribe(s BackendUpdateSubscriber) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subscribers = append(r.subscribers, s)
}

// ProvideUpdates registers a subscriber and immediately provides it with the current list of backends.
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

// Publish sends a backend update to all registered subscribers.
func (r *Registry) Publish(u BackendUpdate) {
	r.mu.RLock()
	subs := slices.Clone(r.subscribers)
	r.mu.RUnlock()

	for _, s := range subs {
		s.ReceiveUpdate(u)
	}
}

// BackendsList is a slice of Backend pointers.
type BackendsList []*Backend

// Addresses returns a slice containing the addresses of all backends in the list.
func (l BackendsList) Addresses() []string {
	res := make([]string, len(l))
	for i, b := range l {
		res[i] = b.Address
	}
	return res
}

// BackendUpdate represents a change to a backend.
type BackendUpdate struct {
	Kind    BackendUpdateKind
	Address string
	Backend *Backend
}

// BackendUpdateKind defines the type of backend update.
type BackendUpdateKind int

const (
	// UpdBackendAdded indicates a new backend was added.
	UpdBackendAdded BackendUpdateKind = iota
	// UpdBackendModified indicates an existing backend was modified.
	UpdBackendModified
	// UpdBackendRemoved indicates a backend was removed.
	UpdBackendRemoved
)

// BackendUpdateProvider is the interface for components that provide backend updates.
type BackendUpdateProvider interface {
	ProvideUpdates(BackendUpdateSubscriber)
}

// BackendUpdateSubscriber is the interface for components that receive backend updates.
type BackendUpdateSubscriber interface {
	ReceiveUpdate(BackendUpdate)
}

// BackendProvider is the interface for components that can provide a single backend.
type BackendProvider interface {
	GetBackend(wait bool) (*Backend, func())
}

// BackendListProvider is the interface for components that can provide a list of backends.
type BackendListProvider interface {
	GetBackendList() []*Backend
}
