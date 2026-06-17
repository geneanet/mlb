package backends_processor

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/module"
	"sort"
	"strings"
	"sync"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zclconf/go-cty/cty"
)

func init() {
	module.RegisterFactory("backends_processor", "simple_filter", &SimpleFilterFactory{})
}

// SimpleFilter implements a backend processor that filters backends based on a condition,
// and can optionally sort and limit the resulting list.
type SimpleFilter struct {
	id          string
	backends    *backend.Registry           // Registry used for subscription management and random access
	allMatched  map[string]*backend.Backend // Internal cache of all backends passing the condition
	sortedList  []*backend.Backend          // Cache of the current top-N backends in their sort order
	listMu      sync.RWMutex                // Protects sortedList
	log         zerolog.Logger
	updChan     chan backend.BackendUpdate
	updChanStop chan struct{}
	source      string
	condition   hcl.Expression
	sortBy      hcl.Expression
	sortOrder   string
	limit       int
	evalCtx     *hcl.EvalContext
}

// SimpleFilterConfig represents the HCL configuration for the simple_filter processor.
type SimpleFilterConfig struct {
	ID        string         `hcl:"id,label"`
	Source    string         `hcl:"source"`
	Condition hcl.Expression `hcl:"condition"`           // Boolean expression evaluated for each backend
	SortBy    hcl.Expression `hcl:"sort_by,optional"`    // Optional expression to sort by (evaluated per backend)
	SortOrder *string        `hcl:"sort_order,optional"` // "asc" (default) or "desc"
	Limit     *int           `hcl:"limit,optional"`      // Optional limit on the number of backends
}

type SimpleFilterFactory struct{}

func (w SimpleFilterFactory) ValidateConfig(tc *module.Config) hcl.Diagnostics {
	config := &SimpleFilterConfig{}
	return gohcl.DecodeBody(tc.Config, tc.Ctx, config)
}

func (w SimpleFilterFactory) parseConfig(tc *module.Config) *SimpleFilterConfig {
	config := &SimpleFilterConfig{}
	if diags := gohcl.DecodeBody(tc.Config, tc.Ctx, config); diags.HasErrors() {
		log.Error().Err(diags).Msg("failed to decode simple filter backend processor config")
	}
	config.ID = fmt.Sprintf("backends_processor.%s.%s", tc.Type, tc.Name)
	return config
}

func (w SimpleFilterFactory) New(tc *module.Config, wg *sync.WaitGroup, ctx context.Context) module.Module {
	config := w.parseConfig(tc)

	sortOrder := "asc"
	if config.SortOrder != nil {
		sortOrder = strings.ToLower(*config.SortOrder)
	}

	limit := 0
	if config.Limit != nil {
		limit = *config.Limit
	}

	f := &SimpleFilter{
		id:          config.ID,
		backends:    backend.NewRegistry(),
		allMatched:  make(map[string]*backend.Backend),
		log:         log.With().Str("id", config.ID).Logger(),
		updChan:     make(chan backend.BackendUpdate, 100),
		updChanStop: make(chan struct{}),
		source:      config.Source,
		condition:   config.Condition,
		sortBy:      config.SortBy,
		sortOrder:   sortOrder,
		limit:       limit,
		evalCtx:     tc.Ctx,
	}

	ctx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	f.log.Info().Msg("Filter starting")

	go func() {
		defer wg.Done()
		defer f.log.Info().Msg("Filter stopped")
		defer cancel()
		defer close(f.updChanStop)

	mainloop:
		for {
			select {
			case upd := <-f.updChan: // Backend changed
				changed := false
				switch upd.Kind {
				case backend.UpdBackendAdded, backend.UpdBackendModified:
					if f.matchFilter(upd.Backend) {
						f.allMatched[upd.Address] = upd.Backend.Clone()
						changed = true
					} else {
						if _, ok := f.allMatched[upd.Address]; ok {
							delete(f.allMatched, upd.Address)
							changed = true
						}
					}
				case backend.UpdBackendRemoved:
					if _, ok := f.allMatched[upd.Address]; ok {
						delete(f.allMatched, upd.Address)
						changed = true
					}
				}

				if changed {
					f.refreshBackends()
				}
			case <-ctx.Done(): // Context cancelled
				break mainloop
			}
		}
	}()

	return f
}

func (f *SimpleFilter) ProvideUpdates(s backend.BackendUpdateSubscriber) {
	f.backends.Subscribe(s)

	f.listMu.RLock()
	list := make([]*backend.Backend, len(f.sortedList))
	copy(list, f.sortedList)
	f.listMu.RUnlock()

	for _, b := range list {
		s.ReceiveUpdate(backend.BackendUpdate{
			Kind:    backend.UpdBackendAdded,
			Address: b.Address,
			Backend: b,
		})
	}
}

func (f *SimpleFilter) matchFilter(b *backend.Backend) bool {
	var condition bool
	known, diags := b.ResolveExpression(f.condition, f.evalCtx, &condition)
	if diags.HasErrors() {
		f.log.Error().Msg(diags.Error())
		return false
	}
	return known && condition
}

// refreshBackends re-evaluates the sorting and limiting on all backends that currently pass
// the condition, and updates the public registry accordingly.
func (f *SimpleFilter) refreshBackends() {
	type sortable struct {
		backend *backend.Backend
		val     cty.Value
	}

	var list []sortable
	for _, b := range f.allMatched {
		val := cty.StringVal(b.Address) // Default sort value is address
		if f.sortBy != nil {
			var sortVal cty.Value
			_, diags := b.ResolveExpression(f.sortBy, f.evalCtx, &sortVal)
			if diags.HasErrors() {
				f.log.Error().Msg(diags.Error())
			} else if !sortVal.IsNull() {
				val = sortVal
			}
		}
		list = append(list, sortable{backend: b, val: val})
	}

	// Sort based on the evaluated expression values
	sort.Slice(list, func(i, j int) bool {
		cmp := compareCtyValues(list[i].val, list[j].val)
		if cmp != 0 {
			if f.sortOrder == "desc" {
				return cmp > 0
			}
			return cmp < 0
		}
		// Tie-break with address for deterministic results
		return list[i].backend.Address < list[j].backend.Address
	})

	// Apply limit if specified
	if f.limit > 0 && len(list) > f.limit {
		list = list[:f.limit]
	}

	newBackends := make(map[string]*backend.Backend)
	newSortedList := make([]*backend.Backend, len(list))
	for i, s := range list {
		newBackends[s.backend.Address] = s.backend
		newSortedList[i] = s.backend
	}

	// Update the cached sorted list
	f.listMu.Lock()
	f.sortedList = newSortedList
	f.listMu.Unlock()

	// Reconcile public registry:
	// 1. Remove backends that are no longer in the top-N list
	for _, address := range f.backends.GetList().Addresses() {
		if _, ok := newBackends[address]; !ok {
			f.backends.Remove(address)
			f.backends.Publish(backend.BackendUpdate{
				Kind:    backend.UpdBackendRemoved,
				Address: address,
			})
		}
	}

	// 2. Add or update backends that are in the top-N list
	for _, s := range list {
		existing := f.backends.Get(s.backend.Address)
		if existing == nil {
			f.backends.Add(s.backend.Clone())
			f.backends.Publish(backend.BackendUpdate{
				Kind:    backend.UpdBackendAdded,
				Address: s.backend.Address,
				Backend: f.backends.Get(s.backend.Address),
			})
		} else if !existing.Equal(s.backend) {
			f.backends.Update(s.backend.Clone())
			f.backends.Publish(backend.BackendUpdate{
				Kind:    backend.UpdBackendModified,
				Address: s.backend.Address,
				Backend: f.backends.Get(s.backend.Address),
			})
		}
	}
}

// compareCtyValues returns -1 if a < b, 1 if a > b, and 0 if a == b.
// It supports strings, numbers, and booleans.
func compareCtyValues(a, b cty.Value) int {
	if a.Type() != b.Type() {
		// ponytail: unstable sort if types differ, but ResolveExpression usually returns same type for same expression
		return strings.Compare(a.Type().FriendlyName(), b.Type().FriendlyName())
	}

	switch a.Type() {
	case cty.String:
		return strings.Compare(a.AsString(), b.AsString())
	case cty.Number:
		if a.LessThan(b).True() {
			return -1
		}
		if a.GreaterThan(b).True() {
			return 1
		}
		return 0
	case cty.Bool:
		av := a.True()
		bv := b.True()
		if av == bv {
			return 0
		}
		if !av && bv {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func (f *SimpleFilter) ReceiveUpdate(upd backend.BackendUpdate) {
	select {
	case f.updChan <- upd:
	case <-f.updChanStop:
	}
}

func (f *SimpleFilter) SubscribeTo(bup backend.BackendUpdateProvider) {
	bup.ProvideUpdates(f)
}

func (f *SimpleFilter) GetUpdateSource() string {
	return f.source
}

func (f *SimpleFilter) GetID() string {
	return f.id
}

func (f *SimpleFilter) GetBackendList() []*backend.Backend {
	f.listMu.RLock()
	defer f.listMu.RUnlock()
	list := make([]*backend.Backend, len(f.sortedList))
	copy(list, f.sortedList)
	return list
}

func (f *SimpleFilter) Bind(modules module.ModulesRegistry) {
	f.SubscribeTo(module.Get[backend.BackendUpdateProvider](modules, f.source))
}
