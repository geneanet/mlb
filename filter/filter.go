package filter

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"sync"

	"github.com/hashicorp/hcl/v2/gohcl"
)

func NewFilter(tc *config.TypedConfig, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable {
	switch tc.Type {
	case "simple":
		c := &SimpleFilterConfig{}
		gohcl.DecodeBody(tc.Config, nil, c)
		c.FullName = fmt.Sprintf("filter.%s.%s", tc.Type, tc.Name)
		return NewSimpleFilter(c, sources, wg, ctx)
	default:
		panic("") // TODO
	}
}
