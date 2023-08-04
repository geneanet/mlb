package balancer

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"sync"

	"github.com/hashicorp/hcl/v2/gohcl"
)

func NewBalancer(tc *config.TypedConfig, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) backend.BackendProvider {
	switch tc.Type {
	case "wrr":
		c := &WRRBalancerConfig{}
		gohcl.DecodeBody(tc.Config, nil, c)
		c.FullName = fmt.Sprintf("balancer.%s.%s", tc.Type, tc.Name)
		return NewBalancerWRR(c, sources, wg, ctx)
	default:
		panic("") // TODO
	}
}
