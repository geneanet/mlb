package proxy

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"sync"

	"github.com/hashicorp/hcl/v2/gohcl"
)

func NewProxy(tc *config.TypedConfig, sources map[string]backend.BackendProvider, wg *sync.WaitGroup, ctx context.Context) {
	switch tc.Type {
	case "tcp":
		c := &TCPProxyConfig{}
		gohcl.DecodeBody(tc.Config, nil, c)
		c.FullName = fmt.Sprintf("proxy.%s.%s", tc.Type, tc.Name)
		NewProxyTCP(c, sources, wg, ctx)
	default:
		panic("") // TODO
	}
}
