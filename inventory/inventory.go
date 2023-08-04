package inventory

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"sync"

	"github.com/hashicorp/hcl/v2/gohcl"
)

func NewInventory(tc *config.TypedConfig, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable {
	switch tc.Type {
	case "consul":
		c := &ConsulInventoryConfig{}
		gohcl.DecodeBody(tc.Config, nil, c)
		c.FullName = fmt.Sprintf("inventory.%s.%s", tc.Type, tc.Name)
		return NewInventoryConsul(c, wg, ctx)
	default:
		panic("") // TODO
	}
}
