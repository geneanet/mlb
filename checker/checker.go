package checker

import (
	"context"
	"fmt"
	"mlb/backend"
	"mlb/config"
	"sync"

	"github.com/hashicorp/hcl/v2/gohcl"
)

func NewChecker(tc *config.TypedConfig, sources map[string]backend.Subscribable, wg *sync.WaitGroup, ctx context.Context) backend.Subscribable {
	switch tc.Type {
	case "mysql":
		c := &MySQLCheckerConfig{}
		gohcl.DecodeBody(tc.Config, nil, c)
		c.FullName = fmt.Sprintf("checker.%s.%s", tc.Type, tc.Name)
		return NewCheckerMySQL(c, sources, wg, ctx)
	default:
		panic("") // TODO
	}
}
