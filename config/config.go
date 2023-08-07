package config

import (
	"mlb/balancer"
	"mlb/checker"
	"mlb/filter"
	"mlb/inventory"
	"mlb/metrics"
	"mlb/proxy"
	"mlb/system"
	"os"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
)

type Config struct {
	InventoryList []*inventory.Config    `hcl:"inventory,block"`
	CheckerList   []*checker.Config      `hcl:"checker,block"`
	FilterList    []*filter.Config       `hcl:"filter,block"`
	BalancerList  []*balancer.Config     `hcl:"balancer,block"`
	ProxyList     []*proxy.Config        `hcl:"proxy,block"`
	Metrics       *metrics.MetricsConfig `hcl:"metrics,block"`
	System        *system.SystemConfig   `hcl:"system,block"`
}

///////////////////////////

var configFileSchema = &hcl.BodySchema{
	Blocks: []hcl.BlockHeaderSchema{
		{
			Type:       "inventory",
			LabelNames: []string{"type", "id"},
		},
		{
			Type:       "checker",
			LabelNames: []string{"type", "id"},
		},
		{
			Type:       "filter",
			LabelNames: []string{"type", "id"},
		},
		{
			Type:       "balancer",
			LabelNames: []string{"type", "id"},
		},
		{
			Type:       "proxy",
			LabelNames: []string{"type", "id"},
		},
		{
			Type: "metrics",
		},
		{
			Type: "system",
		},
	},
}

func RenderConfigDiag(diags hcl.Diagnostics, parser *hclparse.Parser) {
	wr := hcl.NewDiagnosticTextWriter(
		os.Stdout,      // writer to send messages to
		parser.Files(), // the parser's file cache, for source snippets
		78,             // wrapping width
		true,           // generate colored/highlighted output
	)
	wr.WriteDiagnostics(diags)
}

func LoadConfig(path string) (*Config, hcl.Diagnostics) {
	diags := hcl.Diagnostics{}
	p := hclparse.NewParser()
	c := &Config{
		InventoryList: []*inventory.Config{},
		CheckerList:   []*checker.Config{},
		FilterList:    []*filter.Config{},
		BalancerList:  []*balancer.Config{},
		ProxyList:     []*proxy.Config{},
	}

	defer func() {
		if diags != nil {
			RenderConfigDiag(diags, p)
		}
	}()

	hclfile, parseDiags := p.ParseHCLFile(path)
	diags = append(diags, parseDiags...)

	content, contentDiags := hclfile.Body.Content(configFileSchema)
	diags = append(diags, contentDiags...)

	for _, block := range content.Blocks {
		switch block.Type {
		case "inventory":
			config, diagsInventory := inventory.DecodeConfigBlock(block)
			diags = append(diags, diagsInventory...)
			if config != nil {
				c.InventoryList = append(c.InventoryList, config)
			}
		case "checker":
			config, diagsChecker := checker.DecodeConfigBlock(block)
			diags = append(diags, diagsChecker...)
			if config != nil {
				c.CheckerList = append(c.CheckerList, config)
			}
		case "filter":
			config, diagsFilter := filter.DecodeConfigBlock(block)
			diags = append(diags, diagsFilter...)
			if config != nil {
				c.FilterList = append(c.FilterList, config)
			}
		case "balancer":
			config, diagsBalancer := balancer.DecodeConfigBlock(block)
			diags = append(diags, diagsBalancer...)
			if config != nil {
				c.BalancerList = append(c.BalancerList, config)
			}
		case "proxy":
			config, diagsProxy := proxy.DecodeConfigBlock(block)
			diags = append(diags, diagsProxy...)
			if config != nil {
				c.ProxyList = append(c.ProxyList, config)
			}
		case "metrics":
			var metricsDiags hcl.Diagnostics
			c.Metrics, metricsDiags = metrics.DecodeConfigBlock(block)
			diags = append(diags, metricsDiags...)
		case "system":
			var systemDiags hcl.Diagnostics
			c.System, systemDiags = system.DecodeConfigBlock(block)
			diags = append(diags, systemDiags...)
		}
	}

	return c, diags
}
