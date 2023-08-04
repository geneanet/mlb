package config

import (
	"mlb/balancer"
	"mlb/checker"
	"mlb/filter"
	"mlb/inventory"
	"mlb/proxy"
	"os"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
)

type Config struct {
	InventoryList []*inventory.Config `hcl:"inventory,block"`
	CheckerList   []*checker.Config   `hcl:"checker,block"`
	FilterList    []*filter.Config    `hcl:"filter,block"`
	BalancerList  []*balancer.Config  `hcl:"balancer,block"`
	ProxyList     []*proxy.Config     `hcl:"proxy,block"`
	Metrics       *MetricsConfig      `hcl:"metrics,block"`
	System        *SystemConfig       `hcl:"system,block"`
}

type MetricsConfig struct {
	Address string `hcl:"address"`
}

type SystemConfig struct {
	RLimit RLimitConfig `hcl:"rlimit,block"`
}

type RLimitConfig struct {
	NOFile uint64 `hcl:"nofile"`
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

func decodeMetricsBlock(block *hcl.Block) (*MetricsConfig, hcl.Diagnostics) {
	c := &MetricsConfig{}
	diag := gohcl.DecodeBody(block.Body, nil, c)
	return c, diag
}

func decodeSystemBlock(block *hcl.Block) (*SystemConfig, hcl.Diagnostics) {
	c := &SystemConfig{}
	diag := gohcl.DecodeBody(block.Body, nil, c)
	return c, diag
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
			config := inventory.DecodeConfigBlock(block)
			diagsInventory := inventory.ValidateConfig(config)
			diags = append(diags, diagsInventory...)
			c.InventoryList = append(c.InventoryList, config)
		case "checker":
			config := checker.DecodeConfigBlock(block)
			diagsChecker := checker.ValidateConfig(config)
			diags = append(diags, diagsChecker...)
			c.CheckerList = append(c.CheckerList, config)
		case "filter":
			config := filter.DecodeConfigBlock(block)
			diagsFilter := filter.ValidateConfig(config)
			diags = append(diags, diagsFilter...)
			c.FilterList = append(c.FilterList, config)
		case "balancer":
			config := balancer.DecodeConfigBlock(block)
			diagsBalancer := balancer.ValidateConfig(config)
			diags = append(diags, diagsBalancer...)
			c.BalancerList = append(c.BalancerList, config)
		case "proxy":
			config := proxy.DecodeConfigBlock(block)
			diagsProxy := proxy.ValidateConfig(config)
			diags = append(diags, diagsProxy...)
			c.ProxyList = append(c.ProxyList, config)
		case "metrics":
			var metricsDiags hcl.Diagnostics
			c.Metrics, metricsDiags = decodeMetricsBlock(block)
			diags = append(diags, metricsDiags...)
		case "system":
			var systemDiags hcl.Diagnostics
			c.System, systemDiags = decodeSystemBlock(block)
			diags = append(diags, systemDiags...)
		}
	}

	return c, diags
}
