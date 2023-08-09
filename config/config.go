package config

import (
	"mlb/backends_inventory"
	"mlb/backends_processor"
	"mlb/balancer"
	"mlb/metrics"
	"mlb/proxy"
	"mlb/system"
	"os"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
)

type Config struct {
	BackendsInventoryList []*backends_inventory.Config
	BackendsProcessorList []*backends_processor.Config
	BalancerList          []*balancer.Config
	ProxyList             []*proxy.Config
	Metrics               *metrics.MetricsConfig
	System                *system.SystemConfig
}

var configFileSchema = &hcl.BodySchema{
	Blocks: []hcl.BlockHeaderSchema{
		{
			Type:       "backends_inventory",
			LabelNames: []string{"type", "id"},
		},
		{
			Type:       "backends_processor",
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
		BackendsInventoryList: []*backends_inventory.Config{},
		BackendsProcessorList: []*backends_processor.Config{},
		BalancerList:          []*balancer.Config{},
		ProxyList:             []*proxy.Config{},
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
		case "backends_inventory":
			config, diagsBackendsInventory := backends_inventory.DecodeConfigBlock(block)
			diags = append(diags, diagsBackendsInventory...)
			if config != nil {
				c.BackendsInventoryList = append(c.BackendsInventoryList, config)
			}
		case "backends_processor":
			config, diagsBackendsProcessor := backends_processor.DecodeConfigBlock(block)
			diags = append(diags, diagsBackendsProcessor...)
			if config != nil {
				c.BackendsProcessorList = append(c.BackendsProcessorList, config)
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
