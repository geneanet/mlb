package config

import (
	"os"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
)

type TypedConfig struct {
	Type   string
	Name   string
	Config hcl.Body
}

type Config struct {
	InventoryList []*TypedConfig `hcl:"inventory,block"`
	CheckerList   []*TypedConfig `hcl:"checker,block"`
	FilterList    []*TypedConfig `hcl:"filter,block"`
	BalancerList  []*TypedConfig `hcl:"balancer,block"`
	ProxyList     []*TypedConfig `hcl:"proxy,block"`
	Metrics       *MetricsConfig `hcl:"metrics,block"`
	System        *SystemConfig  `hcl:"system,block"`
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

func decodeTypedConfigBlock(block *hcl.Block) *TypedConfig {
	return &TypedConfig{
		Type:   block.Labels[0],
		Name:   block.Labels[1],
		Config: block.Body,
	}
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
	var diags hcl.Diagnostics
	p := hclparse.NewParser()
	c := &Config{
		InventoryList: []*TypedConfig{},
		CheckerList:   []*TypedConfig{},
		FilterList:    []*TypedConfig{},
		BalancerList:  []*TypedConfig{},
		ProxyList:     []*TypedConfig{},
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
			c.InventoryList = append(c.InventoryList, decodeTypedConfigBlock(block))
		case "checker":
			c.CheckerList = append(c.CheckerList, decodeTypedConfigBlock(block))
		case "filter":
			c.FilterList = append(c.FilterList, decodeTypedConfigBlock(block))
		case "balancer":
			c.BalancerList = append(c.BalancerList, decodeTypedConfigBlock(block))
		case "proxy":
			c.ProxyList = append(c.ProxyList, decodeTypedConfigBlock(block))
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
