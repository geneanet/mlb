package config

import (
	"fmt"
	"mlb/metrics"
	"mlb/module"
	"mlb/system"
	"os"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
	"github.com/zclconf/go-cty/cty/function/stdlib"
)

type Config struct {
	BackendsInventoryList []*module.Config
	BackendsProcessorList []*module.Config
	BalancerList          []*module.Config
	ProxyList             []*module.Config
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
		BackendsInventoryList: []*module.Config{},
		BackendsProcessorList: []*module.Config{},
		BalancerList:          []*module.Config{},
		ProxyList:             []*module.Config{},
	}

	defer func() {
		if diags != nil {
			RenderConfigDiag(diags, p)
		}
	}()

	hclfile, parseDiags := p.ParseHCLFile(path)
	diags = append(diags, parseDiags...)

	if hclfile == nil {
		return c, diags
	}

	content, contentDiags := hclfile.Body.Content(configFileSchema)
	diags = append(diags, contentDiags...)

	// First pass over all blocks to list modules instances and create an eval context for the next pass
	modules := map[string]map[string]map[string]string{}
	for _, block := range content.Blocks {
		switch block.Type {
		case "backends_inventory", "backends_processor", "balancer", "proxy":
			if _, ok := modules[block.Type]; !ok {
				modules[block.Type] = make(map[string]map[string]string)
			}
			if _, ok := modules[block.Type][block.Labels[0]]; !ok {
				modules[block.Type][block.Labels[0]] = make(map[string]string)
			}
			modules[block.Type][block.Labels[0]][block.Labels[1]] = fmt.Sprintf("%s.%s.%s", block.Type, block.Labels[0], block.Labels[1])
		}
	}
	modulesCty := map[string]cty.Value{}
	for module, buckets := range modules {
		bucketsCty := map[string]cty.Value{}
		for bucket, keys := range buckets {
			bucketCty := map[string]cty.Value{}
			for key, id := range keys {
				bucketCty[key] = cty.StringVal(id)
			}
			bucketsCty[bucket] = cty.ObjectVal(bucketCty)
		}
		modulesCty[module] = cty.ObjectVal(bucketsCty)
	}
	ctx := &hcl.EvalContext{
		Variables: modulesCty,
		Functions: map[string]function.Function{
			"abs":      stdlib.AbsoluteFunc,
			"ceil":     stdlib.CeilFunc,
			"contains": stdlib.ContainsFunc,
			"floor":    stdlib.FloorFunc,
			"int":      stdlib.IntFunc,
			"join":     stdlib.JoinFunc,
			"len":      stdlib.LengthFunc,
			"max":      stdlib.MaxFunc,
			"min":      stdlib.MinFunc,
			"parseint": stdlib.ParseIntFunc,
			"split":    stdlib.SplitFunc,
			"strlen":   stdlib.StrlenFunc,
		},
	}

	// Second pass to actually parse the blocks contents
	for _, block := range content.Blocks {
		switch block.Type {
		case "backends_inventory":
			config, diagsBackendsInventory := module.DecodeConfigBlock(block, ctx, "backends_inventory")
			diags = append(diags, diagsBackendsInventory...)
			if config != nil {
				c.BackendsInventoryList = append(c.BackendsInventoryList, config)
			}
		case "backends_processor":
			config, diagsBackendsProcessor := module.DecodeConfigBlock(block, ctx, "backends_processor")
			diags = append(diags, diagsBackendsProcessor...)
			if config != nil {
				c.BackendsProcessorList = append(c.BackendsProcessorList, config)
			}
		case "balancer":
			config, diagsBalancer := module.DecodeConfigBlock(block, ctx, "balancer")
			diags = append(diags, diagsBalancer...)
			if config != nil {
				c.BalancerList = append(c.BalancerList, config)
			}
		case "proxy":
			config, diagsProxy := module.DecodeConfigBlock(block, ctx, "proxy")
			diags = append(diags, diagsProxy...)
			if config != nil {
				c.ProxyList = append(c.ProxyList, config)
			}
		case "metrics":
			var metricsDiags hcl.Diagnostics
			c.Metrics, metricsDiags = metrics.DecodeConfigBlock(block, ctx)
			diags = append(diags, metricsDiags...)
		case "system":
			var systemDiags hcl.Diagnostics
			c.System, systemDiags = system.DecodeConfigBlock(block, ctx)
			diags = append(diags, systemDiags...)
		}
	}

	return c, diags
}

// CheckDuration validates that a string is a valid duration and appends a diagnostic error if not.
func CheckDuration(diags *hcl.Diagnostics, val string, name string) {
	if val != "" {
		if _, err := time.ParseDuration(val); err != nil {
			*diags = append(*diags, &hcl.Diagnostic{
				Severity: hcl.DiagError,
				Summary:  fmt.Sprintf("Invalid %s", name),
				Detail:   err.Error(),
			})
		}
	}
}
