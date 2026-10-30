package dashboard

import (
	"encoding/json"
	"mlb/backend"
	"mlb/config"
	"mlb/metrics"
	"mlb/module"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
)

type mockBackendListProvider struct {
	backends []*backend.Backend
}

func (m *mockBackendListProvider) GetBackendList() []*backend.Backend {
	return m.backends
}

func TestRegisterHandlers(t *testing.T) {
	ml := make(module.ModulesRegistry)
	conf := &config.Config{}
	mux := http.NewServeMux()

	RegisterHandlers(mux, ml, conf)

	tests := []struct {
		name           string
		url            string
		expectedStatus int
	}{
		{"Backends", "/backends", http.StatusOK},
		{"Topology", "/topology", http.StatusOK},
		{"ProxyMetrics", "/proxy_metrics", http.StatusOK},
		{"Dashboard", "/dashboard", http.StatusMovedPermanently},
		{"DashboardSlash", "/dashboard/", http.StatusOK},
		{"Root", "/", http.StatusMovedPermanently},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					rr.Code, tt.expectedStatus)
			}
		})
	}
}

func TestBackendsHandler(t *testing.T) {
	ml := make(module.ModulesRegistry)
	provider := &mockBackendListProvider{
		backends: []*backend.Backend{
			backend.NewBackend("10.0.0.3:80", nil),
			backend.NewBackend("10.0.0.1:80", nil),
			backend.NewBackend("10.0.0.2:80", nil),
		},
	}
	ml["test_provider"] = provider

	mux := http.NewServeMux()
	RegisterHandlers(mux, ml, &config.Config{})

	req, err := http.NewRequest("GET", "/backends", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	var res map[string][]*backend.Backend
	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := res["test_provider"]; !ok {
		t.Errorf("expected test_provider in response")
	}
	if len(res["test_provider"]) != 3 {
		t.Fatalf("expected 3 backends in response, got %d", len(res["test_provider"]))
	}
	expectedAddrs := []string{"10.0.0.1:80", "10.0.0.2:80", "10.0.0.3:80"}
	for i, expected := range expectedAddrs {
		if res["test_provider"][i].Address != expected {
			t.Errorf("expected backend[%d] = %s, got %s", i, expected, res["test_provider"][i].Address)
		}
	}
}

func TestTopologyHandler(t *testing.T) {
	conf := &config.Config{
		ProxyList: []*module.Config{
			{
				Category: "proxy",
				Type:     "tcp",
				Name:     "test_proxy",
				RawHCL:   "proxy tcp test_proxy {}",
				Config:   hcl.EmptyBody(),
				Ctx:      &hcl.EvalContext{},
			},
		},
	}

	mux := http.NewServeMux()
	RegisterHandlers(mux, make(module.ModulesRegistry), conf)

	req, err := http.NewRequest("GET", "/topology", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	var nodes []TopologyNode
	if err := json.NewDecoder(rr.Body).Decode(&nodes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	} else if nodes[0].Name != "test_proxy" {
		t.Errorf("unexpected node name: %s", nodes[0].Name)
	}
}

func TestTopologyHandler_MultiSources(t *testing.T) {
	multiHCL := `
sources = ["balancer.wrr.app1", "balancer.wrr.app2"]
`
	backupHCL := `
source = "balancer.wrr.app1"
backup_source = "balancer.wrr.app2"
`
	singleHCL := `
source = "balancer.wrr.app1"
`

	file1, diags1 := hclsyntax.ParseConfig([]byte(multiHCL), "multi.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags1.HasErrors() {
		t.Fatal(diags1)
	}
	file2, diags2 := hclsyntax.ParseConfig([]byte(backupHCL), "backup.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags2.HasErrors() {
		t.Fatal(diags2)
	}
	file3, diags3 := hclsyntax.ParseConfig([]byte(singleHCL), "single.hcl", hcl.Pos{Line: 1, Column: 1})
	if diags3.HasErrors() {
		t.Fatal(diags3)
	}

	conf := &config.Config{
		ProxyList: []*module.Config{
			{
				Category: "proxy",
				Type:     "tcp",
				Name:     "proxy_multi",
				RawHCL:   multiHCL,
				Config:   file1.Body,
				Ctx:      &hcl.EvalContext{},
			},
			{
				Category: "proxy",
				Type:     "tcp",
				Name:     "proxy_backup",
				RawHCL:   backupHCL,
				Config:   file2.Body,
				Ctx:      &hcl.EvalContext{},
			},
			{
				Category: "proxy",
				Type:     "redis",
				Name:     "proxy_single",
				RawHCL:   singleHCL,
				Config:   file3.Body,
				Ctx:      &hcl.EvalContext{},
			},
		},
	}

	mux := http.NewServeMux()
	RegisterHandlers(mux, make(module.ModulesRegistry), conf)

	req, err := http.NewRequest("GET", "/topology", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var nodes []TopologyNode
	if err := json.NewDecoder(rr.Body).Decode(&nodes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// proxy_multi
	if len(nodes[0].Sources) != 2 || nodes[0].Sources[0] != "balancer.wrr.app1" || nodes[0].Sources[1] != "balancer.wrr.app2" {
		t.Errorf("expected proxy_multi sources [balancer.wrr.app1, balancer.wrr.app2], got %v", nodes[0].Sources)
	}

	// proxy_backup
	if len(nodes[1].Sources) != 2 || nodes[1].Sources[0] != "balancer.wrr.app1" || nodes[1].Sources[1] != "balancer.wrr.app2" {
		t.Errorf("expected proxy_backup sources [balancer.wrr.app1, balancer.wrr.app2], got %v", nodes[1].Sources)
	}
	if nodes[1].Source != "balancer.wrr.app1" {
		t.Errorf("expected proxy_backup source 'balancer.wrr.app1', got %s", nodes[1].Source)
	}

	// proxy_single
	if nodes[2].Source != "balancer.wrr.app1" {
		t.Errorf("expected proxy_single source 'balancer.wrr.app1', got %s", nodes[2].Source)
	}
	if len(nodes[2].Sources) != 0 {
		t.Errorf("expected proxy_single sources to be empty, got %v", nodes[2].Sources)
	}
}


func TestProxyMetricsHandler(t *testing.T) {
	// Use existing metrics to avoid registration conflicts
	metrics.FeActCnx.WithLabelValues("127.0.0.1", "test_proxy").Set(42)
	metrics.FeCnxProcessed.WithLabelValues("127.0.0.1", "test_proxy").Add(100)
	metrics.FeBytesIn.WithLabelValues("127.0.0.1", "test_proxy").Add(1000)
	metrics.FeBytesOut.WithLabelValues("127.0.0.1", "test_proxy").Add(2000)
	metrics.FeRequests.WithLabelValues("127.0.0.1", "test_proxy").Add(10)
	metrics.FeCnxErrors.WithLabelValues("127.0.0.1", "test_proxy").Add(1)

	defer func() {
		metrics.FeActCnx.DeleteLabelValues("127.0.0.1", "test_proxy")
		metrics.FeCnxProcessed.DeleteLabelValues("127.0.0.1", "test_proxy")
		metrics.FeBytesIn.DeleteLabelValues("127.0.0.1", "test_proxy")
		metrics.FeBytesOut.DeleteLabelValues("127.0.0.1", "test_proxy")
		metrics.FeRequests.DeleteLabelValues("127.0.0.1", "test_proxy")
		metrics.FeCnxErrors.DeleteLabelValues("127.0.0.1", "test_proxy")
	}()

	mux := http.NewServeMux()
	RegisterHandlers(mux, make(module.ModulesRegistry), &config.Config{})

	req, err := http.NewRequest("GET", "/proxy_metrics", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	var res map[string]struct {
		ActiveCnx    float64 `json:"active_connections"`
		ProcessedCnx float64 `json:"processed_connections"`
		BytesIn      float64 `json:"bytes_in"`
		BytesOut     float64 `json:"bytes_out"`
		Requests     float64 `json:"requests"`
		Errors       float64 `json:"errors"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	val, ok := res["test_proxy"]
	if !ok {
		t.Fatalf("expected test_proxy in response")
	}

	if val.ActiveCnx != 42 {
		t.Errorf("expected 42 active connections, got %v", val.ActiveCnx)
	}
	if val.ProcessedCnx != 100 {
		t.Errorf("expected 100 processed connections, got %v", val.ProcessedCnx)
	}
	if val.BytesIn != 1000 {
		t.Errorf("expected 1000 bytes in, got %v", val.BytesIn)
	}
	if val.BytesOut != 2000 {
		t.Errorf("expected 2000 bytes out, got %v", val.BytesOut)
	}
	if val.Requests != 10 {
		t.Errorf("expected 10 requests, got %v", val.Requests)
	}
	if val.Errors != 1 {
		t.Errorf("expected 1 error, got %v", val.Errors)
	}
}

func TestDashboardAssetsNoExternalDependencies(t *testing.T) {
	content, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatalf("failed to read index.html: %v", err)
	}
	s := string(content)

	forbidden := []string{
		"http://",
		"https://",
		"//cdnjs",
		"//cdn",
		"//unpkg",
		"mermaid",
		"alpine",
		"tailwind",
	}

	for _, token := range forbidden {
		checkContent := s
		if token == "http://" {
			// Allow standard W3C XML namespaces used in SVG DOM creation
			checkContent = strings.ReplaceAll(checkContent, "http://www.w3.org/2000/svg", "")
		}
		if strings.Contains(strings.ToLower(checkContent), strings.ToLower(token)) {
			t.Errorf("index.html must not contain external dependency or library token %q", token)
		}
	}
}

func TestDashboardEssentialDOM(t *testing.T) {
	content, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatalf("failed to read index.html: %v", err)
	}
	s := string(content)

	requiredElements := []string{
		`id="topology-svg"`,
		`id="module-drawer"`,
		`id="tab-topology"`,
		`id="tab-backends"`,
		`id="tab-metrics"`,
		`id="theme-toggle"`,
	}

	for _, elem := range requiredElements {
		if !strings.Contains(s, elem) {
			t.Errorf("index.html is missing required DOM element %q", elem)
		}
	}
}

func TestDashboardCrossingReductionPresent(t *testing.T) {
	content, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatalf("failed to read index.html: %v", err)
	}
	s := string(content)

	if !strings.Contains(s, "barycenter") {
		t.Errorf("index.html must implement crossing reduction barycenter heuristic")
	}
}

func TestDashboardHCLSyntaxHighlighting(t *testing.T) {
	content, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatalf("failed to read index.html: %v", err)
	}
	s := string(content)

	requiredTokens := []string{
		"highlightHCL",
		"hcl-string",
		"hcl-comment",
		"hcl-bool",
		"hcl-block",
	}

	for _, token := range requiredTokens {
		if !strings.Contains(s, token) {
			t.Errorf("index.html is missing HCL syntax highlighting token %q", token)
		}
	}

	// Ensure no malformed CSS selectors with trailing comma before @media
	if strings.Contains(s, ",\n        @media") || strings.Contains(s, ",\r\n        @media") {
		t.Errorf("index.html contains malformed CSS with dangling comma before @media")
	}
}

func TestDashboardFixesAndRobustness(t *testing.T) {
	content, err := FS.ReadFile("index.html")
	if err != nil {
		t.Fatalf("failed to read index.html: %v", err)
	}
	s := string(content)

	checks := []struct {
		name     string
		needle   string
		expected bool
	}{
		{"resetGraphView active tab guard", "if (state.activeTab !== 'topology') return;", true},
		{"pointerdown left button guard", "if (e.button !== 0) return;", true},
		{"pointercancel listener", "svg.addEventListener('pointercancel', onPointerEnd);", true},
		{"closeDrawer reset query", "state.drawerFilterQuery = '';", true},
		{"drawer filter input reset", "const filterInput = document.getElementById('drawer-backend-filter');", true},
		{"proxy filter pill", "setCategoryFilter('proxy', this)", true},
		{"formatBytes petabytes and abs", "const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];", true},
		{"SVG name truncation tooltip", "<title>${escapeHtml(rawName)}</title>", true},
		{"badge CSS variables", "--badge-inventory-bg", true},
		{"badge CSS variables for proxies", "--badge-proxy-color", true},
		{"safe address access in drawer search", "const addr = (b.address || '').toLowerCase();", true},
		{"tabs-nav absolute center", "transform: translateX(-50%);", true},
		{"refresh-status min-width", "min-width: 85px;", true},
		{"drawer default width 600px", "width: 600px;", true},
		{"multi-sources helper", "function getNodeSources(node)", true},
	}

	for _, c := range checks {
		if strings.Contains(s, c.needle) != c.expected {
			t.Errorf("check %q failed: contains %q expected %v", c.name, c.needle, c.expected)
		}
	}
}
