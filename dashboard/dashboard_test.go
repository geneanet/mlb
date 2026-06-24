package dashboard

import (
	"encoding/json"
	"mlb/backend"
	"mlb/config"
	"mlb/metrics"
	"mlb/module"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hashicorp/hcl/v2"
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
			{Address: "1.2.3.4:80"},
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
	if len(res["test_provider"]) != 1 || res["test_provider"][0].Address != "1.2.3.4:80" {
		t.Errorf("unexpected backend in response: %+v", res["test_provider"])
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
