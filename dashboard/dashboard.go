package dashboard

import (
	"embed"
	"encoding/json"
	"mlb/backend"
	"mlb/config"
	"mlb/module"
	"net/http"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

// FS contains the dashboard static assets.
//
//go:embed index.html
var FS embed.FS

// TopologyNode represents a node in the MLB module graph.
type TopologyNode struct {
	ID       string `json:"id"`
	Category string `json:"category"`
	Type     string `json:"type"`
	Name     string `json:"name"`
	Source   string `json:"source,omitempty"`
	HCL      string `json:"hcl,omitempty"`
}

// RegisterHandlers registers the dashboard and API endpoints.
func RegisterHandlers(mux *http.ServeMux, ml module.ModulesRegistry, conf *config.Config) {
	// Backends / Modules
	mux.HandleFunc("/backends", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		backendListProviders := module.Filter[backend.BackendListProvider](ml)
		backendsByProvider := make(map[string]backend.BackendsList, len(backendListProviders))
		for id := range backendListProviders {
			backendsByProvider[id] = module.Get[backend.BackendListProvider](backendListProviders, id).GetBackendList()
		}
		out, err := json.Marshal(backendsByProvider)
		if err != nil {
			http.Error(w, "serialization error", http.StatusInternalServerError)
			return
		}
		if _, err := w.Write(out); err != nil {
			log.Warn().Err(err).Msg("Failed to write /backends response")
		}
	})

	// Topology
	mux.HandleFunc("/topology", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		var nodes []TopologyNode
		allModules := [][]*module.Config{
			conf.BackendsInventoryList,
			conf.BackendsProcessorList,
			conf.BalancerList,
			conf.ProxyList,
		}

		type sourceOnly struct {
			Source string   `hcl:"source,optional"`
			Remain hcl.Body `hcl:",remain"`
		}

		for _, list := range allModules {
			for _, m := range list {
				node := TopologyNode{
					ID:       m.FullID(),
					Category: m.Category,
					Type:     m.Type,
					Name:     m.Name,
					HCL:      m.RawHCL,
				}
				var so sourceOnly
				gohcl.DecodeBody(m.Config, m.Ctx, &so)
				node.Source = so.Source
				nodes = append(nodes, node)
			}
		}

		if err := json.NewEncoder(w).Encode(nodes); err != nil {
			log.Warn().Err(err).Msg("Failed to write /topology response")
		}
	})

	// Proxy Metrics
	mux.HandleFunc("/proxy_metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		type ProxyMetrics struct {
			ActiveCnx    float64 `json:"active_connections"`
			ProcessedCnx float64 `json:"processed_connections"`
			BytesIn      float64 `json:"bytes_in"`
			BytesOut     float64 `json:"bytes_out"`
			Requests     float64 `json:"requests"`
			Errors       float64 `json:"errors"`
		}

		res := make(map[string]*ProxyMetrics)

		for _, mf := range mfs {
			name := mf.GetName()
			for _, m := range mf.GetMetric() {
				var proxyID string
				for _, l := range m.GetLabel() {
					if l.GetName() == "proxy" {
						proxyID = l.GetValue()
						break
					}
				}
				if proxyID == "" {
					continue
				}

				if _, ok := res[proxyID]; !ok {
					res[proxyID] = &ProxyMetrics{}
				}

				val := 0.0
				if m.GetCounter() != nil {
					val = m.GetCounter().GetValue()
				} else if m.GetGauge() != nil {
					val = m.GetGauge().GetValue()
				}

				switch name {
				case "mlb_frontend_active_connections":
					res[proxyID].ActiveCnx += val
				case "mlb_frontend_connections_processed":
					res[proxyID].ProcessedCnx += val
				case "mlb_frontend_bytes_in":
					res[proxyID].BytesIn += val
				case "mlb_frontend_bytes_out":
					res[proxyID].BytesOut += val
				case "mlb_frontend_requests_total":
					res[proxyID].Requests += val
				case "mlb_connection_errors":
					res[proxyID].Errors += val
				}
			}
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			log.Warn().Err(err).Msg("Failed to write /proxy_metrics response")
		}
	})

	// Dashboard
	mux.Handle("/dashboard/", http.StripPrefix("/dashboard/", http.FileServer(http.FS(FS))))
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})
}
