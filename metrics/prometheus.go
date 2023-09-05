package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	FeCnxProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_connections_processed",
			Help: "The number connections processed by frontend",
		},
		[]string{"address", "proxy"},
	)

	BeCnxProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_connections_processed",
			Help: "The number connections processed by backend",
		},
		[]string{"address", "proxy"},
	)

	FeCnxErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_connection_errors",
			Help: "The number of connection errors",
		},
		[]string{"frontend", "proxy"},
	)

	FeActCnx = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mlb_frontend_active_connections",
			Help: "The number of active connections at frontend",
		},
		[]string{"address", "proxy"},
	)

	BeActCnx = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mlb_backend_active_connections",
			Help: "The number of active connections at backend",
		},
		[]string{"address", "proxy"},
	)

	FeBytesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_bytes_in",
			Help: "The number of inwards bytes processed by frontend",
		},
		[]string{"address", "proxy"},
	)

	FeBytesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_bytes_out",
			Help: "The number of outwards bytes processed by frontend",
		},
		[]string{"address", "proxy"},
	)

	BeBytesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_bytes_in",
			Help: "The number of inwards bytes processed by backend",
		},
		[]string{"address", "proxy"},
	)

	BeBytesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_bytes_out",
			Help: "The number of outwards bytes processed by backend",
		},
		[]string{"address", "proxy"},
	)
)
