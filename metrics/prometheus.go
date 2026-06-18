package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// FeCnxProcessed counts total frontend connections processed.
	FeCnxProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_connections_processed",
			Help: "The number connections processed by frontend",
		},
		[]string{"address", "proxy"},
	)

	// BeCnxProcessed counts total backend connections processed.
	BeCnxProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_connections_processed",
			Help: "The number connections processed by backend",
		},
		[]string{"address", "proxy"},
	)

	// FeCnxErrors counts total connection errors at frontend.
	FeCnxErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_connection_errors",
			Help: "The number of connection errors",
		},
		[]string{"frontend", "proxy"},
	)

	// FeActCnx tracks current active frontend connections.
	FeActCnx = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mlb_frontend_active_connections",
			Help: "The number of active connections at frontend",
		},
		[]string{"address", "proxy"},
	)

	// BeActCnx tracks current active backend connections.
	BeActCnx = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mlb_backend_active_connections",
			Help: "The number of active connections at backend",
		},
		[]string{"address", "proxy"},
	)

	// FeBytesIn counts total bytes received from frontend.
	FeBytesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_bytes_in",
			Help: "The number of inwards bytes processed by frontend",
		},
		[]string{"address", "proxy"},
	)

	// FeBytesOut counts total bytes sent to frontend.
	FeBytesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_bytes_out",
			Help: "The number of outwards bytes processed by frontend",
		},
		[]string{"address", "proxy"},
	)

	// BeBytesIn counts total bytes received from backends.
	BeBytesIn = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_bytes_in",
			Help: "The number of inwards bytes processed by backend",
		},
		[]string{"address", "proxy"},
	)

	// BeBytesOut counts total bytes sent to backends.
	BeBytesOut = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_bytes_out",
			Help: "The number of outwards bytes processed by backend",
		},
		[]string{"address", "proxy"},
	)
)
