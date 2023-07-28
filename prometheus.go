package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metrics_FeCnxProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_frontend_connections_processed",
			Help: "The number connections processed by frontend",
		},
		[]string{"address"},
	)

	metrics_BeCnxProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_backend_connections_processed",
			Help: "The number connections processed by backend",
		},
		[]string{"address"},
	)

	metrics_FeCnxErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mlb_connection_errors",
			Help: "The number of connection errors",
		},
		[]string{"frontend"},
	)

	metrics_FeActCnx = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mlb_frontend_active_connections",
			Help: "The number of active connections at frontend",
		},
		[]string{"address"},
	)

	metrics_BeActCnx = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "mlb_backend_active_connections",
			Help: "The number of active connections at backend",
		},
		[]string{"address"},
	)
)
