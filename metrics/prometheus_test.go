// Package metrics provides functionality for tracking and exporting application metrics.
package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestMetricsInitializationAndUsage verifies that all defined Prometheus metrics
// (Counters and Gauges) are correctly initialized and can be manipulated (incremented/decremented).
// It tests both backend-side and frontend-side metrics with various labels (address, proxy ID).
func TestMetricsInitializationAndUsage(t *testing.T) {
	// 1. Verify BeCnxProcessed (Backend Connections Processed - Counter)
	BeCnxProcessed.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeCnxProcessed.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeCnxProcessed to be 1, got %v", val)
	}

	// 2. Verify BeActCnx (Active Backend Connections - Gauge)
	BeActCnx.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	BeActCnx.WithLabelValues("127.0.0.1:8080", "proxy-1").Dec()
	if val := testutil.ToFloat64(BeActCnx.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 0 {
		t.Errorf("Expected BeActCnx to be 0 after Inc and Dec, got %v", val)
	}

	// 3. Verify FeCnxErrors (Frontend Connection Errors - Counter)
	FeCnxErrors.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeCnxErrors.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeCnxErrors to be 1, got %v", val)
	}

	// 4. Verify FeCnxProcessed (Frontend Connections Processed - Counter)
	FeCnxProcessed.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeCnxProcessed.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeCnxProcessed to be 1, got %v", val)
	}

	// 5. Verify FeActCnx (Active Frontend Connections - Gauge)
	FeActCnx.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeActCnx.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeActCnx to be 1, got %v", val)
	}

	// 6. Verify FeBytesIn (Frontend Bytes Received - Counter)
	FeBytesIn.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeBytesIn.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeBytesIn to be 1, got %v", val)
	}

	// 7. Verify FeBytesOut (Frontend Bytes Sent - Counter)
	FeBytesOut.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeBytesOut.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeBytesOut to be 1, got %v", val)
	}

	// 8. Verify BeBytesIn (Backend Bytes Received - Counter)
	BeBytesIn.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeBytesIn.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeBytesIn to be 1, got %v", val)
	}

	// 9. Verify BeBytesOut (Backend Bytes Sent - Counter)
	BeBytesOut.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeBytesOut.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeBytesOut to be 1, got %v", val)
	}

	// 10. Verify FeRequests (Frontend Requests - Counter)
	FeRequests.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeRequests.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeRequests to be 1, got %v", val)
	}

	// 11. Verify BeRequests (Backend Requests - Counter)
	BeRequests.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeRequests.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeRequests to be 1, got %v", val)
	}
}
