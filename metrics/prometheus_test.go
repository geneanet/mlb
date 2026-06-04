package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsInitializationAndUsage(t *testing.T) {
	// Verify BeCnxProcessed metric (Counter)
	BeCnxProcessed.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeCnxProcessed.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeCnxProcessed to be 1, got %v", val)
	}

	// Verify BeActCnx metric (Gauge)
	BeActCnx.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	BeActCnx.WithLabelValues("127.0.0.1:8080", "proxy-1").Dec()
	if val := testutil.ToFloat64(BeActCnx.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 0 {
		t.Errorf("Expected BeActCnx to be 0 after Inc and Dec, got %v", val)
	}

	// Verify FeCnxErrors metric (Counter)
	FeCnxErrors.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeCnxErrors.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeCnxErrors to be 1, got %v", val)
	}

	// Verify FeCnxProcessed metric (Counter)
	FeCnxProcessed.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeCnxProcessed.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeCnxProcessed to be 1, got %v", val)
	}

	// Verify FeActCnx metric (Gauge)
	FeActCnx.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeActCnx.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeActCnx to be 1, got %v", val)
	}

	// Verify FeBytesIn metric (Counter)
	FeBytesIn.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeBytesIn.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeBytesIn to be 1, got %v", val)
	}

	// Verify FeBytesOut metric (Counter)
	FeBytesOut.WithLabelValues("0.0.0.0:6379", "proxy-1").Inc()
	if val := testutil.ToFloat64(FeBytesOut.WithLabelValues("0.0.0.0:6379", "proxy-1")); val != 1 {
		t.Errorf("Expected FeBytesOut to be 1, got %v", val)
	}

	// Verify BeBytesIn metric (Counter)
	BeBytesIn.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeBytesIn.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeBytesIn to be 1, got %v", val)
	}

	// Verify BeBytesOut metric (Counter)
	BeBytesOut.WithLabelValues("127.0.0.1:8080", "proxy-1").Inc()
	if val := testutil.ToFloat64(BeBytesOut.WithLabelValues("127.0.0.1:8080", "proxy-1")); val != 1 {
		t.Errorf("Expected BeBytesOut to be 1, got %v", val)
	}
}
