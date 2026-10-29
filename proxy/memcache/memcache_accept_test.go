package memcache

import (
	"context"
	"errors"
	"mlb/backend"
	"mlb/testutil"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type mockFailingListener struct {
	net.Listener
	failCount int
	maxFails  int
	mu        sync.Mutex
}

func (m *mockFailingListener) Accept() (net.Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failCount < m.maxFails {
		m.failCount++
		return nil, errors.New("simulated temporary accept error")
	}
	return m.Listener.Accept()
}

func TestMemcacheProxy_AcceptLoop_Backoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mockLn := &mockFailingListener{
		Listener: ln,
		maxFails: 2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nopLogger := zerolog.Nop()
	p := &MemcacheProxy{
		id:       "test-accept-backoff",
		log:      nopLogger,
		ctx:      ctx,
		cancel:   cancel,
		backends: backend.NewRegistry(nopLogger, false),
		ring:     newMemcacheHashRing(),
	}

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_processed_mc_backoff"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_active_mc_backoff"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_in_mc_backoff"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bytes_out_mc_backoff"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_requests_mc_backoff"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_cnx_errors_mc_backoff"}),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go p.acceptLoop(mockLn, ln.Addr().String(), &wg, feMetrics)

	// Connect client after backoff retries succeed
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	testutil.Eventually(t, func() bool {
		mockLn.mu.Lock()
		defer mockLn.mu.Unlock()
		return mockLn.failCount == 2
	}, 1*time.Second, 10*time.Millisecond)

	cancel()
	_ = ln.Close()
	wg.Wait()
}

func TestMemcacheProxy_AcceptLoop_CancelDuringBackoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mockLn := &mockFailingListener{
		Listener: ln,
		maxFails: 1000,
	}

	ctx, cancel := context.WithCancel(context.Background())

	nopLogger := zerolog.Nop()
	p := &MemcacheProxy{
		id:       "test-accept-cancel",
		log:      nopLogger,
		ctx:      ctx,
		cancel:   cancel,
		backends: backend.NewRegistry(nopLogger, false),
		ring:     newMemcacheHashRing(),
	}

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_proc_mc_cancel"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_act_mc_cancel"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bin_mc_cancel"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bout_mc_cancel"}),
		requests:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_req_mc_cancel"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_err_mc_cancel"}),
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go p.acceptLoop(mockLn, ln.Addr().String(), &wg, feMetrics)

	// Wait for at least 1 failed accept
	testutil.Eventually(t, func() bool {
		mockLn.mu.Lock()
		defer mockLn.mu.Unlock()
		return mockLn.failCount >= 1
	}, 1*time.Second, 10*time.Millisecond)

	// Cancel context while it is backing off
	cancel()

	// WaitGroup MUST unblock quickly
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("acceptLoop failed to exit promptly upon context cancellation")
	}
}
