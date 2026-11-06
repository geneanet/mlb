package memcache

import (
	"context"
	"mlb/backend"
	"mlb/testutil"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

func TestMemcacheProxy_AcceptLoop_Backoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	mockLn := testutil.NewMockFailingListener(ln, 2)

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
		return mockLn.FailCount() == 2
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

	mockLn := testutil.NewMockFailingListener(ln, 1000)

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
		return mockLn.FailCount() >= 1
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

func TestMemcacheProxy_MultipleListeners_Independence(t *testing.T) {
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln1.Close()

	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nopLogger := zerolog.Nop()
	p := &MemcacheProxy{
		id:     "test-mc-multi-ln",
		log:    nopLogger,
		ctx:    ctx,
		cancel: cancel,
	}

	feMetrics := &Metrics{
		processed: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_proc_mc_mln"}),
		active:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fe_act_mc_mln"}),
		bytesIn:   prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bin_mc_mln"}),
		bytesOut:  prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_bout_mc_mln"}),
		cnxErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "fe_err_mc_mln"}),
	}

	var wg1, wg2 sync.WaitGroup
	wg1.Add(1)
	wg2.Add(1)
	go p.acceptLoop(ln1, ln1.Addr().String(), &wg1, feMetrics)
	go p.acceptLoop(ln2, ln2.Addr().String(), &wg2, feMetrics)

	// Close ln1: acceptLoop for ln1 should exit cleanly without cancelling the proxy context
	// or blocking/waiting prematurely on connectionsWG.
	_ = ln1.Close()

	// Wait deterministically for ln1's acceptLoop to exit
	done1 := make(chan struct{})
	go func() {
		wg1.Wait()
		close(done1)
	}()
	select {
	case <-done1:
	case <-time.After(1 * time.Second):
		t.Fatal("ln1 acceptLoop did not exit after listener closed")
	}

	if p.ctx.Err() != nil {
		t.Fatalf("closing one listener cancelled the entire proxy context: %v", p.ctx.Err())
	}

	// Dial ln2: should connect and accept without WaitGroup misuse panic
	conn, err := net.Dial("tcp", ln2.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial active listener ln2: %v", err)
	}
	_ = conn.Close()

	cancel()
	_ = ln2.Close()
	wg2.Wait()
}
