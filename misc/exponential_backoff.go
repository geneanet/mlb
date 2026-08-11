package misc

import (
	"context"
	"time"
)

// ExponentialBackoff implements an exponential backoff algorithm.
type ExponentialBackoff struct {
	defaultDuration time.Duration
	maxDuration     time.Duration
	currentDuration time.Duration
	backoffFactor   float64
}

// NewExponentialBackoff creates a new ExponentialBackoff instance.
func NewExponentialBackoff(defaultDuration time.Duration, maxDuration time.Duration, backoffFactor float64) *ExponentialBackoff {
	if backoffFactor < 1.0 {
		backoffFactor = 1.0
	}
	return &ExponentialBackoff{
		defaultDuration: defaultDuration,
		maxDuration:     maxDuration,
		currentDuration: defaultDuration,
		backoffFactor:   backoffFactor,
	}
}

// Reset resets the duration to the initial default value.
func (eb *ExponentialBackoff) Reset() {
	eb.currentDuration = eb.defaultDuration
}

// Get returns the current duration and increases it for the next call according to the backoff factor.
func (eb *ExponentialBackoff) Get() time.Duration {
	duration := eb.currentDuration

	if eb.currentDuration < eb.maxDuration {
		eb.currentDuration = time.Duration(float64(eb.currentDuration) * eb.backoffFactor)
		if eb.currentDuration > eb.maxDuration {
			eb.currentDuration = eb.maxDuration
		}
	}

	return duration
}

// Sleep blocks for the current duration or until the context is cancelled.
// It also increases the duration for the next call.
func (eb *ExponentialBackoff) Sleep(ctx context.Context) {
	d := eb.Get()
	if d <= 0 {
		d = 100 * time.Millisecond // safeguard against 0 duration
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
