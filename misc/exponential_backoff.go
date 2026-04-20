package misc

import (
	"context"
	"time"
)

type ExponentialBackoff struct {
	defaultDuration time.Duration
	maxDuration     time.Duration
	currentDuration time.Duration
	backoffFactor   float64
}

func NewExponentialBackoff(defaultDuration time.Duration, maxDuration time.Duration, backoffFactor float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		defaultDuration: defaultDuration,
		maxDuration:     maxDuration,
		currentDuration: defaultDuration,
		backoffFactor:   backoffFactor,
	}
}

// Reset the duration to the default value
func (eb *ExponentialBackoff) Reset() {
	eb.currentDuration = eb.defaultDuration
}

// Return the current duration and increase it for the next use
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

// Sleep for the current duration and increase it for the next use
func (eb *ExponentialBackoff) Sleep(ctx context.Context) {
	timer := time.NewTimer(eb.Get())
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
