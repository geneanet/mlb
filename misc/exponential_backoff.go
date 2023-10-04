package misc

import (
	"context"
	"time"
)

type ExponentialBackoff struct {
	default_duration time.Duration
	max_duration     time.Duration
	current_duration time.Duration
	backoff_factor   float64
}

func NewExponentialBackoff(default_duration time.Duration, max_duration time.Duration, backoff_factor float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		default_duration: default_duration,
		max_duration:     max_duration,
		current_duration: default_duration,
		backoff_factor:   backoff_factor,
	}
}

// Reset the duration to the default value
func (eb *ExponentialBackoff) Reset() {
	eb.current_duration = eb.default_duration
}

// Return the current duration and increase it for the next use
func (eb *ExponentialBackoff) Get() time.Duration {
	duration := eb.current_duration

	if eb.current_duration < eb.max_duration {
		eb.current_duration = time.Duration(float64(eb.current_duration) * eb.backoff_factor)
		if eb.current_duration > eb.max_duration {
			eb.current_duration = eb.max_duration
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
