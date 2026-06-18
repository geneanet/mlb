package misc

import "time"

// ExponentialBackoffTicker is a ticker that uses exponential backoff for its period.
type ExponentialBackoffTicker struct {
	backoff *ExponentialBackoff
	ticker  *time.Ticker
	period  time.Duration
	C       <-chan time.Time
}

// NewExponentialBackoffTicker creates a new ExponentialBackoffTicker.
func NewExponentialBackoffTicker(defaultDuration time.Duration, maxDuration time.Duration, backoffFactor float64) *ExponentialBackoffTicker {
	backoff := NewExponentialBackoff(defaultDuration, maxDuration, backoffFactor)
	ticker := time.NewTicker(backoff.Get())

	return &ExponentialBackoffTicker{
		backoff: backoff,
		ticker:  ticker,
		period:  defaultDuration,
		C:       ticker.C,
	}
}

// Stop stops the ticker.
func (eb *ExponentialBackoffTicker) Stop() {
	eb.ticker.Stop()
}

// Reset resets the backoff to its initial state and returns the new period and whether it changed.
func (eb *ExponentialBackoffTicker) Reset() (time.Duration, bool) {
	oldPeriod := eb.period
	eb.backoff.Reset()
	eb.period = eb.backoff.Get()

	eb.ticker.Reset(eb.period)

	if eb.period != oldPeriod {
		return eb.period, true
	} else {
		return eb.period, false
	}
}

// ApplyBackoff increases the backoff period and returns the new period and whether it changed.
func (eb *ExponentialBackoffTicker) ApplyBackoff() (time.Duration, bool) {
	oldPeriod := eb.period
	eb.period = eb.backoff.Get()
	if eb.period != oldPeriod {
		eb.ticker.Reset(eb.period)
		return eb.period, true
	} else {
		return eb.period, false
	}
}
