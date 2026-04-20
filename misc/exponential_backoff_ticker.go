package misc

import "time"

type ExponentialBackoffTicker struct {
	backoff *ExponentialBackoff
	ticker  *time.Ticker
	period  time.Duration
	C       <-chan time.Time
}

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

func (eb *ExponentialBackoffTicker) Stop() {
	eb.ticker.Stop()
}

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
