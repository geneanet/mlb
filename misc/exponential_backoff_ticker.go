package misc

import "time"

type ExponentialBackoffTicker struct {
	backoff *ExponentialBackoff
	ticker  *time.Ticker
	period  time.Duration
	C       <-chan time.Time
}

func NewExponentialBackoffTicker(default_duration time.Duration, max_duration time.Duration, backoff_factor float64) *ExponentialBackoffTicker {
	backoff := NewExponentialBackoff(default_duration, max_duration, backoff_factor)
	ticker := time.NewTicker(backoff.Get())

	return &ExponentialBackoffTicker{
		backoff: backoff,
		ticker:  ticker,
		period:  default_duration,
		C:       ticker.C,
	}
}

func (eb *ExponentialBackoffTicker) Stop() {
	eb.ticker.Stop()
}

func (eb *ExponentialBackoffTicker) Reset() (time.Duration, bool) {
	old_period := eb.period
	eb.backoff.Reset()
	eb.period = eb.backoff.Get()

	eb.ticker.Reset(eb.period)

	if eb.period != old_period {
		return eb.period, true
	} else {
		return eb.period, false
	}
}

func (eb *ExponentialBackoffTicker) ApplyBackoff() (time.Duration, bool) {
	old_period := eb.period
	eb.period = eb.backoff.Get()
	if eb.period != old_period {
		eb.ticker.Reset(eb.period)
		return eb.period, true
	} else {
		return eb.period, false
	}
}
