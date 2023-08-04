package checker

import (
	"database/sql"
	"mlb/backend"
	"mlb/misc"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

type CheckerMySQLCheck struct {
	backend        *backend.Backend
	user           string
	password       string
	period         time.Duration
	default_period time.Duration
	max_period     time.Duration
	backoff_factor float64
	status_chan    chan *backend.Backend
	ticker         *time.Ticker
	stop_chan      chan bool
	running        bool
	db             *sql.DB
}

func NewCheckerMySQLCheck(backend *backend.Backend, user string, password string, default_period time.Duration, max_period time.Duration, backoff_factor float64, status_chan chan *backend.Backend) *CheckerMySQLCheck {
	c := &CheckerMySQLCheck{
		backend:        backend,
		user:           user,
		password:       password,
		period:         default_period,
		default_period: default_period,
		max_period:     max_period,
		backoff_factor: backoff_factor,
		status_chan:    status_chan,
		stop_chan:      make(chan bool),
		running:        false,
	}
	return c
}

func (c *CheckerMySQLCheck) UpdateBackend(b *backend.Backend) {
	c.backend.Weight = b.Weight
	c.backend.UpdateTags(b.Tags)

}

func (c *CheckerMySQLCheck) fetchStatus() (ret_s string, ret_e error) {
	defer func() {
		if r := recover(); r != nil {
			ret_s = "err"
			ret_e = r.(error)

			c.applyBackoff()
		}
	}()

	log.Trace().Str("address", c.backend.Address).Msg("Probing Backend")

	result, err := c.db.Query("SELECT @@read_only")
	misc.PanicIfErr(err)
	defer result.Close()

	var read_only bool

	result.Next()
	err = result.Scan(&read_only)
	misc.PanicIfErr(err)

	c.resetPeriod()

	if read_only {
		return "ro", nil
	} else {
		return "rw", nil
	}
}

func (c *CheckerMySQLCheck) updateStatus() {
	new_status, err := c.fetchStatus()

	if err != nil {
		log.Error().Str("address", c.backend.Address).Err(err).Msg("Error while fetching status from backend")
	}

	if new_status != c.backend.Status {
		log.Info().Str("address", c.backend.Address).Str("old_status", c.backend.Status).Str("new_status", new_status).Msg("Backend status changed")
		c.status_chan <- c.backend
	}

	c.backend.Status = new_status
}

func (c *CheckerMySQLCheck) StartPolling() error {
	if c.running {
		return nil
	}
	c.running = true

	db, err := sql.Open("mysql", c.user+":"+c.password+"@tcp("+c.backend.Address+")/")
	if err != nil {
		return err
	}
	c.db = db

	c.period = c.default_period
	c.ticker = time.NewTicker(c.period)

	go func() {
		defer func() { c.running = false }()

		for {
			c.updateStatus()

			// Wait next iteration
			select {
			case <-c.stop_chan:
				return
			case <-c.ticker.C:
			}
		}
	}()

	return nil
}

func (c *CheckerMySQLCheck) StopPolling() {
	if !c.running {
		return
	}

	c.db.Close()
	c.ticker.Stop()
	c.stop_chan <- true
}

func (c *CheckerMySQLCheck) updatePeriod(period time.Duration) {
	if c.running && (c.period != period) {
		c.period = period
		c.ticker.Reset(c.period)

		log.Warn().Dur("period", c.period).Str("address", c.backend.Address).Msg("Updating Backend probing period")
	}
}

func (c *CheckerMySQLCheck) resetPeriod() {
	c.updatePeriod(c.default_period)
}

func (c *CheckerMySQLCheck) applyBackoff() {
	new_period := time.Duration(float64(c.period) * c.backoff_factor)
	if new_period > c.max_period {
		new_period = c.max_period
	}
	c.updatePeriod(new_period)
}
