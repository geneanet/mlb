package main

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"
)

// MySQL Backend
type Backend struct {
	address        string
	weight         int
	tags           []string
	user           string
	password       string
	period         float64
	default_period float64
	max_period     float64
	backoff_factor float64
	status         string
	ticker         *time.Ticker
	stop_chan      chan bool
	running        bool
	db             *sql.DB
}

func newBackend(address string, weight int, tags []string, user string, password string, default_period float64, max_period float64, backoff_factor float64) *Backend {
	return &Backend{
		address:        address,
		weight:         weight,
		tags:           tags,
		user:           user,
		password:       password,
		period:         default_period,
		default_period: default_period,
		max_period:     max_period,
		backoff_factor: backoff_factor,
		status:         "unk",
		stop_chan:      make(chan bool),
		running:        false,
	}
}

func (b *Backend) _fetchStatus() (ret_s string, ret_e error) {
	defer func() {
		if r := recover(); r != nil {
			ret_s = "err"
			ret_e = r.(error)

			b._applyBackoff()
		}
	}()

	log.Debug().Str("address", b.address).Msg("Probing Backend")

	result, err := b.db.Query("SELECT @@read_only")
	panicIfErr(err)
	defer result.Close()

	var read_only bool

	result.Next()
	err = result.Scan(&read_only)
	panicIfErr(err)

	b._resetPeriod()

	if read_only {
		return "ro", nil
	} else {
		return "rw", nil
	}
}

func (b *Backend) _updateStatus() {
	new_status, err := b._fetchStatus()

	if err != nil {
		log.Error().Str("address", b.address).Err(err).Msg("Error while fetching status from backend")
	}

	if new_status != b.status {
		log.Info().Str("address", b.address).Str("old_status", b.status).Str("new_status", new_status).Msg("Backend status changed")
	}

	b.status = new_status
}

func (b *Backend) startPolling() error {
	if b.running {
		return nil
	}
	b.running = true

	db, err := sql.Open("mysql", b.user+":"+b.password+"@tcp("+b.address+")/")
	if err != nil {
		return err
	}
	b.db = db

	b.period = b.default_period
	b.ticker = time.NewTicker(time.Duration(b.period * float64(time.Second)))

	go func() {
		defer func() { b.running = false }()

		for {
			b._updateStatus()

			// Wait next iteration
			select {
			case <-b.stop_chan:
				return
			case <-b.ticker.C:
			}
		}
	}()

	return nil
}

func (b *Backend) stopPolling() {
	if !b.running {
		return
	}

	b.db.Close()
	b.ticker.Stop()
	b.stop_chan <- true
}

func (b *Backend) _updatePeriod(period float64) {
	if b.running && (b.period != period) {
		b.period = period
		b.ticker.Reset(time.Duration(b.period * float64(time.Second)))

		log.Warn().Float64("period", b.period).Str("address", b.address).Msg("Updating Backend probing period")
	}
}

func (b *Backend) _resetPeriod() {
	b._updatePeriod(b.default_period)
}

func (b *Backend) _applyBackoff() {
	new_period := b.period * b.backoff_factor
	if new_period > b.max_period {
		new_period = b.max_period
	}
	b._updatePeriod(new_period)
}
