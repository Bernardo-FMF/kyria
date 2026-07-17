// Package telemetry collects kyria's process-wide operation counters for the STATS command. Every
// counter is atomic so the many connection and coordinator goroutines can record events lock-free — a
// mutex here would serialize the whole server on the hot path. Counters are in-memory only: they
// reset on restart, and an external scraper is expected to own any history.
package telemetry

import (
	"sync/atomic"
	"time"
)

// Telemetry holds the live counters. It must be used as a pointer, since its atomic fields must not
// be copied. The Record methods are nil-safe, so a Handler built without telemetry (tests, standalone
// construction) can call them freely as no-ops.
type Telemetry struct {
	startedAt time.Time
	gets      atomic.Int64
	sets      atomic.Int64
	deletes   atomic.Int64
}

// New returns a Telemetry with its uptime clock started at the current time.
func New() *Telemetry {
	return &Telemetry{
		startedAt: time.Now(),
	}
}

// RecordGet counts one client GET. Nil-safe: a no-op on a nil receiver.
func (t *Telemetry) RecordGet() {
	if t != nil {
		t.gets.Add(1)
	}
}

// RecordSet counts one client SET. Nil-safe: a no-op on a nil receiver.
func (t *Telemetry) RecordSet() {
	if t != nil {
		t.sets.Add(1)
	}
}

// RecordDelete counts one client DEL. Nil-safe: a no-op on a nil receiver.
func (t *Telemetry) RecordDelete() {
	if t != nil {
		t.deletes.Add(1)
	}
}

// Snapshot is a plain, copyable read of the counters at one instant — safe to pass around and format,
// unlike the atomics it is taken from. Each field is loaded independently, so the set is only
// approximately consistent (no cross-counter atomicity), which is fine for stats.
type Snapshot struct {
	Uptime              time.Duration
	Gets, Sets, Deletes int64
}

// Snapshot reads the current counters and uptime into a Snapshot for the STATS command.
func (t *Telemetry) Snapshot() Snapshot {
	if t == nil {
		return Snapshot{}
	}
	return Snapshot{
		Uptime:  time.Since(t.startedAt),
		Gets:    t.gets.Load(),
		Sets:    t.sets.Load(),
		Deletes: t.deletes.Load(),
	}
}
