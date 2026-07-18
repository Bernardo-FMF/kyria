// Package telemetry collects kyria's process-wide operation counters for the STATS command. Every
// counter is atomic so the many connection and coordinator goroutines can record events lock-free — a
// mutex here would serialize the whole server on the hot path. Counters are in-memory only: they
// reset on restart, and an external scraper is expected to own any history.
package telemetry

import (
	"sync/atomic"
	"time"
)

// commandStats holds one command's counters. Grouping per command is what gives the metrics a
// {command} dimension without a separately-named counter for every (command, outcome) pair. hits and
// misses are only meaningful for lookups (GET); they stay zero for the rest.
type commandStats struct {
	total  atomic.Int64
	hits   atomic.Int64
	misses atomic.Int64
	errors atomic.Int64
}

// gauge is a value telemetry does not own: it is sampled from its owner at Snapshot time rather than
// accumulated. Registering a func rather than a number is what keeps the reading live — the body runs
// on every Snapshot, so it always reflects current state instead of freezing at registration.
type gauge struct {
	name string
	fn   func() int64
}

// Telemetry holds the live counters, keyed by command. It must be used as a pointer, since its
// atomic fields must not be copied. Every method is nil-safe, so a Handler built without telemetry
// (tests, standalone construction) can call them freely as no-ops.
type Telemetry struct {
	startedAt time.Time
	names     []string                 // registration order, so Snapshot/STATS output is stable
	commands  map[string]*commandStats // written once in New, read-only after
	gauges    []gauge
}

// New returns a Telemetry tracking the given commands, with its uptime clock started. The command
// set is fixed here and never grows: the map is written once and only read afterwards, which is what
// keeps recording lock-free. It also bounds cardinality — a command that was never registered is
// silently ignored rather than creating a new entry, so a typo or a rogue verb cannot grow the
// metric set without bound.
func New(commands ...string) *Telemetry {
	cmds := make(map[string]*commandStats, len(commands))
	for _, c := range commands {
		cmds[c] = &commandStats{}
	}
	return &Telemetry{
		startedAt: time.Now(),
		names:     commands,
		commands:  cmds,
		gauges:    make([]gauge, 0),
	}
}

// RegisterGauge adds a sampled value under name. The component that owns the value supplies fn, so
// telemetry never mirrors state it does not own. fn runs on the STATS path: it must be cheap,
// concurrency-safe, and must not take a lock its caller might already hold.
//
// Register during startup, before serving begins — the slice is written here and only read
// afterwards, which is what keeps Snapshot free of synchronization.
func (t *Telemetry) RegisterGauge(name string, fn func() int64) {
	if t == nil {
		return
	}

	g := gauge{
		name: name,
		fn:   fn,
	}

	t.gauges = append(t.gauges, g)
}

// stats returns the counters for command, or nil when the receiver is nil or the command was never
// registered. Every recorder funnels through it, so both cases collapse to a no-op.
func (t *Telemetry) stats(command string) *commandStats {
	if t == nil {
		return nil
	}
	return t.commands[command]
}

// RecordCommand counts one occurrence of command — the traffic metric ("how many did this node
// receive"), independent of how the command turned out.
func (t *Telemetry) RecordCommand(command string) {
	cs := t.stats(command)
	if cs != nil {
		cs.total.Add(1)
	}
}

// RecordHit counts one lookup of command that found a value.
func (t *Telemetry) RecordHit(command string) {
	cs := t.stats(command)
	if cs != nil {
		cs.hits.Add(1)
	}
}

// RecordMiss counts one lookup of command that found nothing.
func (t *Telemetry) RecordMiss(command string) {
	cs := t.stats(command)
	if cs != nil {
		cs.misses.Add(1)
	}
}

// RecordError counts one occurrence of command that failed.
func (t *Telemetry) RecordError(command string) {
	cs := t.stats(command)
	if cs != nil {
		cs.errors.Add(1)
	}
}

// CommandSnapshot is one command's counters at an instant.
type CommandSnapshot struct {
	Command                     string
	Total, Hits, Misses, Errors int64
}

// GaugeSnapshot is one sampled gauge at an instant.
type GaugeSnapshot struct {
	Name  string
	Value int64
}

// Snapshot is a plain, copyable read of the counters at one instant — safe to pass around and
// format, unlike the atomics it is taken from. Each counter is loaded independently, so the set is
// only approximately consistent (no cross-counter atomicity), which is fine for stats.
type Snapshot struct {
	Uptime   time.Duration
	Commands []CommandSnapshot
	Gauges   []GaugeSnapshot
}

// Snapshot reads the current counters and uptime into a Snapshot for the STATS command. Commands come
// back in registration order, so the output is stable from call to call. Each registered gauge is
// SAMPLED here — its function runs now, which is what makes gauge values current rather than frozen
// at the moment they were registered.
func (t *Telemetry) Snapshot() Snapshot {
	if t == nil {
		return Snapshot{}
	}

	cmds := make([]CommandSnapshot, 0, len(t.names))
	for _, n := range t.names {
		c := t.commands[n]
		cmds = append(cmds, CommandSnapshot{
			Command: n,
			Total:   c.total.Load(),
			Hits:    c.hits.Load(),
			Misses:  c.misses.Load(),
			Errors:  c.errors.Load(),
		})
	}

	g := make([]GaugeSnapshot, 0, len(t.gauges))
	for _, f := range t.gauges {
		g = append(g, GaugeSnapshot{
			Name:  f.name,
			Value: f.fn(),
		})
	}

	return Snapshot{
		Uptime:   time.Since(t.startedAt),
		Commands: cmds,
		Gauges:   g,
	}
}
