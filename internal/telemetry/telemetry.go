// Package telemetry collects kyria's process-wide operation counters for the STATS command. Every
// counter is atomic so the many connection and coordinator goroutines can record events lock-free — a
// mutex here would serialize the whole server on the hot path. Counters are in-memory only: they
// reset on restart, and an external scraper is expected to own any history.
package telemetry

import (
	"slices"
	"sync/atomic"
	"time"
)

// histogram bins observations into fixed buckets so quantiles can be estimated without storing every
// sample. Counters are atomic, so Observe stays lock-free like the rest of the package.
type histogram struct {
	bounds []time.Duration // upper bounds, ascending; immutable after construction
	counts []atomic.Int64  // len(bounds)+1 — the extra one is the overflow bucket
	sum    atomic.Int64    // total nanoseconds observed
	count  atomic.Int64    // total observations
}

// commandStats holds one command's counters. Grouping per command is what gives the metrics a
// {command} dimension without a separately-named counter for every (command, outcome) pair. hits and
// misses are only meaningful for lookups (GET); they stay zero for the rest.
type commandStats struct {
	total  atomic.Int64
	hits   atomic.Int64
	misses atomic.Int64
	errors atomic.Int64
	// latency is per command, matching the {command} dimension of the counters above. One shared
	// histogram would blend GET and SET into a single distribution, which says nothing useful: a SET
	// waits on W acks and a GET on R, so their shapes genuinely differ.
	latency *histogram
}

// gauge is a value telemetry does not own: it is sampled from its owner at Snapshot time rather than
// accumulated. Registering a func rather than a number is what keeps the reading live — the body runs
// on every Snapshot, so it always reflects current state instead of freezing at registration.
type gauge struct {
	name string
	fn   func() int64
}

// defaultBuckets are the latency bucket bounds for a command histogram, tuned to where an in-memory
// cache actually operates: a 1–2.5–5 progression per decade starting at 250ns, so the fine buckets
// sit on the sub-microsecond local hot path (the Phase-0 benchmarks measure ops at 0.3–2µs) and
// coarsen up to 500ms — high enough that a pathological clustered request lands in a real bucket
// rather than the overflow. Bounds must stay ascending: newHistogram assumes it and Quantile walks
// them in order. (A prior ladder started at 100µs, above the entire signal, which pinned every
// percentile to 100µs — the resolution has to bracket the data, not sit above it.)
var defaultBuckets = []time.Duration{
	250 * time.Nanosecond, 500 * time.Nanosecond,
	time.Microsecond, 2500 * time.Nanosecond, 5 * time.Microsecond,
	10 * time.Microsecond, 25 * time.Microsecond, 50 * time.Microsecond, 100 * time.Microsecond,
	250 * time.Microsecond, 500 * time.Microsecond,
	time.Millisecond, 5 * time.Millisecond, 25 * time.Millisecond,
	100 * time.Millisecond, 500 * time.Millisecond,
}

// newHistogram returns a histogram over the given ascending bounds. The bounds are cloned rather than
// aliased, so the histogram owns them and no caller can retroactively re-label tallies by mutating the
// slice it passed in. counts gets len(bounds)+1 entries — the extra one is the overflow bucket that
// catches everything above the largest bound; without it the slowest observations would vanish, which
// is precisely the tail a histogram exists to show.
func newHistogram(bounds []time.Duration) *histogram {
	return &histogram{
		bounds: slices.Clone(bounds),
		counts: make([]atomic.Int64, len(bounds)+1),
	}
}

// Observe records one duration: it bumps the total count and the nanosecond sum, then increments the
// bucket d falls into — the first bound where d <= bound, or the overflow bucket when d exceeds them
// all. The bucket search is a linear scan, which beats binary search at this few bounds (no branch
// misprediction, cache-friendly); Prometheus does the same for small bucket sets.
//
// It never stores d itself, only a tally, which is why memory stays proportional to the bucket count
// rather than the observation count — and why quantiles come back as estimates. Nil-safe, and
// lock-free so the many connection goroutines can call it concurrently.
func (h *histogram) Observe(d time.Duration) {
	if h == nil {
		return
	}

	h.count.Add(1)
	h.sum.Add(int64(d))

	for i, b := range h.bounds {
		if d <= b {
			h.counts[i].Add(1)
			return
		}
	}
	h.counts[len(h.bounds)].Add(1)
}

// Quantile estimates the duration below which q of the observations fall — Quantile(0.99) is the p99.
// It turns q into a rank over the total count, walks the buckets accumulating their tallies, and
// returns the upper bound of the bucket where the running total first reaches that rank. Falling past
// the last bound means the rank landed in the overflow bucket, so the largest bound is returned.
//
// The result is an UPPER estimate — "p99 is at most 5ms" — because the raw samples are gone and only
// bucket edges can be reported; resolution is exactly the bucket granularity. A quantile pinned at the
// largest bound is the overflow bucket answering: the bounds are too narrow, not evidence that latency
// equals that value. Returns 0 when nothing has been observed. Nil-safe.
func (h *histogram) Quantile(q float64) time.Duration {
	if h == nil {
		return 0
	}

	t := h.count.Load()
	if t == 0 {
		return 0
	}

	rank := q * float64(t)

	var running int64
	for i, b := range h.bounds {
		running += h.counts[i].Load()
		if float64(running) >= rank {
			return b
		}
	}

	return h.bounds[len(h.bounds)-1]
}

// Telemetry holds the live counters, keyed by command. It must be used as a pointer, since its
// atomic fields must not be copied. Every method is nil-safe, so a Handler built without telemetry
// (tests, standalone construction) can call them freely as no-ops.
type Telemetry struct {
	startedAt  time.Time
	names      []string                 // registration order, so Snapshot/STATS output is stable
	commands   map[string]*commandStats // written once in New, read-only after
	gauges     []gauge
	eventNames []string
	events     map[string]*atomic.Int64
}

// New returns a Telemetry tracking the given commands, with its uptime clock started. The command
// set is fixed here and never grows: the map is written once and only read afterwards, which is what
// keeps recording lock-free. It also bounds cardinality — a command that was never registered is
// silently ignored rather than creating a new entry, so a typo or a rogue verb cannot grow the
// metric set without bound.
func New(commands ...string) *Telemetry {
	cmds := make(map[string]*commandStats, len(commands))
	for _, c := range commands {
		cmds[c] = &commandStats{
			latency: newHistogram(defaultBuckets),
		}
	}
	return &Telemetry{
		startedAt:  time.Now(),
		names:      commands,
		commands:   cmds,
		gauges:     make([]gauge, 0),
		eventNames: make([]string, 0),
		events:     map[string]*atomic.Int64{},
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

func (t *Telemetry) RegisterEvents(events []string) {
	if t == nil {
		return
	}

	for _, e := range events {
		if t.events[e] != nil {
			continue
		}

		t.eventNames = append(t.eventNames, e)
		t.events[e] = &atomic.Int64{}
	}
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

// RecordDuration observes how long one occurrence of command took. Like the counters it funnels
// through stats, so a nil receiver or an unregistered command is a no-op.
func (t *Telemetry) RecordDuration(command string, d time.Duration) {
	cs := t.stats(command)
	if cs != nil {
		cs.latency.Observe(d)
	}
}

func (t *Telemetry) RecordEvent(name string) {
	if t == nil {
		return
	}
	e := t.events[name]
	if e != nil {
		e.Add(1)
	}
}

// CommandSnapshot is one command's counters at an instant.
type CommandSnapshot struct {
	Command                     string
	Total, Hits, Misses, Errors int64
	P50, P99                    time.Duration
}

// GaugeSnapshot is one sampled gauge at an instant.
type GaugeSnapshot struct {
	Name  string
	Value int64
}

type EventSnapshot struct {
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
	Events   []EventSnapshot
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
			P50:     c.latency.Quantile(0.5),
			P99:     c.latency.Quantile(0.99),
		})
	}

	g := make([]GaugeSnapshot, 0, len(t.gauges))
	for _, f := range t.gauges {
		g = append(g, GaugeSnapshot{
			Name:  f.name,
			Value: f.fn(),
		})
	}

	e := make([]EventSnapshot, 0, len(t.events))
	for _, n := range t.eventNames {
		e = append(e, EventSnapshot{
			Name:  n,
			Value: t.events[n].Load(),
		})
	}

	return Snapshot{
		Uptime:   time.Since(t.startedAt),
		Commands: cmds,
		Gauges:   g,
		Events:   e,
	}
}
