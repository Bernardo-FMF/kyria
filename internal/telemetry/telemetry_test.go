package telemetry

import (
	"sync"
	"testing"
	"time"
)

// find returns the snapshot entry for command, failing the test if it is absent.
func find(t *testing.T, s Snapshot, command string) CommandSnapshot {
	t.Helper()
	for _, c := range s.Commands {
		if c.Command == command {
			return c
		}
	}
	t.Fatalf("no snapshot entry for %q (have %+v)", command, s.Commands)
	return CommandSnapshot{}
}

// TestTelemetry_RecordsPerCommand: each recorder lands on the right command's counters, and commands
// are tracked independently of one another.
func TestTelemetry_RecordsPerCommand(t *testing.T) {
	tel := New("GET", "SET", "DEL")

	tel.RecordCommand("GET")
	tel.RecordCommand("GET")
	tel.RecordHit("GET")
	tel.RecordMiss("GET")
	tel.RecordCommand("SET")
	tel.RecordError("SET")

	time.Sleep(time.Millisecond) // let uptime accrue a measurable amount
	s := tel.Snapshot()

	if got := find(t, s, "GET"); got.Total != 2 || got.Hits != 1 || got.Misses != 1 || got.Errors != 0 {
		t.Errorf("GET = %+v, want {Total:2 Hits:1 Misses:1 Errors:0}", got)
	}
	if got := find(t, s, "SET"); got.Total != 1 || got.Errors != 1 {
		t.Errorf("SET = %+v, want {Total:1 Errors:1}", got)
	}
	if got := find(t, s, "DEL"); got.Total != 0 || got.Hits != 0 {
		t.Errorf("DEL = %+v, want all zero (never recorded)", got)
	}
	if s.Uptime <= 0 {
		t.Errorf("Uptime = %v, want a positive duration", s.Uptime)
	}
}

// TestTelemetry_UnregisteredCommandIgnored: recording a command that was never registered is a
// silent no-op and adds no entry — that fixed set is what bounds cardinality.
func TestTelemetry_UnregisteredCommandIgnored(t *testing.T) {
	tel := New("GET")

	tel.RecordCommand("RGET") // internal verb, never registered
	tel.RecordHit("BOGUS")
	tel.RecordError("")

	s := tel.Snapshot()
	if len(s.Commands) != 1 || s.Commands[0].Command != "GET" {
		t.Fatalf("Snapshot commands = %+v, want only the registered GET", s.Commands)
	}
	if got := s.Commands[0]; got.Total != 0 || got.Hits != 0 || got.Errors != 0 {
		t.Errorf("GET = %+v, want all zero (unregistered records must not leak into it)", got)
	}
}

// TestTelemetry_SnapshotShapeAndOrder: Snapshot returns exactly one entry per registered command, in
// registration order — no padding entries, and no map-iteration shuffle between calls.
func TestTelemetry_SnapshotShapeAndOrder(t *testing.T) {
	names := []string{"GET", "SET", "DEL"}
	tel := New(names...)

	for range 3 { // repeat: a map-ordered implementation would vary across calls
		s := tel.Snapshot()
		if len(s.Commands) != len(names) {
			t.Fatalf("Snapshot has %d entries, want %d (make-with-length pads it with blanks)", len(s.Commands), len(names))
		}
		for i, want := range names {
			if s.Commands[i].Command != want {
				t.Errorf("Commands[%d] = %q, want %q (registration order)", i, s.Commands[i].Command, want)
			}
		}
	}
}

// gaugeValue returns the sampled value for the named gauge, failing the test if it is absent.
func gaugeValue(t *testing.T, s Snapshot, name string) int64 {
	t.Helper()
	for _, g := range s.Gauges {
		if g.Name == name {
			return g.Value
		}
	}
	t.Fatalf("no gauge %q (have %+v)", name, s.Gauges)
	return 0
}

// TestTelemetry_GaugeIsSampledNotFrozen: a gauge registers a FUNCTION, so every Snapshot re-reads the
// current value instead of capturing it at registration time.
func TestTelemetry_GaugeIsSampledNotFrozen(t *testing.T) {
	tel := New()
	live := int64(1)
	tel.RegisterGauge("live", func() int64 { return live })

	if got := gaugeValue(t, tel.Snapshot(), "live"); got != 1 {
		t.Fatalf("gauge = %d, want 1", got)
	}

	live = 42 // the value the gauge watches moves

	if got := gaugeValue(t, tel.Snapshot(), "live"); got != 42 {
		t.Errorf("gauge = %d, want 42 — a gauge must re-sample, not freeze at registration", got)
	}
}

// TestTelemetry_GaugesInRegistrationOrder: gauges come back in the order they were registered, so the
// STATS output is stable.
func TestTelemetry_GaugesInRegistrationOrder(t *testing.T) {
	tel := New()
	tel.RegisterGauge("a", func() int64 { return 1 })
	tel.RegisterGauge("b", func() int64 { return 2 })
	tel.RegisterGauge("c", func() int64 { return 3 })

	s := tel.Snapshot()
	if len(s.Gauges) != 3 {
		t.Fatalf("got %d gauges, want 3", len(s.Gauges))
	}
	for i, want := range []string{"a", "b", "c"} {
		if s.Gauges[i].Name != want {
			t.Errorf("Gauges[%d] = %q, want %q (registration order)", i, s.Gauges[i].Name, want)
		}
	}
}

// TestTelemetry_NilIsSafe: a nil *Telemetry no-ops on every recorder and returns the zero Snapshot,
// so a Handler built without telemetry never panics.
func TestTelemetry_NilIsSafe(t *testing.T) {
	var tel *Telemetry // nil

	tel.RecordCommand("GET")
	tel.RecordHit("GET")
	tel.RecordMiss("GET")
	tel.RecordError("GET")
	tel.RegisterGauge("x", func() int64 { return 1 })

	if got := tel.Snapshot(); got.Uptime != 0 || got.Commands != nil {
		t.Errorf("nil Snapshot = %+v, want the zero Snapshot", got)
	}
}

// TestTelemetry_ConcurrentRecords: concurrent recording is race-free (atomic counters) and no
// increment is lost. Meaningful under -race.
func TestTelemetry_ConcurrentRecords(t *testing.T) {
	tel := New("GET")
	const goroutines, each = 50, 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range each {
				tel.RecordCommand("GET")
				tel.RecordHit("GET")
			}
		}()
	}
	wg.Wait()

	got := find(t, tel.Snapshot(), "GET")
	if got.Total != goroutines*each || got.Hits != goroutines*each {
		t.Errorf("after concurrent records = {Total:%d Hits:%d}, want %d each", got.Total, got.Hits, goroutines*each)
	}
}

// TestHistogram_ObserveTracksCountAndSum: every observation must bump the total count and the
// nanosecond sum, not just a bucket tally — Quantile short-circuits on a zero count, so a histogram
// that only fills buckets reports no percentiles at all.
func TestHistogram_ObserveTracksCountAndSum(t *testing.T) {
	h := newHistogram([]time.Duration{time.Millisecond, 10 * time.Millisecond})

	h.Observe(500 * time.Microsecond)
	h.Observe(2 * time.Millisecond)

	if got := h.count.Load(); got != 2 {
		t.Errorf("count = %d, want 2 (Observe must increment the total)", got)
	}
	if want := int64(2500 * time.Microsecond); h.sum.Load() != want {
		t.Errorf("sum = %d ns, want %d (Observe must accumulate the duration)", h.sum.Load(), want)
	}
}

// TestHistogram_ObserveBuckets: an observation lands in the first bucket whose upper bound it fits
// under, boundary values land in their own bucket (bounds are inclusive), and anything above every
// bound goes to the overflow slot rather than being dropped.
func TestHistogram_ObserveBuckets(t *testing.T) {
	bounds := []time.Duration{time.Millisecond, 10 * time.Millisecond}
	h := newHistogram(bounds)

	h.Observe(500 * time.Microsecond) // → bucket 0
	h.Observe(time.Millisecond)       // → bucket 0 (inclusive upper bound)
	h.Observe(5 * time.Millisecond)   // → bucket 1
	h.Observe(time.Second)            // → overflow

	want := []int64{2, 1, 1}
	for i, w := range want {
		if got := h.counts[i].Load(); got != w {
			t.Errorf("counts[%d] = %d, want %d", i, got, w)
		}
	}
}

// TestHistogram_Quantile: percentiles resolve to the upper bound of the bucket holding the target
// rank, computed against a known distribution.
func TestHistogram_Quantile(t *testing.T) {
	bounds := []time.Duration{100 * time.Microsecond, 250 * time.Microsecond, 500 * time.Microsecond, time.Millisecond}
	h := newHistogram(bounds)

	// 60 ≤100µs, 25 ≤250µs, 10 ≤500µs, 4 ≤1ms, 1 overflow — 100 observations total.
	for range 60 {
		h.Observe(50 * time.Microsecond)
	}
	for range 25 {
		h.Observe(200 * time.Microsecond)
	}
	for range 10 {
		h.Observe(400 * time.Microsecond)
	}
	for range 4 {
		h.Observe(900 * time.Microsecond)
	}
	h.Observe(5 * time.Second) // overflow

	cases := []struct {
		q    float64
		want time.Duration
	}{
		{0.5, 100 * time.Microsecond},  // rank 50 → running 60 covers it
		{0.95, 500 * time.Microsecond}, // rank 95 → 60,85 short; 95 reaches it
		{0.99, time.Millisecond},       // rank 99 → reached in the last bounded bucket
		{0.999, time.Millisecond},      // rank 99.9 → overflow, pinned at the largest bound
	}
	for _, c := range cases {
		if got := h.Quantile(c.q); got != c.want {
			t.Errorf("Quantile(%v) = %v, want %v", c.q, got, c.want)
		}
	}
}

// TestHistogram_EmptyAndNil: an unobserved histogram reports 0 rather than a misleading bound, and a
// nil histogram no-ops on both methods.
func TestHistogram_EmptyAndNil(t *testing.T) {
	h := newHistogram(defaultBuckets)
	if got := h.Quantile(0.99); got != 0 {
		t.Errorf("Quantile on an empty histogram = %v, want 0", got)
	}

	var nilH *histogram
	nilH.Observe(time.Second) // must not panic
	if got := nilH.Quantile(0.99); got != 0 {
		t.Errorf("nil Quantile = %v, want 0", got)
	}
}

// TestHistogram_ConcurrentObserve: Observe is lock-free but must lose no observations. Meaningful
// under -race.
func TestHistogram_ConcurrentObserve(t *testing.T) {
	h := newHistogram(defaultBuckets)
	const goroutines, each = 50, 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range each {
				h.Observe(200 * time.Microsecond)
			}
		}()
	}
	wg.Wait()

	if got := h.count.Load(); got != goroutines*each {
		t.Errorf("count after concurrent Observe = %d, want %d", got, goroutines*each)
	}
}
