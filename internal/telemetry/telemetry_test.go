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

// TestTelemetry_NilIsSafe: a nil *Telemetry no-ops on every recorder and returns the zero Snapshot,
// so a Handler built without telemetry never panics.
func TestTelemetry_NilIsSafe(t *testing.T) {
	var tel *Telemetry // nil

	tel.RecordCommand("GET")
	tel.RecordHit("GET")
	tel.RecordMiss("GET")
	tel.RecordError("GET")

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
