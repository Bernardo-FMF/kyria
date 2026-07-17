package telemetry

import (
	"sync"
	"testing"
	"time"
)

// TestTelemetry_RecordsCounts: the Record methods increment their counters, and Snapshot reads them
// back with a positive uptime.
func TestTelemetry_RecordsCounts(t *testing.T) {
	tel := New()
	for range 2 {
		tel.RecordGet()
	}
	tel.RecordSet()
	for range 3 {
		tel.RecordDelete()
	}

	time.Sleep(time.Millisecond) // let uptime accrue a measurable amount
	s := tel.Snapshot()
	if s.Gets != 2 || s.Sets != 1 || s.Deletes != 3 {
		t.Errorf("Snapshot = {Gets:%d Sets:%d Deletes:%d}, want {2 1 3}", s.Gets, s.Sets, s.Deletes)
	}
	if s.Uptime <= 0 {
		t.Errorf("Uptime = %v, want a positive duration", s.Uptime)
	}
}

// TestTelemetry_NilIsSafe: a nil *Telemetry no-ops on every Record call and returns the zero
// Snapshot, so a Handler built without telemetry never panics.
func TestTelemetry_NilIsSafe(t *testing.T) {
	var tel *Telemetry // nil

	tel.RecordGet()
	tel.RecordSet()
	tel.RecordDelete()

	if got := tel.Snapshot(); got != (Snapshot{}) {
		t.Errorf("nil Snapshot = %+v, want the zero Snapshot", got)
	}
}

// TestTelemetry_ConcurrentRecords: concurrent Record calls are race-free (atomic counters) and no
// increment is lost. Meaningful under -race.
func TestTelemetry_ConcurrentRecords(t *testing.T) {
	tel := New()
	const goroutines, each = 50, 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range each {
				tel.RecordGet()
			}
		}()
	}
	wg.Wait()

	if got := tel.Snapshot().Gets; got != goroutines*each {
		t.Errorf("Gets after concurrent records = %d, want %d", got, goroutines*each)
	}
}
