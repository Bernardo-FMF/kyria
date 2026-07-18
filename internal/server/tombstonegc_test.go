package server

import (
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/telemetry"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// tombBlob encodes a single tombstone stamped with deletedAt — the on-store form of a deleted key.
func tombBlob(clock vclock.Clock, deletedAt int64) []byte {
	return version.Encode([]version.Version{version.Tombstone(clock, deletedAt)})
}

// TestReapable: a set is reapable only when it is non-empty, holds no live version, and every
// tombstone in it is strictly older than the grace period.
func TestReapable(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	grace := time.Hour
	tomb := func(deletedAt int64) version.Version { return version.Tombstone(vclock.Clock{"a": 1}, deletedAt) }
	val := version.Version{Value: []byte("v"), Clock: vclock.Clock{"a": 1}}

	agedAt := now.Add(-2 * time.Hour).Unix()  // well past grace
	freshAt := now.Add(-time.Minute).Unix()   // within grace
	exactAt := now.Add(-grace).Unix()         // exactly grace old — boundary, NOT reapable (<=)
	olderAt := now.Add(-3 * time.Hour).Unix() // also past grace

	cases := []struct {
		name     string
		versions []version.Version
		want     bool
	}{
		{"empty set", nil, false},
		{"live value only", []version.Version{val}, false},
		{"live value concurrent with an aged tombstone", []version.Version{val, tomb(agedAt)}, false},
		{"single fresh tombstone", []version.Version{tomb(freshAt)}, false},
		{"tombstone exactly grace old", []version.Version{tomb(exactAt)}, false},
		{"single aged tombstone", []version.Version{tomb(agedAt)}, true},
		{"aged plus fresh tombstones", []version.Version{tomb(agedAt), tomb(freshAt)}, false},
		{"two aged tombstones", []version.Version{tomb(agedAt), tomb(olderAt)}, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := reapable(c.versions, now, grace); got != c.want {
				t.Errorf("reapable(%s) = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

// TestTombstoneGC_SweepReapsOnlyAgedTombstones: one sweep removes keys whose set is entirely
// tombstones past grace, and leaves everything else — a fresh tombstone, a live value, and a live
// value that is a concurrent sibling of a tombstone.
func TestTombstoneGC_SweepReapsOnlyAgedTombstones(t *testing.T) {
	s := store.NewSharded(4)
	now := time.Unix(1_700_000_000, 0)
	grace := time.Hour

	s.Set("aged", tombBlob(vclock.Clock{"a": 1}, now.Add(-2*time.Hour).Unix()))  // reaped
	s.Set("fresh", tombBlob(vclock.Clock{"a": 1}, now.Add(-time.Minute).Unix())) // kept: within grace
	s.Set("live", verBlob("v", vclock.Clock{"a": 1}))                            // kept: live value
	s.Set("mixed", version.Encode([]version.Version{                             // kept: live sibling
		{Value: []byte("v"), Clock: vclock.Clock{"a": 1}},
		version.Tombstone(vclock.Clock{"b": 1}, now.Add(-2*time.Hour).Unix()),
	}))

	gc := &TombstoneGC{store: s, grace: grace}

	if reaped := gc.sweep(now); reaped != 1 {
		t.Errorf("sweep reaped %d keys, want 1 (only the aged tombstone)", reaped)
	}
	if _, ok := s.Get("aged"); ok {
		t.Error("aged tombstone was not reaped")
	}
	for _, key := range []string{"fresh", "live", "mixed"} {
		if _, ok := s.Get(key); !ok {
			t.Errorf("key %q was reaped, want it kept", key)
		}
	}
}

// revivingStore wraps a Store and, on the first DeleteIf call, overwrites the key with a live value
// BEFORE delegating — simulating a write that lands between the sweep's collect and delete phases.
type revivingStore struct {
	store.Store
	revived bool
}

func (r *revivingStore) DeleteIf(key string, pred func(old []byte) bool) bool {
	if !r.revived {
		r.revived = true
		r.Store.Set(key, verBlob("revived", vclock.Clock{"a": 2}))
	}
	return r.Store.DeleteIf(key, pred)
}

// TestTombstoneGC_SweepRechecksRevivedKey: a key collected as an aged tombstone but revived by a live
// write before phase 2 must NOT be reaped — DeleteIf re-runs reapable on the current value under the
// lock, so the resurrected key survives. This is the whole reason the sweep uses DeleteIf.
func TestTombstoneGC_SweepRechecksRevivedKey(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	base := store.NewSharded(1)
	base.Set("k", tombBlob(vclock.Clock{"a": 1}, now.Add(-2*time.Hour).Unix())) // aged → collected in phase 1
	s := &revivingStore{Store: base}

	gc := &TombstoneGC{store: s, grace: time.Hour}
	if reaped := gc.sweep(now); reaped != 0 {
		t.Errorf("sweep reaped %d keys, want 0 — the key was revived before the delete", reaped)
	}
	if _, ok := base.Get("k"); !ok {
		t.Error("revived key was reaped; phase-2 re-check failed to protect it")
	}
}

// TestTombstoneGC_RunReapsPeriodically: the background loop sweeps on its interval and reaps an aged
// tombstone with no direct sweep call.
func TestTombstoneGC_RunReapsPeriodically(t *testing.T) {
	s := store.NewSharded(4)
	s.Set("aged", tombBlob(vclock.Clock{"a": 1}, time.Now().Add(-2*time.Hour).Unix()))

	gc := NewTombstoneGC(s, time.Hour, 5*time.Millisecond, nil)
	defer gc.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := s.Get("aged"); !ok {
			return // reaped
		}
		if time.Now().After(deadline) {
			t.Fatal("aged tombstone was not reaped within the timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTombstoneGC_StopIsIdempotent: Stop can be called repeatedly without panicking (double close)
// or hanging (receive on the already-closed done channel).
func TestTombstoneGC_StopIsIdempotent(t *testing.T) {
	gc := NewTombstoneGC(store.NewSharded(1), time.Hour, time.Hour, nil)
	gc.Stop()
	gc.Stop()
}

// TestTombstoneGC_RecordsReaps: the sweep records one event per tombstone actually reaped — not per
// key examined — so the counter tracks real reclamation rather than sweep activity.
func TestTombstoneGC_RecordsReaps(t *testing.T) {
	s := store.NewSharded(4)
	now := time.Unix(1_700_000_000, 0)
	tel := telemetry.New()
	tel.RegisterEvents(ReplicationEvents)

	s.Set("aged1", tombBlob(vclock.Clock{"a": 1}, now.Add(-2*time.Hour).Unix())) // reaped
	s.Set("aged2", tombBlob(vclock.Clock{"a": 1}, now.Add(-3*time.Hour).Unix())) // reaped
	s.Set("fresh", tombBlob(vclock.Clock{"a": 1}, now.Add(-time.Minute).Unix())) // examined, not reaped
	s.Set("live", verBlob("v", vclock.Clock{"a": 1}))                            // examined, not reaped

	gc := &TombstoneGC{store: s, grace: time.Hour, telemetry: tel}
	if reaped := gc.sweep(now); reaped != 2 {
		t.Fatalf("sweep reaped %d, want 2", reaped)
	}

	if got := eventCount(t, tel.Snapshot(), evTombstonesReaped); got != 2 {
		t.Errorf("tombstones_reaped = %d, want 2 (one per key actually reaped)", got)
	}
}
