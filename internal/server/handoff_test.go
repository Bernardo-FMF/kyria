package server

import (
	"bytes"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/vclock"
)

// TestHintStore_AddSizeSnapshot: adding parks a hint per target+key, overwriting an
// existing target+key keeps the latest (not a new entry), and snapshot returns an
// independent copy that can be mutated without touching the store.
func TestHintStore_AddSizeSnapshot(t *testing.T) {
	s := NewHintStore()
	s.add("b", "k1", verBlob("v1", vclock.Clock{"a": 1}))
	s.add("b", "k2", verBlob("v2", vclock.Clock{"a": 1}))
	s.add("c", "k1", verBlob("v3", vclock.Clock{"a": 1}))

	if got := s.Size(); got != 3 {
		t.Fatalf("size = %d, want 3", got)
	}

	// Overwriting an existing target+key stores the newer blob without growing size.
	newer := verBlob("v1b", vclock.Clock{"a": 2})
	s.add("b", "k1", newer)
	if got := s.Size(); got != 3 {
		t.Errorf("size after overwrite = %d, want 3 (not a new entry)", got)
	}

	snap := s.snapshot()
	if !bytes.Equal(snap["b"]["k1"], newer) {
		t.Error("snapshot did not reflect the overwritten (newer) blob")
	}

	// A snapshot is a copy: mutating it must not change the store.
	delete(snap, "b")
	if got := s.Size(); got != 3 {
		t.Errorf("mutating the snapshot changed the store: size = %d, want 3", got)
	}
}

// TestHintStore_RemoveConditional: remove drops a hint only when the parked blob still
// matches what was delivered — a newer write that landed mid-delivery must not be lost.
func TestHintStore_RemoveConditional(t *testing.T) {
	s := NewHintStore()
	old := verBlob("old", vclock.Clock{"a": 1})
	s.add("b", "k", old)

	// A newer write lands before the replayer finishes delivering the old blob.
	newer := verBlob("new", vclock.Clock{"a": 2})
	s.add("b", "k", newer)

	// Removing with the STALE blob is a no-op: the newer, undelivered hint stays.
	s.remove("b", "k", old)
	if got := s.Size(); got != 1 {
		t.Fatalf("stale remove dropped the newer hint: size = %d, want 1", got)
	}
	if snap := s.snapshot(); !bytes.Equal(snap["b"]["k"], newer) {
		t.Error("the newer blob should still be parked after a stale remove")
	}

	// Removing with the matching blob clears it and cleans up the now-empty target.
	s.remove("b", "k", newer)
	if got := s.Size(); got != 0 {
		t.Errorf("size after matching remove = %d, want 0", got)
	}
	if snap := s.snapshot(); len(snap) != 0 {
		t.Errorf("empty target map should be cleaned up, got %v", snap)
	}
}

// TestHintReplayer_ReplayOnce: one synchronous sweep delivers hints to reachable
// targets (dropping them) and leaves hints for targets that are still down.
func TestHintReplayer_ReplayOnce(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["c"] = true // c is still down
	s := NewHintStore()
	s.add("b", "k1", verBlob("v", vclock.Clock{"a": 1}))
	s.add("c", "k2", verBlob("w", vclock.Clock{"a": 1}))

	// Construct the replayer directly (no goroutine) to test the sweep in isolation.
	r := &HintReplayer{store: s, replicator: peer}

	if got := r.replayOnce(); got != 1 {
		t.Errorf("replayOnce delivered %d, want 1 (b delivered, c still down)", got)
	}
	if got := s.Size(); got != 1 {
		t.Errorf("size after replay = %d, want 1 (c's hint kept)", got)
	}

	snap := s.snapshot()
	if _, ok := snap["b"]; ok {
		t.Error("b's hint should be gone after delivery")
	}
	if _, ok := snap["c"]; !ok {
		t.Error("c's hint should remain — c is still down")
	}

	peer.mu.Lock()
	bCount, cCount := peer.replicated["b"], peer.replicated["c"]
	peer.mu.Unlock()
	if bCount != 1 {
		t.Errorf("b was Replicated %d times, want 1", bCount)
	}
	if cCount == 0 {
		t.Error("c was never even attempted")
	}
}

// TestHintReplayer_PeriodicDeliversThenStop: the background loop delivers a parked
// hint on its own, and after Stop no further replay happens.
func TestHintReplayer_PeriodicDeliversThenStop(t *testing.T) {
	peer := newFakeReplicator()
	s := NewHintStore()
	s.add("b", "k", verBlob("v", vclock.Clock{"a": 1}))

	r := NewHintReplayer(s, peer, 5*time.Millisecond)

	// The loop should deliver the hint within a few ticks.
	deadline := time.Now().Add(time.Second)
	for s.Size() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("replayer never delivered the hint: size = %d", s.Size())
		}
		time.Sleep(2 * time.Millisecond)
	}
	peer.mu.Lock()
	delivered := peer.replicated["b"]
	peer.mu.Unlock()
	if delivered == 0 {
		t.Error("hint was cleared but never Replicated")
	}

	// After Stop, park a fresh hint and confirm it lingers — the loop has stopped.
	r.Stop()
	s.add("c", "k2", verBlob("w", vclock.Clock{"a": 1}))
	time.Sleep(30 * time.Millisecond) // several intervals
	if got := s.Size(); got != 1 {
		t.Errorf("a hint was delivered after Stop (size = %d), want it to linger", got)
	}
}

// TestHintReplayer_StopIdempotent: Stop can be called more than once without a panic
// or a hang (the sync.Once guards close(stop), the closed done stays ready).
func TestHintReplayer_StopIdempotent(t *testing.T) {
	r := NewHintReplayer(NewHintStore(), newFakeReplicator(), time.Hour)
	r.Stop()
	r.Stop()
}
