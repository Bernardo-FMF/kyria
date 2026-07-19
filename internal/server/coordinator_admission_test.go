package server

import (
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// admissionRejectingStore is a store whose eviction policy refuses every new key: Update
// reports !admitted and stores nothing, exactly as a full TinyLFU shard does. UpdateReplica
// is inherited from the embedded store and stores normally, which is the whole point — the
// coordinator must reach the local copy through a path admission cannot refuse.
type admissionRejectingStore struct {
	store.Store
}

func (a *admissionRejectingStore) Update(key string, fn func(old []byte) []byte) (bool, error) {
	return false, nil
}

// TestCoordinator_WriteAdvancesClockWhenAdmissionWouldReject is the regression test for the
// silent lost update.
//
// The vector clock is derived from what is STORED: Frontier(existing).Increment(self). If a
// local apply can be discarded, this node's counter never advances, so the NEXT write to the
// same key mints the identical clock — and Reconcile treats an equal clock as already covered
// and drops the incoming version. Two distinct values, the second silently lost, with the
// client told +OK both times. No amount of ack accounting detects it; only never dropping the
// local apply does.
func TestCoordinator_WriteAdvancesClockWhenAdmissionWouldReject(t *testing.T) {
	s := &admissionRejectingStore{Store: store.New()}
	peer := newFakeReplicator()
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(setCmd("k", "v1"))); got != "+OK\r\n" {
		t.Fatalf("first write = %q, want +OK", got)
	}
	if got := encodeReply(t, c.coordinate(setCmd("k", "v2"))); got != "+OK\r\n" {
		t.Fatalf("second write = %q, want +OK", got)
	}

	blob, ok := s.Get("k")
	if !ok {
		t.Fatal("local store is missing the key — the local apply was dropped")
	}
	vs, err := version.Decode(blob)
	if err != nil || len(vs) != 1 {
		t.Fatalf("local versions = %v (err %v), want exactly one", vs, err)
	}
	if got := string(vs[0].Value); got != "v2" {
		t.Errorf("local value = %q, want v2 — the second write was dropped as a duplicate clock", got)
	}
	if got := vs[0].Clock["a"]; got != 2 {
		t.Errorf("clock for self = %d after two writes, want 2 — the counter did not advance", got)
	}
}

// TestCoordinator_DeleteAdvancesClockWhenAdmissionWouldReject: the same for a tombstone. A
// dropped tombstone is worse than a dropped value — the key stays live here, so read-repair
// and anti-entropy can push it back out to the replicas that did bury it.
func TestCoordinator_DeleteAdvancesClockWhenAdmissionWouldReject(t *testing.T) {
	s := &admissionRejectingStore{Store: store.New()}
	peer := newFakeReplicator()
	c := newTestCoordinator(s, peer, 3, 2, 2)

	c.coordinate(setCmd("k", "v"))
	if got := encodeReply(t, c.coordinate(delCmd("k"))); got != ":1\r\n" {
		t.Fatalf("delete of a live key = %q, want :1", got)
	}

	blob, ok := s.Get("k")
	if !ok {
		t.Fatal("local store is missing the key — the tombstone was dropped")
	}
	vs, err := version.Decode(blob)
	if err != nil || len(vs) != 1 {
		t.Fatalf("local versions = %v (err %v), want exactly one", vs, err)
	}
	if !vs[0].Deleted {
		t.Error("the surviving local version is not a tombstone — the delete did not stick")
	}
	if got := len(version.Live(vs)); got != 0 {
		t.Errorf("Live() returned %d versions, want 0 — a deleted key must read as a miss", got)
	}
}

// TestCoordinator_GatherReturnsWithoutWaitingWhenQuorumMet: with W=1 the local copy alone
// satisfies the quorum, so gather must reply without receiving from any peer. Before the
// check moved to the top of the loop it blocked on the first result, making a W=1 write wait
// out the whole -replica-timeout whenever a replica was unreachable.
func TestCoordinator_GatherReturnsWithoutWaitingWhenQuorumMet(t *testing.T) {
	c := newTestCoordinator(store.New(), newFakeReplicator(), 3, 2, 1)

	launched := make(chan string, 2)
	done := make(chan int, 1)
	go func() {
		done <- c.gather([]string{"b", "c"}, 1, func(addr string) bool {
			launched <- addr
			time.Sleep(300 * time.Millisecond) // stands in for a replica timing out
			return true
		})
	}()

	select {
	case acks := <-done:
		if acks != 1 {
			t.Errorf("acks = %d, want 1 (the local replica alone)", acks)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("gather blocked on a peer even though the quorum was met on entry")
	}

	// The fan-out must still have been launched for every peer: it IS the replication, and
	// it is where hints are parked. need decides when we reply, never whether we replicate.
	for range 2 {
		select {
		case <-launched:
		case <-time.After(time.Second):
			t.Fatal("op did not run for every peer — the fan-out was skipped")
		}
	}
}

// TestCoordinator_GatherWaitsUntilQuorumMet: the other direction — when the local ack alone
// is not enough, gather still collects peer results until need is reached.
func TestCoordinator_GatherWaitsUntilQuorumMet(t *testing.T) {
	c := newTestCoordinator(store.New(), newFakeReplicator(), 3, 2, 3)

	if acks := c.gather([]string{"b", "c"}, 3, func(addr string) bool { return true }); acks != 3 {
		t.Errorf("acks = %d, want 3 (local plus both peers)", acks)
	}
	if acks := c.gather([]string{"b", "c"}, 3, func(addr string) bool { return false }); acks != 1 {
		t.Errorf("acks = %d with both peers failing, want 1 (local only)", acks)
	}
}
