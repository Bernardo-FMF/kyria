package server

import (
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
)

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
