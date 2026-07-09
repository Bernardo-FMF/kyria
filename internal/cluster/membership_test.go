package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// node is a small constructor for test Nodes.
func node(id string, state NodeState, inc uint64) Node {
	return Node{ID: id, Addr: id + ":7000", State: state, Incarnation: inc}
}

// find returns the member with id from m's snapshot, failing if it is absent.
func find(t *testing.T, m *Members, id string) Node {
	t.Helper()
	for _, n := range m.Snapshot() {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("node %q not in members", id)
	return Node{}
}

// TestMerge_AddsUnknownNode: a gossiped node we've never heard of is added.
func TestMerge_AddsUnknownNode(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	m.Merge([]Node{node("b", Alive, 1)}, time.Now())

	if got := find(t, m, "b"); got.State != Alive || got.Incarnation != 1 {
		t.Errorf("merged b = %+v, want Alive incarnation 1", got)
	}
}

// TestMerge_HigherIncarnationWins: a strictly newer incarnation is always adopted,
// whatever the state.
func TestMerge_HigherIncarnationWins(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	now := time.Now()

	m.Merge([]Node{node("b", Alive, 1)}, now)
	m.Merge([]Node{node("b", Dead, 3)}, now)

	if got := find(t, m, "b"); got.Incarnation != 3 || got.State != Dead {
		t.Errorf("b = %+v, want Dead incarnation 3", got)
	}
}

// TestMerge_LowerIncarnationIgnored: stale gossip (older incarnation) is dropped.
func TestMerge_LowerIncarnationIgnored(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	now := time.Now()

	m.Merge([]Node{node("b", Alive, 5)}, now)
	m.Merge([]Node{node("b", Dead, 2)}, now) // older → ignored

	if got := find(t, m, "b"); got.Incarnation != 5 || got.State != Alive {
		t.Errorf("b = %+v, want Alive incarnation 5 (stale update ignored)", got)
	}
}

// TestMerge_EqualIncarnationDeaderWins: on an incarnation tie, the deader state wins.
func TestMerge_EqualIncarnationDeaderWins(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	now := time.Now()

	m.Merge([]Node{node("b", Alive, 4)}, now)
	m.Merge([]Node{node("b", Dead, 4)}, now) // same incarnation, deader state

	if got := find(t, m, "b"); got.State != Dead {
		t.Errorf("b state = %v, want Dead (deader state wins on an incarnation tie)", got.State)
	}
}

// TestMerge_RefutesFalseSelfClaim: gossip that says self is dead must be refuted —
// self stays Alive and raises its incarnation above the false claim.
func TestMerge_RefutesFalseSelfClaim(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	m.Merge([]Node{node("self", Dead, 5)}, time.Now())

	got := find(t, m, "self")
	if got.State != Alive {
		t.Errorf("self state = %v, want Alive (must refute a false death claim)", got.State)
	}
	if got.Incarnation <= 5 {
		t.Errorf("self incarnation = %d, want > 5 (must out-live the claim)", got.Incarnation)
	}
}

// TestBump_RaisesSelfIncarnation: each heartbeat raises self's incarnation.
func TestBump_RaisesSelfIncarnation(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	m.Bump(time.Now())

	if got := find(t, m, "self"); got.Incarnation != 2 {
		t.Errorf("self incarnation after Bump = %d, want 2", got.Incarnation)
	}
}

// TestDetectFailures_MarksStaleNodesDead: an alive node whose last update is older
// than the timeout is marked Dead; one seen within the timeout stays Alive.
func TestDetectFailures_MarksStaleNodesDead(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	base := time.Now()

	m.Merge([]Node{node("b", Alive, 1)}, base)                    // b last seen at base
	m.Merge([]Node{node("c", Alive, 1)}, base.Add(9*time.Second)) // c seen later

	m.DetectFailures(base.Add(10*time.Second), 5*time.Second)

	if got := find(t, m, "b"); got.State != Dead {
		t.Errorf("b state = %v, want Dead (10s stale > 5s timeout)", got.State)
	}
	if got := find(t, m, "c"); got.State != Alive {
		t.Errorf("c state = %v, want Alive (1s stale < 5s timeout)", got.State)
	}
}

// TestDetectFailures_NeverMarksSelf: self is never failure-detected, even if its
// lastSeen is arbitrarily old.
func TestDetectFailures_NeverMarksSelf(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	m.DetectFailures(time.Now().Add(time.Hour), time.Second)

	if got := find(t, m, "self"); got.State != Alive {
		t.Errorf("self state = %v, want Alive (self is never failure-detected)", got.State)
	}
}

// TestAlive_ReturnsOnlyAliveMembers: Alive() excludes dead members.
func TestAlive_ReturnsOnlyAliveMembers(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	m.Merge([]Node{node("b", Alive, 1), node("c", Dead, 1)}, time.Now())

	ids := map[string]bool{}
	for _, n := range m.Alive() {
		ids[n.ID] = true
	}
	if !ids["self"] || !ids["b"] || ids["c"] {
		t.Errorf("Alive() ids = %v, want self and b present, c absent", ids)
	}
}

// TestSnapshot_ReturnsEachMemberOnce guards the make-length bug: Snapshot must
// return exactly one Node per member, with no zero-value phantoms.
func TestSnapshot_ReturnsEachMemberOnce(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))
	m.Merge([]Node{node("b", Alive, 1), node("c", Dead, 1)}, time.Now())

	got := m.Snapshot()
	if len(got) != 3 {
		t.Errorf("Snapshot len = %d, want 3 (self, b, c)", len(got))
	}
	for _, n := range got {
		if n.ID == "" {
			t.Errorf("Snapshot contains a zero-value node: %+v", n)
		}
	}
}

// TestMembers_ConcurrentAccess hammers the roster from many goroutines so the
// race detector (and Go's concurrent-map-write panic) guard the locking. Only
// meaningful under `go test -race`.
func TestMembers_ConcurrentAccess(t *testing.T) {
	m := NewMembers(node("self", Alive, 1))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("n%d", i)
			now := time.Now()
			for j := 0; j < 200; j++ {
				m.Merge([]Node{{ID: id, Addr: id + ":7000", State: Alive, Incarnation: uint64(j)}}, now)
				m.Bump(now)
				m.DetectFailures(now, time.Second)
				_ = m.Snapshot()
				_ = m.Alive()
			}
		}(i)
	}
	wg.Wait()
}
