package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// membersWith builds a Members with self plus the given extra alive node IDs.
func membersWith(self string, others ...string) *Members {
	m := NewMembers(Node{ID: self, Addr: self, State: Alive, Incarnation: 1})
	if len(others) > 0 {
		nodes := make([]Node, len(others))
		for i, id := range others {
			nodes[i] = Node{ID: id, Addr: id, State: Alive, Incarnation: 1}
		}
		m.Merge(nodes, time.Now())
	}
	return m
}

// TestRouter_Owner: Owner returns a node from the alive set, and IsLocal agrees with it.
func TestRouter_Owner(t *testing.T) {
	m := membersWith("a", "b", "c")
	r := NewRouter(m, 50, time.Second)

	alive := map[string]bool{"a": true, "b": true, "c": true}
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		owner, ok := r.Owner(key)
		if !ok || !alive[owner] {
			t.Fatalf("Owner(%q) = (%q, %v), want an alive node", key, owner, ok)
		}
		if r.IsLocal(key) != (owner == "a") {
			t.Errorf("IsLocal(%q) = %v, but owner is %q (self is a)", key, r.IsLocal(key), owner)
		}
	}
}

// TestRouter_ReflectsMembership: after a node joins and the ring is rebuilt, some
// keys route to the newcomer (before, self owned everything).
func TestRouter_ReflectsMembership(t *testing.T) {
	m := membersWith("a")
	r := NewRouter(m, 50, time.Second)

	for i := 0; i < 200; i++ {
		if !r.IsLocal(fmt.Sprintf("key-%d", i)) {
			t.Fatal("with only self alive, self should own every key")
		}
	}

	m.Merge([]Node{{ID: "b", Addr: "b", State: Alive, Incarnation: 1}}, time.Now())
	r.rebuild()

	ownedByB := 0
	for i := 0; i < 200; i++ {
		if owner, _ := r.Owner(fmt.Sprintf("key-%d", i)); owner == "b" {
			ownedByB++
		}
	}
	if ownedByB == 0 {
		t.Error("after b joined and the ring rebuilt, b should own some keys")
	}
}

// TestRouter_ConcurrentAccess: request-path reads race against the background
// rebuild and concurrent membership changes. Only meaningful under -race — it
// proves the atomic ring swap needs no lock on the read path.
func TestRouter_ConcurrentAccess(t *testing.T) {
	m := membersWith("a", "b")
	r := NewRouter(m, 30, 5*time.Millisecond)
	r.Start()
	defer r.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			now := time.Now()
			for j := 0; j < 500; j++ {
				key := fmt.Sprintf("k-%d-%d", i, j)
				_, _ = r.Owner(key)
				_ = r.IsLocal(key)
				m.Merge([]Node{{ID: fmt.Sprintf("n%d", j%4), Addr: "x", State: Alive, Incarnation: uint64(j)}}, now)
			}
		}(i)
	}
	wg.Wait()
}
