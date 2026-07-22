package cluster

import (
	"fmt"
	"testing"
)

// sortedAdd adds node then re-sorts — the per-add convenience the tests want. Production
// adds every node first and sorts once (see Router.rebuild), so this lives here, not on Ring.
func sortedAdd(r *Ring, node string) {
	r.Add(node)
	r.Sort()
}

// TestRing_Empty: with no nodes, Get reports no owner.
func TestRing_Empty(t *testing.T) {
	r := NewRing(10)
	if node, ok := r.Get("k"); ok {
		t.Errorf("Get on an empty ring = (%q, true), want (\"\", false)", node)
	}
}

// TestRing_Deterministic: two rings built the same way agree on every key — the
// mapping is a deterministic function of the node set, which is why separate nodes
// that gossip the same membership route identically. Also guards against a per-ring
// random hash seed (which would make the two rings disagree).
func TestRing_Deterministic(t *testing.T) {
	build := func() *Ring {
		r := NewRing(50)
		for _, n := range []string{"a", "b", "c"} {
			sortedAdd(r, n)
		}
		return r
	}
	r1, r2 := build(), build()

	for i := range 1000 {
		k := fmt.Sprintf("key-%d", i)
		n1, _ := r1.Get(k)
		n2, _ := r2.Get(k)
		if n1 != n2 {
			t.Fatalf("key %q → %q in one ring, %q in another — hashing isn't deterministic", k, n1, n2)
		}
	}
}

// TestRing_DistributesAcrossNodes: with enough virtual nodes, every node owns a
// share of the keyspace — none is starved.
func TestRing_DistributesAcrossNodes(t *testing.T) {
	nodes := []string{"a", "b", "c", "d"}
	r := NewRing(100)
	for _, n := range nodes {
		sortedAdd(r, n)
	}

	counts := map[string]int{}
	for i := range 10000 {
		owner, _ := r.Get(fmt.Sprintf("key-%d", i))
		counts[owner]++
	}
	for _, n := range nodes {
		if counts[n] == 0 {
			t.Errorf("node %q owns no keys — virtual nodes aren't distributing load", n)
		}
	}
}

// TestRing_MinimalRemapping is the consistent-hashing guarantee: removing a node
// moves ONLY the keys that were on it — every other key stays put. (With hash % N,
// almost every key would move.)
func TestRing_MinimalRemapping(t *testing.T) {
	nodes := []string{"a", "b", "c", "d"}
	r := NewRing(100)
	for _, n := range nodes {
		sortedAdd(r, n)
	}

	before := map[string]string{}
	for i := range 2000 {
		k := fmt.Sprintf("key-%d", i)
		owner, _ := r.Get(k)
		before[k] = owner
	}

	r.Remove("c")

	for k, was := range before {
		now, _ := r.Get(k)
		if was == "c" {
			if now == "c" {
				t.Errorf("key %q still maps to the removed node c", k)
			}
			continue
		}
		if now != was {
			t.Errorf("key %q moved from %q to %q, but %q was not removed", k, was, now, was)
		}
	}
}

// TestRing_GetN_DistinctReplicas: GetN returns n distinct physical nodes with the
// primary (what Get returns) first — the replica set for a key. The distinctness
// check is the crux: virtual nodes repeat each physical node around the ring, so a
// naive walk would hand back the same machine several times.
func TestRing_GetN_DistinctReplicas(t *testing.T) {
	nodes := []string{"a", "b", "c", "d", "e"}
	r := NewRing(100)
	for _, n := range nodes {
		sortedAdd(r, n)
	}

	for i := range 500 {
		key := fmt.Sprintf("key-%d", i)
		got := r.GetN(key, 3)

		if len(got) != 3 {
			t.Fatalf("GetN(%q, 3) = %v (len %d), want 3 nodes", key, got, len(got))
		}
		seen := map[string]bool{}
		for _, n := range got {
			if seen[n] {
				t.Fatalf("GetN(%q, 3) = %v has a duplicate physical node", key, got)
			}
			seen[n] = true
		}
		if primary, _ := r.Get(key); got[0] != primary {
			t.Errorf("GetN(%q, 3)[0] = %q, want the Get primary %q", key, got[0], primary)
		}
	}
}

// TestRing_GetN_FewerNodesThanN: asking for more replicas than there are nodes
// returns every node exactly once — no duplicates, no padding.
func TestRing_GetN_FewerNodesThanN(t *testing.T) {
	r := NewRing(100)
	sortedAdd(r, "a")
	sortedAdd(r, "b")

	got := r.GetN("key-1", 5)
	if len(got) != 2 {
		t.Fatalf("GetN with 2 nodes and n=5 = %v, want 2 distinct nodes", got)
	}
	if got[0] == got[1] {
		t.Errorf("GetN = %v, want 2 distinct nodes", got)
	}
}

// TestRing_GetN_Empty: no nodes → no replicas.
func TestRing_GetN_Empty(t *testing.T) {
	r := NewRing(10)
	if got := r.GetN("k", 3); len(got) != 0 {
		t.Errorf("GetN on an empty ring = %v, want none", got)
	}
}
