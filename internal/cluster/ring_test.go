package cluster

import (
	"fmt"
	"testing"
)

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
			r.Add(n)
		}
		return r
	}
	r1, r2 := build(), build()

	for i := 0; i < 1000; i++ {
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
		r.Add(n)
	}

	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
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
		r.Add(n)
	}

	before := map[string]string{}
	for i := 0; i < 2000; i++ {
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
