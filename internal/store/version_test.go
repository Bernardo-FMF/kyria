package store

import (
	"testing"

	"github.com/Bernardo-FMF/kyria/internal/vclock"
)

func ver(value string, clock vclock.Clock) Version {
	return Version{Value: []byte(value), Clock: clock}
}

// valueSet collects the values of a sibling set for order-independent assertions.
func valueSet(vs []Version) map[string]bool {
	m := make(map[string]bool, len(vs))
	for _, v := range vs {
		m[string(v.Value)] = true
	}
	return m
}

// TestReconcile_FirstWrite: into an empty set, incoming becomes the only version.
func TestReconcile_FirstWrite(t *testing.T) {
	got := Reconcile(nil, ver("a", vclock.Clock{"n1": 1}))
	if len(got) != 1 || string(got[0].Value) != "a" {
		t.Errorf("Reconcile(empty, a) = %v, want just [a]", valueSet(got))
	}
}

// TestReconcile_Supersedes: a newer clock replaces the old version.
func TestReconcile_Supersedes(t *testing.T) {
	existing := []Version{ver("old", vclock.Clock{"n1": 1})}
	got := Reconcile(existing, ver("new", vclock.Clock{"n1": 2}))
	if len(got) != 1 || string(got[0].Value) != "new" {
		t.Errorf("Reconcile = %v, want just [new] (old superseded)", valueSet(got))
	}
}

// TestReconcile_StaleIgnored: an older clock is dropped, leaving the current version.
func TestReconcile_StaleIgnored(t *testing.T) {
	existing := []Version{ver("new", vclock.Clock{"n1": 2})}
	got := Reconcile(existing, ver("old", vclock.Clock{"n1": 1}))
	if len(got) != 1 || string(got[0].Value) != "new" {
		t.Errorf("Reconcile = %v, want just [new] (stale write ignored)", valueSet(got))
	}
}

// TestReconcile_Duplicate: an equal clock doesn't add a second copy.
func TestReconcile_Duplicate(t *testing.T) {
	existing := []Version{ver("x", vclock.Clock{"n1": 1})}
	got := Reconcile(existing, ver("x", vclock.Clock{"n1": 1}))
	if len(got) != 1 {
		t.Errorf("Reconcile of an equal clock = %v, want 1 version, not a duplicate", valueSet(got))
	}
}

// TestReconcile_Siblings: concurrent clocks are both kept.
func TestReconcile_Siblings(t *testing.T) {
	existing := []Version{ver("x", vclock.Clock{"n1": 1})}
	got := Reconcile(existing, ver("y", vclock.Clock{"n2": 1}))
	set := valueSet(got)
	if len(got) != 2 || !set["x"] || !set["y"] {
		t.Errorf("Reconcile of concurrent writes = %v, want siblings [x, y]", set)
	}
}

// TestReconcile_DropsAndKeeps: incoming supersedes one existing version and is
// concurrent with another — the superseded one goes, the sibling and incoming stay.
func TestReconcile_DropsAndKeeps(t *testing.T) {
	existing := []Version{
		ver("A", vclock.Clock{"n1": 2}), // incoming {n1:3} descends this → dropped
		ver("B", vclock.Clock{"n2": 1}), // concurrent with incoming → kept
	}
	got := Reconcile(existing, ver("C", vclock.Clock{"n1": 3}))
	set := valueSet(got)
	if len(got) != 2 || !set["B"] || !set["C"] || set["A"] {
		t.Errorf("Reconcile = %v, want [B, C] (A superseded)", set)
	}
}
