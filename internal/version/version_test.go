package version

import (
	"maps"
	"reflect"
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

// TestFrontier: the frontier is the pointwise-max join of every sibling's clock —
// so a write incrementing it descends them all.
func TestFrontier(t *testing.T) {
	versions := []Version{
		{Value: []byte("a"), Clock: vclock.Clock{"n1": 2, "n2": 1}},
		{Value: []byte("b"), Clock: vclock.Clock{"n1": 1, "n3": 5}},
	}
	if got := Frontier(versions); !maps.Equal(got, vclock.Clock{"n1": 2, "n2": 1, "n3": 5}) {
		t.Errorf("Frontier = %v, want {n1:2, n2:1, n3:5}", got)
	}
	if got := Frontier(nil); len(got) != 0 {
		t.Errorf("Frontier(nil) = %v, want empty", got)
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

// TestCodec_RoundTrip: Encode then Decode reproduces the sibling set exactly,
// including a binary value and multi-node clocks.
func TestCodec_RoundTrip(t *testing.T) {
	versions := []Version{
		{Value: []byte("hello"), Clock: vclock.Clock{"n1": 3, "n2": 1}},
		{Value: []byte{0x00, 0xff, 0x10, 0x00}, Clock: vclock.Clock{"node-b": 7}},
	}

	got, err := Decode(Encode(versions))
	if err != nil {
		t.Fatalf("Decode(Encode(...)): %v", err)
	}
	if !reflect.DeepEqual(got, versions) {
		t.Errorf("round-trip = %#v, want %#v", got, versions)
	}
}

// TestDecode_Empty: an empty blob is a key with no value yet — an empty set, no error.
func TestDecode_Empty(t *testing.T) {
	got, err := Decode(nil)
	if err != nil {
		t.Fatalf("Decode(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Decode(nil) = %v, want an empty set", got)
	}
}

// TestDecode_Truncated: every proper prefix of a valid blob is rejected, not
// panicked on — the guard against a corrupt/short read.
func TestDecode_Truncated(t *testing.T) {
	full := Encode([]Version{{Value: []byte("hello"), Clock: vclock.Clock{"n1": 1}}})
	for i := 1; i < len(full); i++ {
		if _, err := Decode(full[:i]); err == nil {
			t.Errorf("Decode of a truncated blob (%d of %d bytes) = nil error, want an error", i, len(full))
		}
	}
}
