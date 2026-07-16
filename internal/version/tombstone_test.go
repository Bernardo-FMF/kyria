package version

import (
	"maps"
	"testing"

	"github.com/Bernardo-FMF/kyria/internal/vclock"
)

// TestTombstone_CodecRoundTrip: the Deleted flag survives Encode→Decode (order preserved).
func TestTombstone_CodecRoundTrip(t *testing.T) {
	versions := []Version{
		{Value: []byte("v"), Clock: vclock.Clock{"a": 1}},
		Tombstone(vclock.Clock{"a": 2}),
	}

	got, err := Decode(Encode(versions))
	if err != nil {
		t.Fatalf("Decode(Encode()): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("round-trip = %d versions, want 2", len(got))
	}
	if got[0].Deleted || string(got[0].Value) != "v" || !maps.Equal(got[0].Clock, vclock.Clock{"a": 1}) {
		t.Errorf("value version round-tripped as %+v", got[0])
	}
	if !got[1].Deleted || !maps.Equal(got[1].Clock, vclock.Clock{"a": 2}) {
		t.Errorf("tombstone round-tripped as %+v, want Deleted with clock {a:2}", got[1])
	}
}

// TestTombstone_SupersedesValue: a newer tombstone buries the value (and Live reports a miss); a
// stale value can't resurrect past an existing tombstone.
func TestTombstone_SupersedesValue(t *testing.T) {
	// delete after a write: tombstone {a:2} supersedes value {a:1}
	got := Reconcile([]Version{ver("v", vclock.Clock{"a": 1})}, Tombstone(vclock.Clock{"a": 2}))
	if len(got) != 1 || !got[0].Deleted {
		t.Errorf("Reconcile(value, newer tombstone) = %v, want just the tombstone", got)
	}
	if live := Live(got); len(live) != 0 {
		t.Errorf("Live after delete = %v, want no live versions (a miss)", live)
	}

	// a stale value arriving at a tombstone is dropped
	got = Reconcile([]Version{Tombstone(vclock.Clock{"a": 2})}, ver("stale", vclock.Clock{"a": 1}))
	if len(got) != 1 || !got[0].Deleted {
		t.Errorf("Reconcile(tombstone, stale value) = %v, want the tombstone to survive", got)
	}
}

// TestTombstone_ConcurrentWriteSurvives: a write concurrent with a delete is kept as a sibling and
// stays live — a delete must not silently kill a concurrent write.
func TestTombstone_ConcurrentWriteSurvives(t *testing.T) {
	got := Reconcile([]Version{ver("v", vclock.Clock{"a": 1})}, Tombstone(vclock.Clock{"b": 1}))
	if len(got) != 2 {
		t.Fatalf("Reconcile of concurrent value+tombstone = %v, want both as siblings", got)
	}
	live := Live(got)
	if len(live) != 1 || string(live[0].Value) != "v" {
		t.Errorf("Live = %v, want the concurrent value [v]", live)
	}
}

// TestTombstone_EqualDistinguishesDeleted: a value and a tombstone with the same clock (and the same
// empty bytes) are different versions, so Equal must report them unequal.
func TestTombstone_EqualDistinguishesDeleted(t *testing.T) {
	value := []Version{ver("", vclock.Clock{"a": 1})}
	tomb := []Version{Tombstone(vclock.Clock{"a": 1})}
	if Equal(value, tomb) {
		t.Error("a value and a tombstone with the same clock should not be Equal")
	}
}
