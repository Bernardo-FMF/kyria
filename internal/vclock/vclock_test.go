package vclock

import (
	"maps"
	"testing"
)

// TestIncrement: Increment bumps the node's counter, starts a fresh node at 1 (even
// from a nil clock), and never mutates the receiver — clocks live alongside stored
// values and must stay immutable.
func TestIncrement(t *testing.T) {
	a := Clock{"n1": 1, "n2": 3}
	b := a.Increment("n1")

	if b["n1"] != 2 || b["n2"] != 3 {
		t.Errorf("Increment(n1) = %v, want {n1:2, n2:3}", b)
	}
	if a["n1"] != 1 {
		t.Errorf("Increment mutated the receiver: a = %v, want n1 still 1", a)
	}

	var empty Clock // nil clock is a valid empty clock
	if got := empty.Increment("n9"); got["n9"] != 1 {
		t.Errorf("Increment(n9) on an empty clock = %v, want {n9:1}", got)
	}
}

// TestCompare covers all four causal relationships, including the "missing entry
// reads as 0" rule that makes disjoint clocks concurrent.
func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		a, b Clock
		want Order
	}{
		{"equal", Clock{"n1": 2, "n2": 1}, Clock{"n1": 2, "n2": 1}, Equal},
		{"equal treats a missing node as zero", Clock{"n1": 2}, Clock{"n1": 2, "n2": 0}, Equal},
		{"after (c descends other)", Clock{"n1": 3, "n2": 1}, Clock{"n1": 2, "n2": 1}, After},
		{"before (other descends c)", Clock{"n1": 1}, Clock{"n1": 2}, Before},
		{"concurrent (each ahead on a node)", Clock{"n1": 2, "n2": 1}, Clock{"n1": 1, "n2": 2}, Concurrent},
		{"concurrent when disjoint", Clock{"n1": 1}, Clock{"n2": 1}, Concurrent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("Compare(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// TestMerge: the causal join is the pointwise max across the union of nodes.
func TestMerge(t *testing.T) {
	a := Clock{"n1": 3, "n2": 1}
	b := Clock{"n1": 1, "n2": 2, "n3": 5}

	got := a.Merge(b)
	want := Clock{"n1": 3, "n2": 2, "n3": 5}
	if !maps.Equal(got, want) {
		t.Errorf("Merge(%v, %v) = %v, want %v", a, b, got, want)
	}
	// A merge should descend both inputs (it's their least common successor).
	if got.Compare(a) != After || got.Compare(b) != After {
		t.Errorf("merged clock %v should descend both inputs", got)
	}
}
