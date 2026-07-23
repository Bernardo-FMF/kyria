package vclock

import (
	"maps"
)

// A vector clock is a per-node logical counter that captures a value's causal history,
// so two versions of a key can be compared. One version may descend (supersede) the other,
// or the two may be concurrent - resulting in a conflict that the reconciliation layer must
// keep as siblings rather than silently dropping.

// Clock maps a node ID to its logical counter. A node not present reads as counter 0,
// so the zero value (a nil map) is a valid empty clock.
type Clock map[string]uint64

// Order is the causal relationship of one clock to another.
type Order int

const (
	Equal      Order = iota // identical histories
	Before                  // the receiver happened-before other (other supersedes it)
	After                   // the receiver happened-after other (it supersedes other)
	Concurrent              // neither descends the other — a conflict
)

// Increment returns a copy of the calling clock with the node's counter raised by one,
// recording a new write coordinated by node.
// It must not mutate the calling clock to avoid in-place mutations that would corruct
// that value.
func (c Clock) Increment(node string) Clock {
	clock := make(Clock, len(c)+1)
	maps.Copy(clock, c)
	clock[node]++
	return clock
}

// Merge returns the causal join of the calling clock and other: for every node,
// the higher of the two counters. Reconciliation uses it to fold a set of concurrent
// siblings into the one clock that descends them all.
func (c Clock) Merge(other Clock) Clock {
	clock := make(Clock, len(c))
	maps.Copy(clock, c)

	for node, count := range other {
		clock[node] = max(clock[node], count)
	}

	return clock
}

// Compare reports how the calling clock relates to other causally.
func (c Clock) Compare(other Clock) Order {
	// Walk the union of node IDs (a key missing from one side reads as 0): c is ahead
	// if it has any higher counter, other is ahead if it does. Ahead on both sides
	// means the histories diverged - concurrent.
	cAhead := c.aheadOf(other)
	otherAhead := other.aheadOf(c)

	switch {
	case cAhead && otherAhead:
		return Concurrent
	case cAhead:
		return After
	case otherAhead:
		return Before
	default:
		return Equal
	}
}

// aheadOf reports whether c has any node whose counter exceeds other's — treating a
// node missing from other as 0. It returns on the first such node rather than
// scanning the whole clock.
func (c Clock) aheadOf(other Clock) bool {
	for n, cnt := range c {
		if cnt > other[n] {
			return true
		}
	}
	return false
}
