package store

import "sync/atomic"

// Policy is a pluggable eviction strategy. It keeps an eviction hint on each
// entry — a single number, where "lower is evicted first" — that it updates on
// access and insert. When a shard is over capacity the store samples a few
// entries and drops the one with the smallest hint (MapStore.sampleVictim), so
// there is no global ordering to maintain: eviction is approximate and reads
// never do list surgery.
//
// recordAccess is called from Get under a read lock, possibly concurrently for
// the same entry, so it must touch only the atomic hint — nothing else. That one
// constraint is what keeps reads lock-free.
type Policy interface {
	recordAccess(hint *atomic.Uint64) // called from Get, under a read lock
	recordInsert(hint *atomic.Uint64) // called from set, under the write lock
}

// lru is an approximate LRU policy. It stamps each accessed or inserted entry
// with the next value of a monotonic clock, so the smallest hint marks the
// least-recently-touched entry — the one the store evicts.
type lru struct {
	clock atomic.Uint64
}

// NewLRU returns a fresh approximate-LRU policy. It is the factory passed to
// WithPolicy: NewSharded calls it once per shard, so each shard's clock is its
// own and is never shared or contended across shards.
func NewLRU() Policy {
	return &lru{}
}

func (p *lru) recordAccess(hint *atomic.Uint64) { hint.Store(p.clock.Add(1)) }
func (p *lru) recordInsert(hint *atomic.Uint64) { hint.Store(p.clock.Add(1)) }
