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

// lfuBaseCount is the count a new entry starts at (Redis's LFU_INIT_VAL), so a
// fresh key is not pinned at zero.
const lfuBaseCount = 5

// lfu is an approximate LFU (least-frequently-used) policy: the hint is an access
// count that grows on every access, so the smallest hint is the least-frequently-
// used entry — the one the store evicts. Unlike lru it keeps no per-policy state.
//
// Two known limitations, both addressed by TinyLFU: a new entry starts at the
// base count (the floor), so it is the immediate eviction candidate — new keys
// struggle to get in (cold start); and counts only ever grow, so a key that was
// hot long ago never ages out.
type lfu struct {
}

// NewLFU returns a fresh LFU policy. It is the factory passed to WithPolicy.
func NewLFU() Policy {
	return &lfu{}
}

func (p *lfu) recordAccess(hint *atomic.Uint64) { hint.Add(1) }
func (p *lfu) recordInsert(hint *atomic.Uint64) { hint.Store(lfuBaseCount) }
