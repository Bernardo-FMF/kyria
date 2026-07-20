package store

import "sync/atomic"

// Policy is a pluggable eviction strategy. Each entry carries an atomic hint the
// policy maintains; score turns that (and/or the key) into an eviction priority.
type Policy interface {
	// recordAccess is called from Get under a read lock, possibly concurrently for
	// the same entry, so it must touch only atomic state — nothing else. That one
	// constraint is what keeps reads lock-free.
	recordAccess(key string, hint *atomic.Uint64)
	// recordInsert notes a write.
	recordInsert(key string, hint *atomic.Uint64)
	// score returns an entry's eviction priority; the store evicts the lowest.
	score(key string, hint *atomic.Uint64) uint64
	// admit decides whether a newcomer (candidateScore) should displace the
	// weakest sampled incumbent (victimScore): true evicts the victim, false
	// rejects the newcomer.
	admit(candidateScore, victimScore uint64) bool
}

// lru is an approximate LRU policy. It stamps each accessed or inserted entry
// with the next value of a monotonic clock, so the smallest hint marks the
// least-recently-touched entry - the one the store evicts.
type lru struct {
	clock atomic.Uint64
}

// NewLRU returns a fresh approximate-LRU policy.
func NewLRU() Policy {
	return &lru{}
}

func (p *lru) recordAccess(key string, hint *atomic.Uint64) { hint.Store(p.clock.Add(1)) }
func (p *lru) recordInsert(key string, hint *atomic.Uint64) { hint.Store(p.clock.Add(1)) }
func (p *lru) score(key string, hint *atomic.Uint64) uint64 { return hint.Load() }

// admit always accepts: plain LRU has no admission filter, so a new key always
// displaces the least-recently-used victim.
func (p *lru) admit(candidateScore, victimScore uint64) bool { return true }

// lfuBaseCount is the count a new entry starts at, so a
// fresh key is not pinned at zero.
const lfuBaseCount = 5

// lfu is an approximate LFU (least-frequently-used) policy: the hint is an access
// count that grows on every access, so the smallest hint is the least-frequently-
// used entry — the one the store evicts. Unlike lru it keeps no per-policy state.
//
// Two known limitations, both addressed by TinyLFU:
// 1. a new entry starts at the base count (the floor), so it is the immediate
// eviction candidate - new keys struggle to get in (cold start);
// 2. and counts only ever grow, so a key that was hot long ago never ages out.
type lfu struct {
}

// NewLFU returns a fresh LFU policy.
func NewLFU() Policy {
	return &lfu{}
}

func (p *lfu) recordAccess(key string, hint *atomic.Uint64) { hint.Add(1) }
func (p *lfu) recordInsert(key string, hint *atomic.Uint64) { hint.Store(lfuBaseCount) }
func (p *lfu) score(key string, hint *atomic.Uint64) uint64 { return hint.Load() }

// admit always accepts: plain LFU has no admission filter, so a new key always
// displaces the least-frequently-used victim.
func (p *lfu) admit(candidateScore, victimScore uint64) bool { return true }

// tinyLFU is an admission policy. Its frequency numbers come from a shared
// count-min sketch keyed by the key — it ignores the per-entry hint the other
// policies use. Because the sketch ages itself, old popularity fades (unlike
// lfu), and admit uses its estimates to reject a newcomer that isn't worth
// evicting an incumbent for (fixing lfu's cold-start problem).
type tinyLFU struct {
	sketch *countMinSketch
}

// NewTinyLFU returns a factory for TinyLFU policies whose sketch is sized for
// capacity keys.
func NewTinyLFU(capacity int) func() Policy {
	return func() Policy {
		return &tinyLFU{
			NewCountMinSketch(capacity),
		}
	}
}

func (p *tinyLFU) recordAccess(key string, hint *atomic.Uint64) { p.sketch.add(key) }
func (p *tinyLFU) recordInsert(key string, hint *atomic.Uint64) { p.sketch.add(key) }
func (p *tinyLFU) score(key string, hint *atomic.Uint64) uint64 {
	return uint64(p.sketch.estimate(key))
}

// admit is the admission filter: it accepts the newcomer only if the sketch
// estimates it strictly more frequent than the victim it would displace.
// Ties favor the incumbent.
func (p *tinyLFU) admit(candidateScore, victimScore uint64) bool {
	return candidateScore > victimScore
}
