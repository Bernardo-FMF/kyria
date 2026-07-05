package store

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 2 EXERCISE — concurrency safety. You implement ShardedStore here.
//
// Spec/tests: sharded_store_test.go. Make them pass:
//     go test ./internal/store/ -run TestShardedStore
//
// WHY this phase exists: a plain Go map is NOT safe for concurrent use. Two
// goroutines writing the same map — or one writing while another reads — is a
// data race, and the Go runtime actively detects it and CRASHES with
// "fatal error: concurrent map writes". Your Phase-1 MapStore has this bug the
// moment two goroutines touch it. (The concurrent test tells you how to witness
// the crash.)
//
// THE FIX — sharding (a.k.a. lock striping): one lock over one big map would be
// correct but slow, because every operation would serialise through that single
// lock. Instead, split the keyspace into N independent shards, each with its OWN
// map and its OWN lock. Operations on keys in different shards then run in
// parallel. A ShardedStore with 1 shard == "one global lock"; with many shards,
// contention drops. The benchmark makes the difference visible.
//
// REUSE Phase 1: let each shard wrap a *MapStore, so you inherit validation,
// size limits, and the defensive copy for free — don't re-implement them.
// ─────────────────────────────────────────────────────────────────────────────

// TODO(1): IMPORTS
// You'll need:
//   - "sync"          → sync.RWMutex
//   - "hash/maphash"  → hash a key to a shard index (fast, no allocation)
//       alternative: "hash/fnv" (simpler API, but allocates on []byte(key))
//   docs: https://pkg.go.dev/sync#RWMutex
//         https://pkg.go.dev/hash/maphash
import (
	"hash/maphash"
	"sync"
)

// TODO(2): THE shard TYPE (unexported)
// A struct pairing a lock with the data it protects:
//   - mu    sync.RWMutex
//   - store *MapStore     ← the Phase-1 store this shard owns
//
// Convention: put the mutex right next to the field(s) it guards, so it's
// obvious what the lock protects.
type shard struct {
	mu    sync.RWMutex
	store *MapStore
}

// TODO(3): THE ShardedStore TYPE (exported)
// A struct holding:
//   - shards []*shard         ← fixed set, never resized after construction
//   - seed   maphash.Seed     ← one seed, so a given key always hashes the same way
type ShardedStore struct {
	shards []*shard
	seed   maphash.Seed
}

// TODO(4): THE CONSTRUCTOR
// Suggested signature:  NewSharded(shardCount int, opts ...Option) *ShardedStore
//  1. Guard shardCount: if it's < 1, use 1. (Zero shards → divide-by-zero when
//     you pick a shard. Never build zero.)
//  2. Build the shards slice with shardCount entries. For each entry create a
//     &shard{store: New(opts...)} — note you REUSE the Phase-1 New + Option, so
//     every shard shares the same size limits.
//  3. Create one seed:  maphash.MakeSeed()
//     docs: https://pkg.go.dev/hash/maphash#MakeSeed
func NewSharded(shardCount int, opts ...Option) *ShardedStore {
	if shardCount < 1 {
		shardCount = 1
	}

	shardSlices := make([]*shard, shardCount)
	for shardIdx := range shardCount {
		shardSlices[shardIdx] = &shard{store: New(opts...)}
	}

	return &ShardedStore{
		shards: shardSlices,
		seed:   maphash.MakeSeed(),
	}
}

// TODO(5): COMPILE-TIME INTERFACE ASSERTION
//   var _ Store = (*ShardedStore)(nil)

// TODO(6): SHARD-SELECTION HELPER
// A method:  func (s *ShardedStore) shardFor(key string) *shard
//   - idx := maphash.String(s.seed, key) % uint64(len(s.shards))
//   - return s.shards[idx]
//
// Every public method starts by locating its shard this way. Same key → same
// shard → same lock, every time. (Later optimisation: if shardCount is a power
// of two, `& (n-1)` beats `%`. Skip that for now.)
func (s *ShardedStore) shardFor(key string) *shard {
	idx := maphash.String(s.seed, key) % uint64(len(s.shards))
	return s.shards[idx]
}

// TODO(7): THE METHODS — each finds its shard, locks it, delegates to the inner
// *MapStore, unlocks.
//
// Choose the RIGHT lock:
//   - Reads  → RLock / RUnlock  (many readers may hold it at once)
//   - Writes → Lock  / Unlock   (exclusive)
// ALWAYS release the lock, even on an early return from the inner call. The
// idiom is:  sh.mu.Lock(); defer sh.mu.Unlock()   (defer runs when the method
// returns, so you can't forget).
//   docs: https://go.dev/tour/concurrency/9   (sync.Mutex / RWMutex)
//         https://go.dev/ref/mem              (the Go memory model: WHY a lock —
//                                              not just code that "looks right" —
//                                              is what makes concurrent access
//                                              correct)
//
//   Get(key string) ([]byte, bool)   → shardFor(key); RLock/defer RUnlock; inner.Get
//       Subtle but fine: you return the []byte AFTER unlocking. Safe here because
//       the store never mutates a stored slice in place — Set always inserts a
//       fresh copy — so the bytes you return are never written again.
//
//   Set(key string, value []byte) error  → shardFor(key); Lock/defer Unlock; inner.Set
//
//   Delete(key string) bool               → shardFor(key); Lock/defer Unlock; inner.Delete
//
//   Size() int
//       No single map to len() anymore. Walk every shard, RLock it, add its
//       inner.Size(), RUnlock. The total is a point-in-time snapshot — that's OK.
//
// SIDE NOTE — why not just use sync.Map? It's tuned for one narrow pattern
// (mostly-disjoint keys, write-once/read-many). We rewrite hot keys and will add
// TTL/eviction later that need our own map internals. A sharded RWMutex gives us
// more control and more predictable performance. We'll revisit if benchmarks say
// otherwise.
