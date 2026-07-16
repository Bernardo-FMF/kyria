package store

import (
	"hash/maphash"
	"sync"
	"time"
)

// shard is one partition of a ShardedStore: a MapStore guarded by its own
// RWMutex. The mutex sits next to the store it protects so the locking
// discipline is obvious — nothing touches store without holding mu.
type shard struct {
	mu    sync.RWMutex
	store *MapStore
}

// ShardedStore is a concurrency-safe Store that partitions the keyspace across
// a fixed set of independently locked shards (lock striping). Operations on
// keys in different shards proceed in parallel, so contention falls as the
// shard count grows; a ShardedStore with a single shard is equivalent to one
// global lock.
//
// Each shard wraps a MapStore, inheriting its validation, size limits, and
// defensive value copies. The shard set is created at construction and never
// resized, so a given key maps to the same shard for the store's lifetime.
type ShardedStore struct {
	shards []*shard
	seed   maphash.Seed
}

// NewSharded returns a ShardedStore with shardCount shards, each initialized
// with the supplied options. shardCount is clamped to a minimum of 1. Every
// shard shares the same options, so per-entry size limits are uniform across
// the store.
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

// Compile-time assertion that *ShardedStore satisfies Store.
var _ Store = (*ShardedStore)(nil)

// shardFor returns the shard responsible for key. The mapping is stable for the
// lifetime of the store, since both the seed and the shard count are fixed at
// construction: the same key always resolves to the same shard, hence the same
// lock.
//
// Selection is plain modulo hashing (hash(key) % shardCount) for now. That is
// safe here only because shardCount never changes — if it did, nearly every key
// would remap. Distributing keys across a changing set of cluster nodes is a
// separate, later concern that needs consistent hashing; this in-process shard
// selection deliberately does not.
func (s *ShardedStore) shardFor(key string) *shard {
	idx := maphash.String(s.seed, key) % uint64(len(s.shards))
	return s.shards[idx]
}

// Get returns the value stored for key and whether it was present. The returned
// slice aliases internal storage and must not be modified by the caller.
func (s *ShardedStore) Get(key string) ([]byte, bool) {
	shard := s.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Returning the slice after the lock is released is safe: Set never mutates
	// a stored value in place (it inserts a fresh copy), so these bytes are
	// never written again.
	return shard.store.Get(key)
}

// Set stores value under key. It reports whether the entry was admitted and
// returns a sentinel error if key or value violates the configured size limits.
func (s *ShardedStore) Set(key string, value []byte) (bool, error) {
	shard := s.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.store.Set(key, value)
}

// SetWithTTL stores value under key with a time-to-live, returning ErrInvalidTTL
// if ttl is not positive, or a size-limit sentinel error as Set does. Like Set,
// it reports whether the entry was admitted.
func (s *ShardedStore) SetWithTTL(key string, value []byte, ttl time.Duration) (bool, error) {
	shard := s.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.store.SetWithTTL(key, value, ttl)
}

// Update atomically applies fn to key's value. It holds the key's shard lock across
// the whole read-modify-write, so concurrent Updates to the same key serialize
// instead of racing and losing writes. Admission and size errors propagate as by Set.
func (s *ShardedStore) Update(key string, fn func(old []byte) []byte) (bool, error) {
	shard := s.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.store.Update(key, fn)
}

// Delete removes key and reports whether it had been present.
func (s *ShardedStore) Delete(key string) bool {
	shard := s.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.store.Delete(key)
}

// Size reports the total number of entries across all shards. Each shard is
// locked only while its own count is read, so the result is a point-in-time
// snapshot rather than an instantaneous total: writes to other shards may land
// mid-scan.
func (s *ShardedStore) Size() int {
	size := 0

	for _, shard := range s.shards {
		shard.mu.RLock()

		size += shard.store.Size()

		shard.mu.RUnlock()
	}

	return size
}

// Range calls fn for every live entry across all shards, one shard at a time: each shard is
// held under its read lock only while its own entries are visited, so writes to other shards
// proceed during the sweep, and the per-iteration closure releases the lock even if fn
// panics. fn runs under a shard's read lock, so it must not call back into the store.
func (s *ShardedStore) Range(fn func(key string, value []byte)) {
	for _, shard := range s.shards {
		func() {
			shard.mu.RLock()
			defer shard.mu.RUnlock()
			shard.store.Range(fn)
		}()
	}
}

// DeleteIf removes key only if pred holds for its current value, holding the key's shard write
// lock across the whole check-and-delete so no concurrent write lands in between. A missing key
// is a no-op reporting false. See Store.DeleteIf for the predicate contract.
func (s *ShardedStore) DeleteIf(key string, pred func(old []byte) bool) bool {
	shard := s.shardFor(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	return shard.store.DeleteIf(key, pred)
}
