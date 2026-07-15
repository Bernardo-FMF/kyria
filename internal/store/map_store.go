package store

import (
	"sync/atomic"
	"time"
)

// MapStore is an in-memory Store backed by a Go map.
//
// It is NOT safe for concurrent use: its methods must not be called from
// multiple goroutines simultaneously. Concurrency-safe access is provided by
// ShardedStore, which wraps a set of MapStore shards behind per-shard locks.
type MapStore struct {
	maxKeySize   int
	maxValueSize int
	data         map[string]entry
	maxEntries   int    // per-shard cap; 0 = unbounded. Global ≈ maxEntries × shards.
	policy       Policy // eviction strategy; nil = no eviction
}

// entry is a stored value together with its expiry. A zero expiresAt means the
// entry never expires.
type entry struct {
	value     []byte
	expiresAt time.Time
	// hint is the entry's eviction score, owned by the Policy (nil when the store
	// has no policy). It's a pointer to an atomic so entry stays a copyable map
	// value while every copy shares one counter, letting Get update it under a
	// read lock without a data race.
	hint *atomic.Uint64
}

// expired reports whether the entry has expired as of now. now is passed in
// (rather than read from time.Now inside) to keep the predicate pure and
// easily testable.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Option configures a MapStore during construction (see New and the With...
// helpers). This is the functional-options pattern: each option is a closure
// that sets one field, so New stays source-compatible as options are added.
type Option func(*MapStore)

// WithMaxKeySize overrides the maximum accepted key size, in bytes.
func WithMaxKeySize(size int) Option {
	return func(m *MapStore) {
		m.maxKeySize = size
	}
}

// WithMaxValueSize overrides the maximum accepted value size, in bytes.
func WithMaxValueSize(size int) Option {
	return func(m *MapStore) {
		m.maxValueSize = size
	}
}

// WithMaxEntries sets the maximum number of entries a store holds (per shard)
// before eviction begins. Zero, the default, means unbounded. It takes effect
// only in combination with WithPolicy.
func WithMaxEntries(n int) Option {
	return func(m *MapStore) {
		m.maxEntries = n
	}
}

// WithPolicy installs an eviction policy. It takes a factory, not a Policy value,
// because NewSharded builds one MapStore per shard and each shard needs its own
// policy instance — a shared one would be mutated under different shard locks.
func WithPolicy(newPolicy func() Policy) Option {
	return func(m *MapStore) {
		m.policy = newPolicy()
	}
}

// New returns a MapStore initialized with the default size limits, overridden
// by any supplied options.
func New(opts ...Option) *MapStore {
	m := &MapStore{
		maxKeySize:   DefaultMaxKeySize,
		maxValueSize: DefaultMaxValueSize,
		data:         make(map[string]entry),
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Compile-time assertion that *MapStore satisfies Store.
var _ Store = (*MapStore)(nil)

// Get returns the value stored for key. The returned slice aliases internal
// storage and must not be modified by the caller.
//
// Under ShardedStore, Get holds only a read lock, so it must not write the map.
// That governs two things here: expiry is lazy (an expired entry is reported
// absent but not deleted — reclamation is left to a later Set or the janitor),
// and the eviction policy records the access via an atomic hint rather than
// touching the map. Concurrent readers may update atomics; they must not mutate
// the map.
func (m *MapStore) Get(key string) ([]byte, bool) {
	e, ok := m.data[key]
	if !ok {
		return nil, false
	}
	if e.expired(time.Now()) {
		return nil, false
	}
	// Record the read for the eviction policy. recordAccess only touches atomic
	// state, so it is safe here under the read lock.
	if m.policy != nil {
		m.policy.recordAccess(key, e.hint)
	}
	return e.value, true
}

// Set stores a private copy of value under key with no expiry. It reports whether
// the entry was admitted (an eviction policy may reject a new key when the store
// is full) and returns a sentinel error on any size-limit violation.
func (m *MapStore) Set(key string, value []byte) (bool, error) {
	return m.set(key, value, time.Time{})
}

// Update reads key's current value, applies fn, and stores the result — the read and
// write as one call. MapStore itself is not synchronized, so on its own this is only
// atomic within a single goroutine; ShardedStore holds the key's lock across the
// whole call to make it atomic under concurrency.
func (m *MapStore) Update(key string, fn func(old []byte) []byte) (bool, error) {
	old, _ := m.Get(key) // absent key: old == nil
	return m.Set(key, fn(old))
}

// SetWithTTL stores a private copy of value under key with a time-to-live: Get
// treats the entry as absent once ttl has elapsed. ttl must be positive,
// otherwise SetWithTTL returns ErrInvalidTTL. Size limits apply as with Set.
func (m *MapStore) SetWithTTL(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidTTL
	}
	return m.set(key, value, time.Now().Add(ttl))
}

// set validates key and value against the configured limits, then stores a
// private copy of value with the given expiry. It reports whether the entry was
// admitted, and is the shared implementation behind Set (a zero expiresAt) and
// SetWithTTL (now + ttl).
func (m *MapStore) set(key string, value []byte, expiresAt time.Time) (bool, error) {
	if len(key) == 0 {
		return false, ErrEmptyKey
	}
	if len(key) > m.maxKeySize {
		return false, ErrKeyTooLarge
	}
	if len(value) > m.maxValueSize {
		return false, ErrValueTooLarge
	}

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	e := entry{value: valueCopy, expiresAt: expiresAt}
	if m.policy != nil {
		e.hint = new(atomic.Uint64)
	}
	_, existed := m.data[key]
	m.data[key] = e

	if m.policy == nil {
		return true, nil
	}
	m.policy.recordInsert(key, e.hint)
	if existed {
		// An update does not grow the store, so it is always admitted.
		return true, nil
	}
	// A new key may overflow the cap; evictIfNeeded decides whether it stays.
	return m.evictIfNeeded(key, e.hint), nil
}

// evictionSampleSize is how many entries sampleVictim inspects. Redis's default
// is 5; higher is more accurate but slower.
const evictionSampleSize = 5

// evictIfNeeded enforces the per-shard cap after a new key has been inserted and
// reports whether that newcomer was admitted (kept). It runs from set under the
// write lock, so deleting from the map here is safe.
//
// Within the cap there is nothing to do. Over it, the weakest incumbent (from
// sampleVictim) and the newcomer are scored and handed to the policy's admit: if
// the newcomer wins, the incumbent is evicted; otherwise the newcomer itself is
// evicted (rejected). Plain LRU/LFU always admit; TinyLFU is where admit can
// refuse.
func (m *MapStore) evictIfNeeded(newKey string, newHint *atomic.Uint64) bool {
	if m.maxEntries == 0 || m.Size() <= m.maxEntries {
		return true
	}
	k, h, ok := m.sampleVictim(newKey)
	if !ok {
		return true
	}

	scoredNewKey := m.policy.score(newKey, newHint)
	scoredVictimKey := m.policy.score(k, h)
	admitted := m.policy.admit(scoredNewKey, scoredVictimKey)

	if admitted {
		delete(m.data, k)
	} else {
		delete(m.data, newKey)
	}

	return admitted
}

// sampleVictim returns the weakest incumbent — the entry with the lowest
// policy.score — to consider evicting, skipping excludeKey (the newcomer, which
// admit judges separately). It inspects at most evictionSampleSize entries; Go
// randomizes map iteration, so those form a random sample, keeping eviction
// O(1)-ish at the cost of an approximate victim. ok is false only if the sample
// was empty.
func (m *MapStore) sampleVictim(excludeKey string) (key string, hint *atomic.Uint64, ok bool) {
	var chosenKey string
	var chosenHint *atomic.Uint64
	var smallestScore uint64
	var counter int
	for k, e := range m.data {
		if k == excludeKey {
			continue
		}

		tmpCount := m.policy.score(k, e.hint)
		if counter == 0 || smallestScore > tmpCount {
			chosenKey = k
			chosenHint = e.hint
			smallestScore = tmpCount
		}

		counter++
		if counter >= evictionSampleSize {
			break
		}
	}

	return chosenKey, chosenHint, counter > 0
}

// Delete removes key and reports whether it was present beforehand.
func (m *MapStore) Delete(key string) bool {
	_, ok := m.data[key]
	delete(m.data, key)
	return ok
}

// Size reports the number of entries currently stored.
//
// With lazy expiry this counts entries physically present, including any that
// have expired but not yet been reclaimed (Get hides them but does not remove
// them). Reclaiming those entries is the job of the active janitor.
func (m *MapStore) Size() int {
	return len(m.data)
}

// Range calls fn for every live entry, skipping any that have expired (lazy expiry as in
// Get: an expired entry is passed over, not deleted — reclamation is the janitor's job).
// It takes no lock; MapStore is the single-threaded core, and ShardedStore.Range supplies
// the per-shard lock. The value passed to fn aliases internal storage, so fn must only read it.
func (m *MapStore) Range(fn func(key string, value []byte)) {
	now := time.Now()
	for k, e := range m.data {
		if e.expired(now) {
			continue
		}

		fn(k, e.value)
	}
}
