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
	// Record the read for the eviction policy. recordAccess only touches the
	// atomic hint, so it is safe here under the read lock.
	if m.policy != nil {
		m.policy.recordAccess(e.hint)
	}
	return e.value, true
}

// Set stores a private copy of value under key with no expiry. It returns a
// sentinel error on any size-limit violation.
func (m *MapStore) Set(key string, value []byte) error {
	return m.set(key, value, time.Time{})
}

// SetWithTTL stores a private copy of value under key with a time-to-live: Get
// treats the entry as absent once ttl has elapsed. ttl must be positive,
// otherwise SetWithTTL returns ErrInvalidTTL. Size limits apply as with Set.
func (m *MapStore) SetWithTTL(key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return m.set(key, value, time.Now().Add(ttl))
}

// set validates key and value against the configured limits, then stores a
// private copy of value with the given expiry. It is the shared implementation
// behind Set (a zero expiresAt) and SetWithTTL (now + ttl).
func (m *MapStore) set(key string, value []byte, expiresAt time.Time) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > m.maxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > m.maxValueSize {
		return ErrValueTooLarge
	}

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	e := entry{value: valueCopy, expiresAt: expiresAt}
	if m.policy != nil {
		e.hint = new(atomic.Uint64)
	}
	_, existed := m.data[key]
	m.data[key] = e

	if m.policy != nil {
		m.policy.recordInsert(e.hint)
		if !existed {
			// Only a new key grows the store, so only a new key can overflow the cap.
			m.evictIfNeeded()
		}
	}

	return nil
}

// evictionSampleSize is how many entries sampleVictim inspects. Redis's default
// is 5; higher is more accurate but slower.
const evictionSampleSize = 5

// evictIfNeeded drops entries until the store is back within its cap. It is
// called from set under the write lock, so deleting from the map here is safe.
func (m *MapStore) evictIfNeeded() {
	for m.maxEntries > 0 && m.Size() > m.maxEntries {
		victim, ok := m.sampleVictim()
		if !ok {
			break
		}
		delete(m.data, victim)
	}
}

// sampleVictim inspects up to evictionSampleSize entries and returns the key with
// the smallest hint — the policy's least-valuable entry (for LRU, the oldest
// stamp). Go randomizes map iteration, so the first entries seen are effectively
// a random sample; sampling keeps eviction O(1)-ish at the cost of an
// approximate, rather than exact, victim.
func (m *MapStore) sampleVictim() (key string, ok bool) {
	var victim string
	var minHint uint64
	n := 0
	for k, e := range m.data {
		h := e.hint.Load()
		if n == 0 || h < minHint {
			victim, minHint = k, h
		}
		if n++; n >= evictionSampleSize {
			break
		}
	}
	return victim, n > 0
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
