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

// entry is a stored value together with its expiry and its hint (eviction score).
type entry struct {
	value     []byte
	expiresAt time.Time // 0 means the entry never expires
	// hint is the entry's eviction score, owned by the Policy (nil when the store
	// has no policy). It's a pointer to an atomic so entry stays a copyable map
	// value while every copy shares one counter, letting Get update it under a
	// read lock without a data race.
	hint *atomic.Uint64
}

// expired reports whether the entry has expired as of now. now is passed in
// (rather than read from time.Now inside) to keep the predicate consistent
// if called for multiple entries.
func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Option configures a MapStore during construction.
// Each option is a closure that sets one field.
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
// policy instance - because if we had a shared one it would be
// mutated under different shard locks.
func WithPolicy(newPolicy func() Policy) Option {
	return func(m *MapStore) {
		m.policy = newPolicy()
	}
}

// New returns a MapStore initialized with the default values, overridden
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
// Expiry is lazy (an expired entry is reported absent but not deleted -
// reclamation is left to a later Set or the janitor)-
// The eviction policy records the access via an atomic hint rather than
// touching the map, because concurrent readers may update atomics;
func (m *MapStore) Get(key string) ([]byte, bool) {
	e, ok := m.data[key]
	if !ok {
		return nil, false
	}
	// Lazy expiry, leave the removal to the janitor goroutine.
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
	return m.set(key, value, time.Time{}, false)
}

// UpdateReplica applies fn to key's value as an atomic read-modify-write operation.
// It bypasses the admission filter, so a full store cannot discard it.
func (m *MapStore) UpdateReplica(key string, fn func(old []byte) []byte) error {
	old, _ := m.Get(key)
	_, err := m.set(key, fn(old), time.Time{}, true)
	return err
}

// SetWithTTL stores a private copy of value under key with a time-to-live, guaranteeing
// that ttl is a positive value, returning ErrInvalidTTL otherwise.
func (m *MapStore) SetWithTTL(key string, value []byte, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidTTL
	}
	return m.set(key, value, time.Now().Add(ttl), false)
}

// set validates key and value against the configured limits, then stores a
// private copy of value with the given expiry. It reports whether the entry was
// admitted, and is the shared implementation behind Set (a zero expiresAt),
// SetWithTTL (now + ttl), and UpdateReplica (bypassAdmission).
func (m *MapStore) set(key string, value []byte, expiresAt time.Time, bypassAdmission bool) (bool, error) {
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
	return m.evictIfNeeded(key, e.hint, bypassAdmission), nil
}

// evictionSampleSize is how many entries sampleVictim inspects.
// Higher values are more accurate but slower.
const evictionSampleSize = 5

// evictIfNeeded enforces the per-shard cap after a new key has been inserted and
// reports whether that newcomer was admitted into the store.
// It runs from set under the write lock, so deleting from the map here is safe.
//
// Within the cap there is nothing to do. Over it, the weakest incumbent (from
// sampleVictim) and the newcomer are scored and handed to the policy's admit: if
// the newcomer wins, the incumbent is evicted; otherwise the newcomer itself is
// evicted (rejected). Plain LRU/LFU always admit; TinyLFU is where admit can
// refuse. bypassAdmission skips that judgement: the incumbent is always the one evicted.
func (m *MapStore) evictIfNeeded(newKey string, newHint *atomic.Uint64, bypassAdmission bool) bool {
	if m.maxEntries == 0 || m.Size() <= m.maxEntries {
		return true
	}
	k, h, ok := m.sampleVictim(newKey)
	if !ok {
		return true
	}

	scoredNewKey := m.policy.score(newKey, newHint)
	scoredVictimKey := m.policy.score(k, h)
	admitted := bypassAdmission
	if !bypassAdmission {
		admitted = m.policy.admit(scoredNewKey, scoredVictimKey)
	}

	if admitted {
		delete(m.data, k)
	} else {
		delete(m.data, newKey)
	}

	return admitted
}

// sampleVictim returns the weakest incumbent (the entry with the lowest policy.score)
// to consider evicting, skipping excludeKey (the newcomer, which admit judges separately).
// It inspects at most evictionSampleSize entries in a randomized map iteration,
// so the selected entry might not be the one with the lowest score in the entire store,
// but it's an approximation.
// Ok is false only if the sample was empty.
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
// them). Reclaiming those entries is the job of the janitor goroutine.
func (m *MapStore) Size() int {
	return len(m.data)
}

// Range calls fn for every live entry, skipping any that have expired.
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

// DeleteIf removes key when pred returns true for its current value, reporting whether it
// removed it; a missing key short-circuits to false without calling pred. Like the other
// MapStore methods it takes no lock of its own — ShardedStore.DeleteIf holds the shard's write
// lock across the whole call, which is what makes the check-and-delete atomic under concurrency.
func (m *MapStore) DeleteIf(key string, pred func(old []byte) bool) bool {
	b, ok := m.Get(key)
	if !ok {
		return false
	}

	v := pred(b)
	if v {
		return m.Delete(key)
	}
	return false
}
