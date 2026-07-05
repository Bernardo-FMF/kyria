package store

import (
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
}

// entry is a stored value together with its expiry. A zero expiresAt means the
// entry never expires.
type entry struct {
	value     []byte
	expiresAt time.Time
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
// Expiry is lazy: an expired entry is reported absent but is not removed here.
// Get is a read — under ShardedStore it holds only a read lock — so deleting
// would write the map under that lock and race. Reclamation happens on a later
// overwriting Set (or, eventually, the active janitor).
func (m *MapStore) Get(key string) ([]byte, bool) {
	e, ok := m.data[key]
	if !ok {
		return nil, false
	}
	if e.expired(time.Now()) {
		return nil, false
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

	m.data[key] = entry{value: valueCopy, expiresAt: expiresAt}

	return nil
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
