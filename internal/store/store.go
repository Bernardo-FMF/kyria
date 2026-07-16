// Package store defines kyria's core key-value storage domain: an in-memory
// map from string keys to opaque byte-slice values.
package store

import (
	"errors"
	"time"
)

// Default size limits, in bytes. They bound how much memory a single entry may
// consume and are enforced by Set. Can be overridden in the store creation.
const (
	DefaultMaxKeySize   = 1 << 10 // 1 KiB
	DefaultMaxValueSize = 1 << 20 // 1 MiB
)

// Sentinel errors returned by Set.
var (
	// ErrEmptyKey is returned when the supplied key is empty.
	ErrEmptyKey = errors.New("store: key must not be empty")
	// ErrKeyTooLarge is returned when the key exceeds the configured maximum.
	ErrKeyTooLarge = errors.New("store: key exceeds maximum size")
	// ErrValueTooLarge is returned when the value exceeds the configured maximum.
	ErrValueTooLarge = errors.New("store: value exceeds maximum size")
	// ErrInvalidTTL is returned by SetWithTTL when the supplied ttl is not
	// positive. Use Set for entries that should never expire.
	ErrInvalidTTL = errors.New("store: ttl must be positive")
)

// Store is kyria's core key-value abstraction, mapping string keys to
// opaque byte-slice values.
type Store interface {
	// Get returns the value stored for key and whether it was present.
	Get(key string) (value []byte, found bool)
	// Set stores value under key with no expiry. It reports whether the entry
	// was admitted — an eviction policy may reject a new key when the store is
	// full — and returns a sentinel error if key or value violates the
	// configured size limits.
	Set(key string, value []byte) (admitted bool, err error)
	// SetWithTTL stores value under key with a time-to-live: Get treats the
	// entry as absent once ttl has elapsed. ttl must be positive, otherwise
	// SetWithTTL returns ErrInvalidTTL. Like Set, it reports whether the entry
	// was admitted. Size limits apply as with Set.
	SetWithTTL(key string, value []byte, ttl time.Duration) (admitted bool, err error)
	// Update atomically replaces key's value with fn(old): it reads the current
	// value (nil if absent), calls fn to compute the new value, and stores that —
	// as one indivisible read-modify-write, so concurrent Updates to the same key
	// cannot interleave and lose a write. It is the primitive the replication layer
	// uses to fold a new version into a key's stored sibling set. Admission and
	// size-limit results are reported as by Set.
	Update(key string, fn func(old []byte) []byte) (admitted bool, err error)
	// Delete removes key, reporting whether it had been present.
	Delete(key string) (deleted bool)
	// Size reports the number of entries currently stored.
	Size() int
	// Range calls fn for every live entry currently stored, skipping expired-but-unreaped
	// ones (the same view as Get). The value passed to fn aliases internal storage — fn
	// must only read it, never mutate it — and, under ShardedStore, runs while a shard lock
	// is held, so fn must not call back into the store.
	Range(fn func(key string, value []byte))
	// DeleteIf removes key only when pred reports true for its current value, with the
	// read of that value and the removal performed as one indivisible step under the key's
	// lock — so no concurrent write can slip between the check and the delete. A missing key
	// (absent, or expired-but-unreaped) is a no-op: pred is not consulted and DeleteIf reports
	// false. pred runs under the key's lock, so it must only read the bytes it is handed and
	// must not call back into the store. It is the primitive the tombstone GC uses to reap a
	// key only while doing so is still safe.
	DeleteIf(key string, pred func(old []byte) bool) (deleted bool)
}
