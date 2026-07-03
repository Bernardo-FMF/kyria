// Package store defines kyria's core key-value storage domain: an in-memory
// map from string keys to opaque byte-slice values.
package store

import (
	"errors"
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
)

// Store is kyria's core key-value abstraction, mapping string keys to
// opaque byte-slice values.
type Store interface {
	// Get returns the value stored for key and whether it was present.
	Get(key string) (value []byte, found bool)
	// Set stores value under key, returning a sentinel error if key or value
	// violates the configured size limits.
	Set(key string, value []byte) error
	// Delete removes key, reporting whether it had been present.
	Delete(key string) (deleted bool)
	// Size reports the number of entries currently stored.
	Size() int
}
