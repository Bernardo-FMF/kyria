package store

// MapStore is an in-memory Store backed by a Go map.
//
// It is NOT safe for concurrent use: its methods must not be called from
// multiple goroutines simultaneously. Concurrency safety arrives in Phase 2.
type MapStore struct {
	maxKeySize   int
	maxValueSize int
	data         map[string][]byte
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
		data:         make(map[string][]byte),
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
func (m *MapStore) Get(key string) ([]byte, bool) {
	v, ok := m.data[key]
	return v, ok
}

// Set validates key and value against the configured limits, then stores a
// private copy of value. It returns a sentinel error on any limit violation.
func (m *MapStore) Set(key string, value []byte) error {
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

	m.data[key] = valueCopy

	return nil
}

// Delete removes key and reports whether it was present beforehand.
func (m *MapStore) Delete(key string) bool {
	_, ok := m.data[key]
	delete(m.data, key)
	return ok
}

// Size reports the number of entries currently stored.
func (m *MapStore) Size() int {
	return len(m.data)
}
