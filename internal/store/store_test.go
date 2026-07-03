package store

import (
	"bytes"
	"errors"
	"testing"
)

func TestMapStore_SetAndGet(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value []byte
	}{
		{name: "simple value", key: "greeting", value: []byte("hello")},
		{name: "empty value", key: "empty", value: []byte{}},
		{name: "binary value", key: "bin", value: []byte{0x00, 0xff, 0x10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if err := s.Set(tt.key, tt.value); err != nil {
				t.Fatalf("Set(%q) returned unexpected error: %v", tt.key, err)
			}

			got, found := s.Get(tt.key)
			if !found {
				t.Fatalf("Get(%q) found=false, want true", tt.key)
			}
			if !bytes.Equal(got, tt.value) {
				t.Errorf("Get(%q) = %q, want %q", tt.key, got, tt.value)
			}
		})
	}
}

func TestMapStore_Get_missingKey(t *testing.T) {
	s := New()
	got, found := s.Get("does-not-exist")
	if found {
		t.Errorf("Get on missing key found=true, want false")
	}
	if got != nil {
		t.Errorf("Get on missing key = %q, want nil", got)
	}
}

func TestMapStore_Set_overwrite(t *testing.T) {
	s := New()
	mustSet(t, s, "k", []byte("first"))
	mustSet(t, s, "k", []byte("second"))

	got, _ := s.Get("k")
	if !bytes.Equal(got, []byte("second")) {
		t.Errorf("after overwrite Get = %q, want %q", got, "second")
	}
	if s.Size() != 1 {
		t.Errorf("Size after overwrite = %d, want 1", s.Size())
	}
}

func TestMapStore_Set_validation(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		key     string
		value   []byte
		wantErr error
	}{
		{
			name:    "empty key rejected",
			key:     "",
			value:   []byte("v"),
			wantErr: ErrEmptyKey,
		},
		{
			name:    "oversized key rejected",
			opts:    []Option{WithMaxKeySize(4)},
			key:     "toolong",
			value:   []byte("v"),
			wantErr: ErrKeyTooLarge,
		},
		{
			name:    "oversized value rejected",
			opts:    []Option{WithMaxValueSize(2)},
			key:     "k",
			value:   []byte("toolong"),
			wantErr: ErrValueTooLarge,
		},
		{
			name:    "key at limit accepted",
			opts:    []Option{WithMaxKeySize(4)},
			key:     "abcd",
			value:   []byte("v"),
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(tt.opts...)
			err := s.Set(tt.key, tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Set error = %v, want %v", err, tt.wantErr)
			}

			// A rejected Set must not store anything.
			if tt.wantErr != nil && s.Size() != 0 {
				t.Errorf("Size after rejected Set = %d, want 0", s.Size())
			}
		})
	}
}

func TestMapStore_Delete(t *testing.T) {
	s := New()
	mustSet(t, s, "k", []byte("v"))

	if deleted := s.Delete("k"); !deleted {
		t.Errorf("Delete(existing) = false, want true")
	}
	if _, found := s.Get("k"); found {
		t.Errorf("key still present after Delete")
	}
	if deleted := s.Delete("k"); deleted {
		t.Errorf("Delete(already-deleted) = true, want false")
	}
}

func TestMapStore_Size(t *testing.T) {
	s := New()
	if s.Size() != 0 {
		t.Fatalf("fresh store Size = %d, want 0", s.Size())
	}
	mustSet(t, s, "a", []byte("1"))
	mustSet(t, s, "b", []byte("2"))
	if s.Size() != 2 {
		t.Errorf("Size = %d, want 2", s.Size())
	}
}

// TestMapStore_Set_defensiveCopy verifies that mutating the caller's slice
// after Set does not corrupt the stored value, and that mutating a slice
// returned by Get does not corrupt the store on the next Get. This pins down
// the ownership contract documented on the Store interface.
func TestMapStore_Set_defensiveCopy(t *testing.T) {
	s := New()
	original := []byte("immutable")
	mustSet(t, s, "k", original)

	// Mutate the caller's slice after handing it to Set.
	original[0] = 'X'

	got, _ := s.Get("k")
	if !bytes.Equal(got, []byte("immutable")) {
		t.Errorf("stored value mutated via caller slice: got %q", got)
	}
}

// mustSet is a tiny test helper that fails the test on a Set error. Marking it
// with t.Helper() makes failure messages point at the calling line, not here.
func mustSet(t *testing.T, s *MapStore, key string, value []byte) {
	t.Helper()
	if err := s.Set(key, value); err != nil {
		t.Fatalf("Set(%q) unexpected error: %v", key, err)
	}
}
