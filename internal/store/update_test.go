package store

import (
	"strconv"
	"sync"
	"testing"
)

// TestUpdate_ReadModifyWrite: fn sees the current value (nil when absent) and its
// result becomes the new value.
func TestUpdate_ReadModifyWrite(t *testing.T) {
	s := NewSharded(4)

	// Absent key: fn sees nil.
	if _, err := s.Update("k", func(old []byte) []byte {
		if old != nil {
			t.Errorf("Update of an absent key: old = %q, want nil", old)
		}
		return []byte("v1")
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if v, ok := s.Get("k"); !ok || string(v) != "v1" {
		t.Errorf("after first Update: Get = (%q, %v), want (v1, true)", v, ok)
	}

	// Present key: fn sees the stored value and transforms it.
	if _, err := s.Update("k", func(old []byte) []byte {
		return append(append([]byte{}, old...), '!')
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if v, ok := s.Get("k"); !ok || string(v) != "v1!" {
		t.Errorf("after second Update: Get = (%q, %v), want (v1!, true)", v, ok)
	}
}

// TestUpdate_Atomic is the reason the primitive exists: many goroutines increment a
// counter held under ONE key. Each increment is a read-modify-write; if Update didn't
// hold the key's lock across the whole thing, increments would race and be lost (and
// -race would flag the map access). A correct atomic Update yields exactly N.
func TestUpdate_Atomic(t *testing.T) {
	s := NewSharded(4)

	const workers, iters = 8, 200

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				s.Update("counter", func(old []byte) []byte {
					n := 0
					if old != nil {
						n, _ = strconv.Atoi(string(old))
					}
					return []byte(strconv.Itoa(n + 1))
				})
			}
		}()
	}
	wg.Wait()

	v, _ := s.Get("counter")
	n, _ := strconv.Atoi(string(v))
	if want := workers * iters; n != want {
		t.Errorf("counter = %d, want %d (lost updates → Update isn't atomic)", n, want)
	}
}
