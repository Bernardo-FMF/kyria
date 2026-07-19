package store

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

// fillPastCap writes n distinct keys through Set, which IS subject to admission, so the
// store ends up at its cap with TinyLFU's sketch warmed up — the state in which a plain
// Set of an unseen key is likely to be refused.
func fillPastCap(t *testing.T, s Store, n int) {
	t.Helper()
	for i := range n {
		if _, err := s.Set("fill"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatalf("fill Set: %v", err)
		}
	}
}

// TestUpdateReplica_ReadModifyWrite: fn sees the current value (nil when absent) and its
// result becomes the new value. This is the regression guard for storing `old` instead of
// `fn(old)` — that bug is invisible to the compiler, because an unused func parameter is
// legal Go, and it silently turns every replicated write into a no-op.
func TestUpdateReplica_ReadModifyWrite(t *testing.T) {
	s := NewSharded(4)

	if err := s.UpdateReplica("k", func(old []byte) []byte {
		if old != nil {
			t.Errorf("UpdateReplica of an absent key: old = %q, want nil", old)
		}
		return []byte("v1")
	}); err != nil {
		t.Fatalf("UpdateReplica: %v", err)
	}
	if v, ok := s.Get("k"); !ok || string(v) != "v1" {
		t.Fatalf("after first UpdateReplica: Get = (%q, %v), want (v1, true)", v, ok)
	}

	if err := s.UpdateReplica("k", func(old []byte) []byte {
		return append(append([]byte{}, old...), '!')
	}); err != nil {
		t.Fatalf("UpdateReplica: %v", err)
	}
	if v, ok := s.Get("k"); !ok || string(v) != "v1!" {
		t.Errorf("after second UpdateReplica: Get = (%q, %v), want (v1!, true)", v, ok)
	}
}

// TestUpdateReplica_KeepsNewcomerOnAFullStore is the fix itself: a replicated write into a
// store already at its cap must land. Repeated over many distinct keys because TinyLFU's
// admission is probabilistic — a single key proves nothing, but "all of them" does.
func TestUpdateReplica_KeepsNewcomerOnAFullStore(t *testing.T) {
	const capacity = 16
	s := NewSharded(1, WithMaxEntries(capacity), WithPolicy(NewTinyLFU(capacity)))
	fillPastCap(t, s, capacity*8)

	for i := range 50 {
		key := "replica" + strconv.Itoa(i)
		if err := s.UpdateReplica(key, func(old []byte) []byte { return []byte("v") }); err != nil {
			t.Fatalf("UpdateReplica(%s): %v", key, err)
		}
		if _, ok := s.Get(key); !ok {
			t.Fatalf("%s was dropped by admission — a replica write must never be refused", key)
		}
	}
}

// TestUpdateReplica_StillEnforcesTheCap: the bypass changes WHO loses, not WHETHER anyone
// does. Without this the fix would trade a durability bug for unbounded memory.
func TestUpdateReplica_StillEnforcesTheCap(t *testing.T) {
	const capacity = 16
	s := NewSharded(1, WithMaxEntries(capacity), WithPolicy(NewTinyLFU(capacity)))

	for i := range 500 {
		if err := s.UpdateReplica("k"+strconv.Itoa(i), func(old []byte) []byte { return []byte("v") }); err != nil {
			t.Fatalf("UpdateReplica: %v", err)
		}
	}

	if got := s.Size(); got > capacity {
		t.Errorf("size = %d after 500 replica writes, want <= %d", got, capacity)
	}
}

// TestSet_StillRejectsOnAFullStore: the standalone path keeps its admission filter. If this
// ever passes trivially, the bypass leaked into Set and TinyLFU has been disabled outright.
func TestSet_StillRejectsOnAFullStore(t *testing.T) {
	const capacity = 16
	s := NewSharded(1, WithMaxEntries(capacity), WithPolicy(NewTinyLFU(capacity)))
	fillPastCap(t, s, capacity*8)

	rejected := 0
	for i := range 200 {
		admitted, err := s.Set("cold"+strconv.Itoa(i), []byte("v"))
		if err != nil {
			t.Fatalf("Set: %v", err)
		}
		if !admitted {
			rejected++
		}
	}

	if rejected == 0 {
		t.Error("no Set was refused on a full TinyLFU store — the admission filter is not running")
	}
}

// TestUpdateReplica_PropagatesSizeErrors: bypassing admission does not bypass validation.
// The size check runs against fn's OUTPUT, which is what makes this catch a call that
// validated the old value instead of the new one.
func TestUpdateReplica_PropagatesSizeErrors(t *testing.T) {
	s := NewSharded(1, WithMaxValueSize(8))

	err := s.UpdateReplica("k", func(old []byte) []byte { return []byte(strings.Repeat("x", 64)) })
	if !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("UpdateReplica with an oversized value = %v, want ErrValueTooLarge", err)
	}
	if _, ok := s.Get("k"); ok {
		t.Error("a rejected oversized value was stored anyway")
	}

	if err := s.UpdateReplica("", func(old []byte) []byte { return []byte("v") }); !errors.Is(err, ErrEmptyKey) {
		t.Errorf("UpdateReplica with an empty key = %v, want ErrEmptyKey", err)
	}
}
