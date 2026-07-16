package store

import (
	"sync"
	"testing"
)

// TestDeleteIf_PredTrueRemoves: a true predicate removes the key and DeleteIf reports true.
func TestDeleteIf_PredTrueRemoves(t *testing.T) {
	m := New()
	m.Set("k", []byte("v"))

	if !m.DeleteIf("k", func(old []byte) bool { return true }) {
		t.Fatal("DeleteIf with a true predicate = false, want true (removed)")
	}
	if _, ok := m.Get("k"); ok {
		t.Error("key still present after DeleteIf removed it")
	}
}

// TestDeleteIf_PredFalseKeeps: a false predicate leaves the key untouched and DeleteIf reports false.
func TestDeleteIf_PredFalseKeeps(t *testing.T) {
	m := New()
	m.Set("k", []byte("v"))

	if m.DeleteIf("k", func(old []byte) bool { return false }) {
		t.Fatal("DeleteIf with a false predicate = true, want false (kept)")
	}
	if v, ok := m.Get("k"); !ok || string(v) != "v" {
		t.Errorf("after DeleteIf kept the key, Get = (%q, %v), want (\"v\", true)", v, ok)
	}
}

// TestDeleteIf_PredSeesCurrentValue: the predicate receives the key's current stored bytes.
func TestDeleteIf_PredSeesCurrentValue(t *testing.T) {
	m := New()
	m.Set("k", []byte("current"))

	var seen []byte
	m.DeleteIf("k", func(old []byte) bool {
		seen = old
		return false
	})
	if string(seen) != "current" {
		t.Errorf("predicate saw %q, want the current value \"current\"", seen)
	}
}

// TestDeleteIf_AbsentKey: DeleteIf on a missing key is a no-op — it reports false and short-circuits
// before consulting the predicate.
func TestDeleteIf_AbsentKey(t *testing.T) {
	m := New()

	called := false
	if m.DeleteIf("missing", func(old []byte) bool { called = true; return true }) {
		t.Error("DeleteIf on an absent key = true, want false")
	}
	if called {
		t.Error("predicate was called for an absent key, want it short-circuited")
	}
}

// TestDeleteIf_Sharded: DeleteIf works through ShardedStore, removing only when the predicate holds.
func TestDeleteIf_Sharded(t *testing.T) {
	s := NewSharded(4)
	s.Set("k", []byte("v"))

	if s.DeleteIf("k", func(old []byte) bool { return false }) {
		t.Fatal("sharded DeleteIf(false) = true, want false")
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatal("sharded DeleteIf(false) removed the key")
	}
	if !s.DeleteIf("k", func(old []byte) bool { return string(old) == "v" }) {
		t.Fatal("sharded DeleteIf(match) = false, want true")
	}
	if _, ok := s.Get("k"); ok {
		t.Error("sharded DeleteIf(match) did not remove the key")
	}
}

// TestDeleteIf_ConcurrentWithSet: DeleteIf holds the shard's write lock across its check-and-delete,
// so it can race Set on the same key with no data race and never leaves torn state — the key ends
// either absent or holding a value that was actually Set. Meaningful under -race.
func TestDeleteIf_ConcurrentWithSet(t *testing.T) {
	s := NewSharded(1) // one shard = maximum contention on the key's lock
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(2 * goroutines)
	for range goroutines {
		go func() { defer wg.Done(); s.Set("k", []byte("v")) }()
		go func() { defer wg.Done(); s.DeleteIf("k", func(old []byte) bool { return true }) }()
	}
	wg.Wait()

	// Whatever the interleaving, the key is either gone or holds exactly the value that was Set.
	if v, ok := s.Get("k"); ok && string(v) != "v" {
		t.Errorf("after concurrent Set/DeleteIf, key = %q, want absent or \"v\"", v)
	}
}
