package store

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// TestEviction_BoundsSize: with a policy and a per-shard cap, a MapStore never
// exceeds the cap no matter how many distinct keys are inserted.
func TestEviction_BoundsSize(t *testing.T) {
	const capacity = 4
	m := New(WithMaxEntries(capacity), WithPolicy(NewLRU))

	for i := 0; i < 100; i++ {
		if err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if m.Size() > capacity {
			t.Fatalf("Size = %d after %d inserts, want <= %d", m.Size(), i+1, capacity)
		}
	}
	if m.Size() != capacity {
		t.Errorf("final Size = %d, want %d", m.Size(), capacity)
	}
}

// TestEviction_LRUEvictsLeastRecentlyUsed: when the whole store fits inside a
// single sample, eviction is exact — so we can assert the least-recently-used
// key is the victim. The cap is evictionSampleSize-1, so the overflow insert
// samples every entry.
func TestEviction_LRUEvictsLeastRecentlyUsed(t *testing.T) {
	capacity := evictionSampleSize - 1
	m := New(WithMaxEntries(capacity), WithPolicy(NewLRU))

	for i := 0; i < capacity; i++ {
		if err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Re-access k0 so it becomes most-recently-used; k1 is now the oldest.
	if _, ok := m.Get("k0"); !ok {
		t.Fatal("k0 missing")
	}
	// Overflow: the whole store (capacity+1 == evictionSampleSize) is sampled, so
	// the true LRU victim — k1 — is evicted.
	if err := m.Set("kNew", []byte("v")); err != nil {
		t.Fatal(err)
	}

	if _, ok := m.Get("k1"); ok {
		t.Error("k1 (least recently used) should have been evicted")
	}
	for _, k := range []string{"k0", "kNew"} {
		if _, ok := m.Get(k); !ok {
			t.Errorf("%s should still be present", k)
		}
	}
	if m.Size() != capacity {
		t.Errorf("Size = %d, want %d", m.Size(), capacity)
	}
}

// TestEviction_Disabled: without a policy the store is unbounded — Phase-3
// behavior is preserved.
func TestEviction_Disabled(t *testing.T) {
	m := New()
	for i := 0; i < 50; i++ {
		if err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if m.Size() != 50 {
		t.Errorf("Size = %d, want 50 (no eviction without a policy)", m.Size())
	}
}

// TestEviction_Sharded: eviction works through the sharded wrapper; the cap is
// per shard, so the store stays bounded by cap*shards.
func TestEviction_Sharded(t *testing.T) {
	const (
		shards   = 4
		capacity = 8
	)
	s := NewSharded(shards, WithMaxEntries(capacity), WithPolicy(NewLRU))

	for i := 0; i < 1000; i++ {
		if err := s.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Size(); got > shards*capacity {
		t.Errorf("Size = %d, want <= %d (cap %d × %d shards)", got, shards*capacity, capacity, shards)
	}
	if s.Size() == 0 {
		t.Error("Size = 0, expected the store to retain entries")
	}
}

// TestEviction_ConcurrentReadsLockFree: reads update the LRU hint while holding
// only a read lock, so many readers touch the same entries at once. This must be
// race-free — the whole point of the atomic hint. Run under -race.
func TestEviction_ConcurrentReadsLockFree(t *testing.T) {
	s := NewSharded(8, WithMaxEntries(64), WithPolicy(NewLRU))
	for i := 0; i < 256; i++ {
		if err := s.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				s.Get("k" + strconv.Itoa(i%256))
				if i%8 == 0 {
					_ = s.Set("k"+strconv.Itoa(i%256), []byte("v"))
				}
			}
		}()
	}
	wg.Wait()
}

// TestLRU_StampsIncrease: a unit check that the LRU policy stamps a rising
// counter, so a later access outranks an earlier one.
func TestLRU_StampsIncrease(t *testing.T) {
	p := NewLRU()
	var a, b atomic.Uint64

	p.recordInsert(&a)
	p.recordInsert(&b)
	if a.Load() >= b.Load() {
		t.Errorf("b (%d) should be stamped after a (%d)", b.Load(), a.Load())
	}

	p.recordAccess(&a) // a is now the most recent
	if a.Load() <= b.Load() {
		t.Errorf("after access, a (%d) should outrank b (%d)", a.Load(), b.Load())
	}
}

// TestEviction_LFUEvictsLeastFrequentlyUsed: every existing key is accessed at
// least once, so a brand-new key (which starts at the base count) is the unique
// least-frequently-used entry — and the one evicted. cap = evictionSampleSize-1
// so the overflow insert samples every entry (exact eviction).
func TestEviction_LFUEvictsLeastFrequentlyUsed(t *testing.T) {
	capacity := evictionSampleSize - 1
	m := New(WithMaxEntries(capacity), WithPolicy(NewLFU))

	for i := 0; i < capacity; i++ {
		k := "k" + strconv.Itoa(i)
		if err := m.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
		m.Get(k) // one access each → every existing key is above the base count
	}
	// New key: base count, never accessed → the least frequently used.
	if err := m.Set("kNew", []byte("v")); err != nil {
		t.Fatal(err)
	}

	if _, ok := m.Get("kNew"); ok {
		t.Error("kNew (least frequently used) should have been evicted")
	}
	for i := 0; i < capacity; i++ {
		if _, ok := m.Get("k" + strconv.Itoa(i)); !ok {
			t.Errorf("k%d (accessed) should have survived", i)
		}
	}
}

// TestEviction_LFUProtectsHotKey: a frequently-accessed key survives a long churn
// of cold keys, because its high count keeps it off the eviction radar. cap =
// evictionSampleSize-1, so every eviction samples the whole store (hot included)
// and hot is never the smallest count.
func TestEviction_LFUProtectsHotKey(t *testing.T) {
	capacity := evictionSampleSize - 1
	m := New(WithMaxEntries(capacity), WithPolicy(NewLFU))

	if err := m.Set("hot", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		m.Get("hot") // make it very frequently used
	}
	// Churn many cold keys through the store.
	for i := 0; i < 200; i++ {
		if err := m.Set("cold"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	if _, ok := m.Get("hot"); !ok {
		t.Error("hot key should have survived the churn (LFU protects frequently-used keys)")
	}
}

// TestLFU_CountsIncrease: recordInsert seeds a nonzero base count, and
// recordAccess increments it.
func TestLFU_CountsIncrease(t *testing.T) {
	p := NewLFU()
	var h atomic.Uint64

	p.recordInsert(&h)
	base := h.Load()
	if base == 0 {
		t.Error("recordInsert should seed a nonzero base count")
	}
	p.recordAccess(&h)
	if h.Load() != base+1 {
		t.Errorf("after one access, count = %d, want %d", h.Load(), base+1)
	}
}
