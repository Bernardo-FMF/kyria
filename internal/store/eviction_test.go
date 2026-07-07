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
		if _, err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
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
		if _, err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Re-access k0 so it becomes most-recently-used; k1 is now the oldest.
	if _, ok := m.Get("k0"); !ok {
		t.Fatal("k0 missing")
	}
	// Overflow: the whole store (capacity+1 == evictionSampleSize) is sampled, so
	// the true LRU victim — k1 — is evicted.
	if _, err := m.Set("kNew", []byte("v")); err != nil {
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
		if _, err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
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
		if _, err := s.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
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
		if _, err := s.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
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
					_, _ = s.Set("k"+strconv.Itoa(i%256), []byte("v"))
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

	p.recordInsert("a", &a)
	p.recordInsert("b", &b)
	if a.Load() >= b.Load() {
		t.Errorf("b (%d) should be stamped after a (%d)", b.Load(), a.Load())
	}

	p.recordAccess("a", &a) // a is now the most recent
	if a.Load() <= b.Load() {
		t.Errorf("after access, a (%d) should outrank b (%d)", a.Load(), b.Load())
	}
}

// TestEviction_LFUEvictsLeastFrequentlyUsed: pure LFU always admits the newcomer
// and evicts the least-frequently-used incumbent. Distinct access counts leave k1
// the unique least-used, so it's the victim while kNew is admitted. cap =
// evictionSampleSize-1, so every incumbent is sampled (exact eviction).
func TestEviction_LFUEvictsLeastFrequentlyUsed(t *testing.T) {
	capacity := evictionSampleSize - 1
	m := New(WithMaxEntries(capacity), WithPolicy(NewLFU))

	for i := 0; i < capacity; i++ {
		if _, err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Distinct access counts leave k1 the unique least-frequently-used incumbent.
	for r := 0; r < 3; r++ {
		m.Get("k0")
	}
	m.Get("k2")
	m.Get("k2")
	m.Get("k3")

	admitted, err := m.Set("kNew", []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Error("kNew should have been admitted (LFU always admits)")
	}
	if _, ok := m.Get("k1"); ok {
		t.Error("k1 (least frequently used) should have been evicted")
	}
	for _, k := range []string{"k0", "k2", "k3", "kNew"} {
		if _, ok := m.Get(k); !ok {
			t.Errorf("%s should have survived", k)
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

	if _, err := m.Set("hot", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		m.Get("hot") // make it very frequently used
	}
	// Churn many cold keys through the store.
	for i := 0; i < 200; i++ {
		if _, err := m.Set("cold"+strconv.Itoa(i), []byte("v")); err != nil {
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

	p.recordInsert("k", &h)
	base := h.Load()
	if base == 0 {
		t.Error("recordInsert should seed a nonzero base count")
	}
	p.recordAccess("k", &h)
	if h.Load() != base+1 {
		t.Errorf("after one access, count = %d, want %d", h.Load(), base+1)
	}
}

// TestTinyLFU_ScoreReflectsFrequency: an unseen key scores 0; recording hits
// raises the score. TinyLFU keys off the key and ignores the per-entry hint, so
// we pass nil for it.
func TestTinyLFU_ScoreReflectsFrequency(t *testing.T) {
	p := NewTinyLFU(128)()

	if got := p.score("x", nil); got != 0 {
		t.Errorf("score of unseen key = %d, want 0", got)
	}

	p.recordInsert("x", nil)
	p.recordAccess("x", nil)
	p.recordAccess("x", nil)
	if got := p.score("x", nil); got < 3 {
		t.Errorf("score after 3 records = %d, want >= 3 (never undercounts)", got)
	}
}

// TestTinyLFU_Admit: the newcomer is admitted only if it's STRICTLY more frequent
// than the victim it would displace; ties favor the incumbent.
func TestTinyLFU_Admit(t *testing.T) {
	p := NewTinyLFU(128)()

	tests := []struct {
		candidate, victim uint64
		want              bool
	}{
		{candidate: 5, victim: 3, want: true},
		{candidate: 3, victim: 5, want: false},
		{candidate: 3, victim: 3, want: false},
	}
	for _, tc := range tests {
		if got := p.admit(tc.candidate, tc.victim); got != tc.want {
			t.Errorf("admit(%d, %d) = %v, want %v", tc.candidate, tc.victim, got, tc.want)
		}
	}
}

// TestEviction_TinyLFUAdmission: TinyLFU rejects a cold newcomer that can't beat
// the weakest incumbent, then admits that same key once repeated requests push its
// estimated frequency above the victim's. The sketch is sized generously so, for
// this handful of keys, its estimates are effectively exact.
func TestEviction_TinyLFUAdmission(t *testing.T) {
	const capacity = evictionSampleSize - 1 // 4, so every incumbent is sampled
	m := New(WithMaxEntries(capacity), WithPolicy(NewTinyLFU(1024)))

	// k0 is the weak victim (inserted, never accessed → frequency 1). k1..k3 are
	// hammered so they're clearly more frequent than any newcomer.
	for i := 0; i < capacity; i++ {
		if _, err := m.Set("k"+strconv.Itoa(i), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < capacity; i++ {
		for r := 0; r < 20; r++ {
			m.Get("k" + strconv.Itoa(i))
		}
	}

	// First request for "new": frequency 1 does not beat k0's 1 (ties go to the
	// incumbent), so it's rejected and not stored.
	admitted, err := m.Set("new", []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if admitted {
		t.Error("first Set(new) should have been rejected (does not beat the victim)")
	}
	if _, ok := m.Get("new"); ok {
		t.Error("new should not be stored after rejection")
	}

	// Second request bumps new's frequency to 2, which now beats k0's 1 → admitted,
	// and k0 (the least-frequent incumbent) is the one evicted.
	admitted, err = m.Set("new", []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Error("second Set(new) should have been admitted (now beats the victim)")
	}
	if _, ok := m.Get("new"); !ok {
		t.Error("new should be stored after admission")
	}
	if _, ok := m.Get("k0"); ok {
		t.Error("k0 (least frequent) should have been evicted")
	}
}

// TestEviction_TinyLFUConcurrent hammers a TinyLFU store from many goroutines.
// recordAccess updates the shared sketch under the read lock, so the sketch's
// counters must be atomic — running this under -race is what proves it. Run:
//
//	go test ./internal/store/ -run TinyLFUConcurrent -race
func TestEviction_TinyLFUConcurrent(t *testing.T) {
	s := NewSharded(8, WithMaxEntries(64), WithPolicy(NewTinyLFU(1024)))
	for i := 0; i < 256; i++ {
		_, _ = s.Set("k"+strconv.Itoa(i), []byte("v"))
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
					_, _ = s.Set("k"+strconv.Itoa(i%256), []byte("v"))
				}
			}
		}()
	}
	wg.Wait()
}
