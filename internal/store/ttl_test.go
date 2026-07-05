package store

import (
	"bytes"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestTTL_EntryExpired unit-tests the expiry predicate with explicit timestamps
// — no store, no sleeps. It pins the boundary semantics: an entry is valid up to
// AND INCLUDING expiresAt, and expired strictly after.
func TestTTL_EntryExpired(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	tests := []struct {
		name      string
		expiresAt time.Time
		now       time.Time
		want      bool
	}{
		{"zero never expires", time.Time{}, base, false},
		{"before expiry", base.Add(time.Minute), base, false},
		{"exactly at expiry", base, base, false},
		{"just after expiry", base, base.Add(time.Nanosecond), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := entry{value: []byte("v"), expiresAt: tc.expiresAt}
			if got := e.expired(tc.now); got != tc.want {
				t.Errorf("expired = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTTL_SetWithTTLStoresExpiry checks that SetWithTTL records an expiry of
// roughly now+ttl and that the value round-trips while still live. White-box: it
// reads the stored entry directly to assert on expiresAt.
func TestTTL_SetWithTTLStoresExpiry(t *testing.T) {
	m := New()
	const ttl = time.Minute

	before := time.Now()
	if err := m.SetWithTTL("k", []byte("v"), ttl); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	after := time.Now()

	e, ok := m.data["k"]
	if !ok {
		t.Fatal("entry not stored")
	}
	// expiresAt must land within [before+ttl, after+ttl].
	if e.expiresAt.Before(before.Add(ttl)) || e.expiresAt.After(after.Add(ttl)) {
		t.Errorf("expiresAt = %v, want within [%v, %v]", e.expiresAt, before.Add(ttl), after.Add(ttl))
	}
	// Still live, so Get returns it.
	if got, ok := m.Get("k"); !ok || !bytes.Equal(got, []byte("v")) {
		t.Errorf(`Get = %q, %v; want "v", true`, got, ok)
	}
}

// TestTTL_GetHidesExpired verifies lazy expiry: an entry whose expiresAt is in
// the past is reported absent by Get, yet is NOT reclaimed — Size still counts
// it until an overwrite (or the future janitor) removes it. We plant the expired
// entry directly (white-box) so the test is deterministic without sleeping.
func TestTTL_GetHidesExpired(t *testing.T) {
	m := New()
	m.data["k"] = entry{value: []byte("v"), expiresAt: time.Now().Add(-time.Hour)}

	if got, ok := m.Get("k"); ok {
		t.Fatalf(`Get expired = %q, %v; want _, false`, got, ok)
	}
	if m.Size() != 1 {
		t.Errorf("Size = %d, want 1 (lazy expiry does not reclaim)", m.Size())
	}

	// Overwriting with a plain Set revives the key with no expiry.
	if err := m.Set("k", []byte("fresh")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, ok := m.Get("k"); !ok || !bytes.Equal(got, []byte("fresh")) {
		t.Errorf(`Get after overwrite = %q, %v; want "fresh", true`, got, ok)
	}
}

// TestTTL_SetNeverExpires confirms plain Set stores an entry with no expiry (a
// zero expiresAt).
func TestTTL_SetNeverExpires(t *testing.T) {
	m := New()
	if err := m.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	e, ok := m.data["k"]
	if !ok {
		t.Fatal("entry not stored")
	}
	if !e.expiresAt.IsZero() {
		t.Errorf("Set stored expiresAt = %v, want zero (never expires)", e.expiresAt)
	}
}

// TestTTL_InvalidTTL checks that a non-positive TTL is rejected and stores
// nothing.
func TestTTL_InvalidTTL(t *testing.T) {
	m := New()
	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := m.SetWithTTL("k", []byte("v"), ttl); !errors.Is(err, ErrInvalidTTL) {
			t.Errorf("SetWithTTL(ttl=%v) error = %v, want ErrInvalidTTL", ttl, err)
		}
	}
	if m.Size() != 0 {
		t.Errorf("Size = %d, want 0 (invalid TTL must not store)", m.Size())
	}
}

// TestTTL_ShardedStore verifies TTL through the sharded wrapper: a live entry is
// returned, and an expired entry planted (white-box) into its owning shard is
// hidden by Get.
func TestTTL_ShardedStore(t *testing.T) {
	s := NewSharded(8)

	if err := s.SetWithTTL("live", []byte("v"), time.Minute); err != nil {
		t.Fatalf("SetWithTTL: %v", err)
	}
	if _, ok := s.Get("live"); !ok {
		t.Error("Get live: not found")
	}

	// Plant an already-expired entry directly into the shard that owns the key.
	sh := s.shardFor("dead")
	sh.store.data["dead"] = entry{value: []byte("v"), expiresAt: time.Now().Add(-time.Hour)}
	if _, ok := s.Get("dead"); ok {
		t.Error("Get expired: still present")
	}
}

// TestTTL_ShardedStoreConcurrent hammers the TTL write path from many goroutines
// so `go test -race` covers it. Run:
//
//	go test ./internal/store/ -run TTL -race
func TestTTL_ShardedStoreConcurrent(t *testing.T) {
	s := NewSharded(16)

	const (
		goroutines = 32
		opsPerG    = 1000
		keySpace   = 256
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				key := "key" + strconv.Itoa((g+i)%keySpace)
				if err := s.SetWithTTL(key, []byte("v"), time.Minute); err != nil {
					t.Errorf("SetWithTTL: %v", err)
					return
				}
				s.Get(key)
			}
		}(g)
	}
	wg.Wait()
}
