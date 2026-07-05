package store

import (
	"strconv"
	"testing"
	"time"
)

// TestReapExpired_MapStore checks the synchronous sweep: expired entries are
// deleted, live ones kept, and the returned count is right. Deterministic — no
// goroutine, no sleeps; `now` is passed in.
func TestReapExpired_MapStore(t *testing.T) {
	m := New()
	now := time.Now()

	// Three expired, two live (one never-expires, one future).
	m.data["e1"] = entry{value: []byte("v"), expiresAt: now.Add(-time.Hour)}
	m.data["e2"] = entry{value: []byte("v"), expiresAt: now.Add(-time.Minute)}
	m.data["e3"] = entry{value: []byte("v"), expiresAt: now.Add(-time.Nanosecond)}
	m.data["live1"] = entry{value: []byte("v")} // zero expiresAt: never expires
	m.data["live2"] = entry{value: []byte("v"), expiresAt: now.Add(time.Hour)}

	if got := m.reapExpired(now); got != 3 {
		t.Errorf("reapExpired removed %d, want 3", got)
	}
	if m.Size() != 2 {
		t.Errorf("Size after reap = %d, want 2", m.Size())
	}
	for _, k := range []string{"e1", "e2", "e3"} {
		if _, ok := m.data[k]; ok {
			t.Errorf("expired key %q survived reap", k)
		}
	}
	for _, k := range []string{"live1", "live2"} {
		if _, ok := m.data[k]; !ok {
			t.Errorf("live key %q was wrongly reaped", k)
		}
	}
}

// TestReapExpired_ShardedStore checks the sweep across all shards.
func TestReapExpired_ShardedStore(t *testing.T) {
	s := NewSharded(4)
	now := time.Now()

	const expiredCount = 20
	for i := 0; i < expiredCount; i++ {
		k := "exp" + strconv.Itoa(i)
		s.shardFor(k).store.data[k] = entry{value: []byte("v"), expiresAt: now.Add(-time.Hour)}
	}
	if err := s.Set("live", []byte("v")); err != nil {
		t.Fatal(err)
	}

	if got := s.reapExpired(now); got != expiredCount {
		t.Errorf("reapExpired removed %d, want %d", got, expiredCount)
	}
	if s.Size() != 1 {
		t.Errorf("Size after reap = %d, want 1", s.Size())
	}
	if _, ok := s.Get("live"); !ok {
		t.Error("live entry was reaped")
	}
}

// fakeReaper records each reapExpired call on a buffered channel, so a test can
// observe the janitor driving it without involving a real store.
type fakeReaper struct {
	calls chan time.Time
}

func (f *fakeReaper) reapExpired(now time.Time) int {
	select {
	case f.calls <- now:
	default: // never block the janitor if the test isn't draining
	}
	return 0
}

// TestJanitor_ReapsPeriodically verifies the goroutine actually fires reaps on
// its ticker. A short interval plus a generous timeout keeps it robust: we only
// assert that at least one reap is observed.
func TestJanitor_ReapsPeriodically(t *testing.T) {
	r := &fakeReaper{calls: make(chan time.Time, 8)}
	j := NewJanitor(r, time.Millisecond)
	defer j.Stop()

	select {
	case <-r.calls: // the janitor reaped at least once
	case <-time.After(2 * time.Second):
		t.Fatal("janitor did not reap within 2s")
	}
}

// TestJanitor_StopIsIdempotent ensures Stop can be called repeatedly without
// panicking (close-of-closed-channel) or hanging.
func TestJanitor_StopIsIdempotent(t *testing.T) {
	r := &fakeReaper{calls: make(chan time.Time, 8)}
	j := NewJanitor(r, time.Hour) // long interval; we're only testing lifecycle

	j.Stop()
	j.Stop() // must not panic or block forever
}

// TestJanitor_StopEndsReaping asserts no reaps happen after Stop returns — i.e.
// Stop truly waits for the goroutine to exit (no leak, no late reap).
func TestJanitor_StopEndsReaping(t *testing.T) {
	r := &fakeReaper{calls: make(chan time.Time, 128)}
	j := NewJanitor(r, time.Millisecond)

	time.Sleep(20 * time.Millisecond) // let a few reaps happen
	j.Stop()

	// After Stop the goroutine is gone, so draining is safe (no concurrent send).
	for len(r.calls) > 0 {
		<-r.calls
	}
	select {
	case <-time.After(50 * time.Millisecond):
		// nothing new after Stop — correct
	case <-r.calls:
		t.Fatal("janitor reaped after Stop returned")
	}
}

// TestJanitor_ReclaimsExpiredEntries is the end-to-end check: a real store with
// planted-expired entries, swept by a running janitor, drops to just the live
// entry. Polls Size with a timeout to stay robust against timing.
func TestJanitor_ReclaimsExpiredEntries(t *testing.T) {
	s := NewSharded(4)
	for i := 0; i < 10; i++ {
		k := "exp" + strconv.Itoa(i)
		s.shardFor(k).store.data[k] = entry{value: []byte("v"), expiresAt: time.Now().Add(-time.Hour)}
	}
	if err := s.Set("live", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if s.Size() != 11 {
		t.Fatalf("Size = %d, want 11 before reaping", s.Size())
	}

	j := NewJanitor(s, time.Millisecond)
	defer j.Stop()

	deadline := time.After(2 * time.Second)
	for s.Size() != 1 {
		select {
		case <-deadline:
			t.Fatalf("Size = %d, want 1 after reaping", s.Size())
		case <-time.After(time.Millisecond):
		}
	}
	if _, ok := s.Get("live"); !ok {
		t.Error("live entry was reaped")
	}
}
