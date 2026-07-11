package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
)

// TestPeer_Replication drives the peer client against a real server: Set/Get/Del
// over a socket, exercising the internal RSET/RGET/RDEL verbs end to end.
func TestPeer_Replication(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	// Set then Get round-trips the value.
	if err := peer.Set(addr, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, found, err := peer.Get(addr, "k"); err != nil || !found || string(got) != "v" {
		t.Fatalf("Get after Set = (%q, %v, %v), want (\"v\", true, nil)", got, found, err)
	}

	// A TTL'd Set (the PX path) is readable before it expires.
	if err := peer.Set(addr, "t", []byte("x"), time.Hour); err != nil {
		t.Fatalf("Set with ttl: %v", err)
	}
	if got, found, _ := peer.Get(addr, "t"); !found || string(got) != "x" {
		t.Errorf("Get of a TTL'd key = (%q, %v), want (\"x\", true)", got, found)
	}

	// Del removes it; a subsequent Get is a clean miss.
	if err := peer.Del(addr, "k"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, found, err := peer.Get(addr, "k"); err != nil || found {
		t.Errorf("Get after Del = (found %v, err %v), want (false, nil)", found, err)
	}
}

// TestPeer_ReusesConnection: a second call to the same peer reuses the pooled
// connection instead of dialing a new one.
func TestPeer_ReusesConnection(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	if err := peer.Set(addr, "k", []byte("v"), 0); err != nil {
		t.Fatalf("first Set: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after 1 call = %d, want 1 (conn returned to the pool)", got)
	}
	first := peer.idle[addr][0]

	if err := peer.Set(addr, "k2", []byte("v"), 0); err != nil {
		t.Fatalf("second Set: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after 2 calls = %d, want 1 (reused, not a fresh dial)", got)
	}
	if peer.idle[addr][0] != first {
		t.Error("second call dialed a new connection instead of reusing the pooled one")
	}
}

// TestPeer_DiscardsBrokenConnection: when an op fails on a pooled connection (here,
// the peer has gone away), that connection is closed and NOT returned to the pool.
func TestPeer_DiscardsBrokenConnection(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	// Prime the pool with one healthy connection.
	if err := peer.Set(addr, "k", []byte("v"), 0); err != nil {
		t.Fatalf("priming Set: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after priming = %d, want 1", got)
	}

	// Kill the server; the pooled connection is now dead.
	srv.Close()

	// The next op reuses the dead conn, fails, and must discard it — not repool it.
	if err := peer.Set(addr, "k", []byte("v"), 0); err == nil {
		t.Error("Set over a dead pooled conn = nil error, want a failure")
	}
	if got := len(peer.idle[addr]); got != 0 {
		t.Errorf("idle conns after a failed op = %d, want 0 (broken conn discarded)", got)
	}
}

// TestPeer_ConcurrentUse: many goroutines hammer the pool at once. Meaningful under
// -race — it proves the mutex-guarded pool is safe and that concurrent ops to one
// peer each get their own connection.
func TestPeer_ConcurrentUse(t *testing.T) {
	// A concurrency test needs a concurrency-safe store: startServer uses a bare
	// MapStore, which races under parallel writes. NewSharded (what production uses)
	// is lock-striped, so the store is no longer the thing under test — the pool is.
	srv := NewServer(store.NewSharded(8), nil, nil, nil)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })
	addr := srv.Addr().String()

	peer := NewPeer(2 * time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			for j := 0; j < 50; j++ {
				if err := peer.Set(addr, key, []byte("v"), 0); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				if got, found, err := peer.Get(addr, key); err != nil || !found || string(got) != "v" {
					t.Errorf("Get = (%q, %v, %v), want (v, true, nil)", got, found, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
