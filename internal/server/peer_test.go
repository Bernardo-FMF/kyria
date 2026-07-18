package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// versionBlob encodes value as a single-version RSET payload — what a coordinator
// sends after minting a clock.
func versionBlob(value string) []byte {
	return version.Encode([]version.Version{{Value: []byte(value), Clock: vclock.Clock{"n1": 1}}})
}

// rset replicates a versioned write of value under key to the peer at addr.
func (p *Peer) rsetTest(addr, key, value string) error {
	return p.Replicate(addr, rset, [][]byte{[]byte(key), versionBlob(value)})
}

// TestPeer_Replication drives the peer client against a real server: a versioned
// RSET and an RGET that returns the stored blob — over a socket, end to end.
func TestPeer_Replication(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	if err := peer.rsetTest(addr, "k", "v"); err != nil {
		t.Fatalf("RSET: %v", err)
	}
	// Get returns the stored sibling blob; decode it back to the value.
	got, found, err := peer.Get(addr, "k")
	if err != nil || !found {
		t.Fatalf("Get after RSET = (found %v, err %v), want found", found, err)
	}
	if vs, err := version.Decode(got); err != nil || len(vs) != 1 || string(vs[0].Value) != "v" {
		t.Fatalf("decoded RGET = %v (err %v), want [v]", vs, err)
	}
}

// TestPeer_ReusesConnection: a second call to the same peer reuses the pooled
// connection instead of dialing a new one.
func TestPeer_ReusesConnection(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	if err := peer.rsetTest(addr, "k", "v"); err != nil {
		t.Fatalf("first RSET: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after 1 call = %d, want 1 (conn returned to the pool)", got)
	}
	first := peer.idle[addr][0]

	if err := peer.rsetTest(addr, "k2", "v"); err != nil {
		t.Fatalf("second RSET: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after 2 calls = %d, want 1 (reused, not a fresh dial)", got)
	}
	if peer.idle[addr][0] != first {
		t.Error("second call dialed a new connection instead of reusing the pooled one")
	}
}

// TestPeer_DiscardsBrokenConnection: when an op fails on a pooled connection (here, the peer has
// gone away), that connection is closed and NOT returned to the pool. With retry-once, do() then
// dials a fresh conn too — which also fails since the peer is down — so the op still errors and the
// pool ends empty. (The peer-alive case, where the retry succeeds, is TestPeer_RetriesStalePooledConn.)
func TestPeer_DiscardsBrokenConnection(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	// Prime the pool with one healthy connection.
	if err := peer.rsetTest(addr, "k", "v"); err != nil {
		t.Fatalf("priming RSET: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after priming = %d, want 1", got)
	}

	// Kill the server; the pooled connection is now dead and a fresh dial will fail too.
	srv.Close()

	// The next op reuses the dead conn (fails), retries onto a fresh dial (also fails), and must
	// leave nothing in the pool.
	if err := peer.rsetTest(addr, "k", "v"); err == nil {
		t.Error("RSET over a dead pooled conn = nil error, want a failure")
	}
	if got := len(peer.idle[addr]); got != 0 {
		t.Errorf("idle conns after a failed op = %d, want 0 (broken conn discarded)", got)
	}
}

// TestPeer_RetriesStalePooledConn: a pooled connection the peer closed while idle (restart, idle
// timeout) fails on reuse, so do() discards it and retries once on a fresh dial — with the server
// still up, the op succeeds, and the fresh connection lands back in the pool.
func TestPeer_RetriesStalePooledConn(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	// Prime the pool with one healthy connection, then close its socket to simulate the peer
	// dropping the idle conn while the server itself stays up.
	if err := peer.rsetTest(addr, "k", "v"); err != nil {
		t.Fatalf("priming RSET: %v", err)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Fatalf("idle conns after priming = %d, want 1", got)
	}
	peer.idle[addr][0].Close()

	// The next op reuses the dead pooled conn (fails), then retry-once dials fresh and succeeds.
	got, found, err := peer.Get(addr, "k")
	if err != nil {
		t.Fatalf("Get after a stale pooled conn = %v, want success via retry", err)
	}
	if !found {
		t.Error("Get found = false, want true")
	}
	if vs, derr := version.Decode(got); derr != nil || len(vs) != 1 || string(vs[0].Value) != "v" {
		t.Fatalf("Get value = %v (err %v), want [v]", vs, derr)
	}
	if got := len(peer.idle[addr]); got != 1 {
		t.Errorf("idle conns after retry = %d, want 1 (fresh conn repooled)", got)
	}
}

// TestPeer_ConcurrentUse: many goroutines hammer the pool at once. Meaningful under
// -race — it proves the mutex-guarded pool is safe and that concurrent ops to one
// peer each get their own connection.
func TestPeer_ConcurrentUse(t *testing.T) {
	// A concurrency test needs a concurrency-safe store: startServer uses a bare
	// MapStore, which races under parallel writes. NewSharded (what production uses)
	// is lock-striped, so the store is no longer the thing under test — the pool is.
	srv := NewServer(store.NewSharded(8), nil, nil, nil, ServerOptions{})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })
	addr := srv.Addr().String()

	peer := NewPeer(2 * time.Second)

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", i)
			for j := 0; j < 50; j++ {
				if err := peer.rsetTest(addr, key, "v"); err != nil {
					t.Errorf("RSET: %v", err)
					return
				}
				if _, found, err := peer.Get(addr, key); err != nil || !found {
					t.Errorf("Get = (found %v, err %v), want found", found, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
