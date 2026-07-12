package server

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// fakeReplicator is a deterministic stand-in for *Peer: ops to any addr in fail
// return an error, everything else acks. It lets the quorum logic be tested without
// real sockets.
type fakeReplicator struct {
	mu         sync.Mutex
	fail       map[string]bool
	replicated map[string]int    // Replicate calls seen, per addr
	blobs      map[string][]byte // what Get returns per addr (absent = a miss)
}

func newFakeReplicator() *fakeReplicator {
	return &fakeReplicator{
		fail:       map[string]bool{},
		replicated: map[string]int{},
		blobs:      map[string][]byte{},
	}
}

func (f *fakeReplicator) Replicate(addr, verb string, args [][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replicated[addr]++
	if f.fail[addr] {
		return errors.New("replica down")
	}
	return nil
}

func (f *fakeReplicator) Get(addr, key string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[addr] {
		return nil, false, errors.New("replica down")
	}
	blob, ok := f.blobs[addr]
	return blob, ok, nil // absent addr → a miss (nil, false, nil)
}

// newTestCoordinator builds a Coordinator over store s and a fixed 3-node cluster
// (self "a", peers "b"/"c"). With n=3 and three nodes, Owners returns all three, so
// the fan-out to {b, c} is deterministic.
func newTestCoordinator(s store.Store, peer replicator, n, r, w int) *Coordinator {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{
		{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1},
		{ID: "c", Addr: "c", State: cluster.Alive, Incarnation: 1},
	}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)
	return NewCoordinator("a", router, s, peer, n, r, w)
}

func setCmd(key, value string) protocol.Command {
	return protocol.Command{Name: "SET", Args: [][]byte{[]byte(key), []byte(value)}}
}

func delCmd(key string) protocol.Command {
	return protocol.Command{Name: "DEL", Args: [][]byte{[]byte(key)}}
}

func getCmd(key string) protocol.Command {
	return protocol.Command{Name: "GET", Args: [][]byte{[]byte(key)}}
}

// verBlob encodes a single versioned value — the on-store form a replica holds.
func verBlob(value string, clock vclock.Clock) []byte {
	return version.Encode([]version.Version{{Value: []byte(value), Clock: clock}})
}

func encodeReply(t *testing.T, v protocol.Value) string {
	t.Helper()
	var buf bytes.Buffer
	if err := v.Encode(&buf); err != nil {
		t.Fatalf("Encode reply: %v", err)
	}
	return buf.String()
}

// TestCoordinator_WriteQuorumMet: W=2 with one replica down — local + the healthy peer
// make quorum → +OK, and the value lands in the local store as a versioned blob.
func TestCoordinator_WriteQuorumMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	s := store.New()
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(setCmd("k", "v"))); got != "+OK\r\n" {
		t.Errorf("write with W=2 and one replica down = %q, want +OK", got)
	}
	blob, ok := s.Get("k")
	if !ok {
		t.Fatal("local store is missing the key after a write")
	}
	if vs, err := version.Decode(blob); err != nil || len(vs) != 1 || string(vs[0].Value) != "v" {
		t.Errorf("local version = %v (err %v), want a single version [v]", vs, err)
	}
}

// TestCoordinator_WriteQuorumNotMet: W=3 needs every replica, but one is down → -ERR.
func TestCoordinator_WriteQuorumNotMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	c := newTestCoordinator(store.New(), peer, 3, 3, 3)

	if got := encodeReply(t, c.coordinate(setCmd("k", "v"))); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("write with W=3 and one replica down = %q, want a -ERR", got)
	}
}

// TestCoordinator_WriteFailsLocallyNotReplicated: when the local store rejects the
// write (an oversized value here — the encoded blob exceeds the tiny size limit), the
// coordinator returns the error and never fans out.
func TestCoordinator_WriteFailsLocallyNotReplicated(t *testing.T) {
	peer := newFakeReplicator()
	s := store.NewSharded(1, store.WithMaxValueSize(4)) // any versioned blob is bigger than 4 bytes
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(setCmd("k", "v"))); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("write rejected locally = %q, want a -ERR", got)
	}
	if len(peer.replicated) != 0 {
		t.Errorf("a write that failed locally was replicated to %v, want no fan-out", peer.replicated)
	}
}

// TestCoordinator_DeleteQuorumMet: DEL removes the key locally and, with W met, replies
// :1. (Delete removes the whole key regardless of its versioned content.)
func TestCoordinator_DeleteQuorumMet(t *testing.T) {
	peer := newFakeReplicator()
	s := store.New()
	s.Set("k", []byte("anything")) // delete doesn't inspect the content
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(delCmd("k"))); got != ":1\r\n" {
		t.Errorf("DEL of an existing key = %q, want :1", got)
	}
	if _, ok := s.Get("k"); ok {
		t.Error("key still present locally after DEL")
	}
}

// TestCoordinator_ReadQuorumMet: R=2 — the local read plus one peer meet the quorum,
// and the read reconciles across replicas, returning the NEWER version. Here the local
// copy is stale ({a:1}) and peer b holds the newer one ({a:2}), so the read returns b's.
func TestCoordinator_ReadQuorumMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["c"] = true                                  // one peer down
	peer.blobs["b"] = verBlob("new", vclock.Clock{"a": 2}) // the healthy peer has the newer version
	s := store.New()
	s.Set("k", verBlob("old", vclock.Clock{"a": 1})) // local holds the older version
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(getCmd("k"))); got != "$3\r\nnew\r\n" {
		t.Errorf("read = %q, want the reconciled newest value $3\\r\\nnew", got)
	}
}

// TestCoordinator_ReadQuorumNotMet: R=2 but both peers are down, so only the local
// read responds and the read fails.
func TestCoordinator_ReadQuorumNotMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	peer.fail["c"] = true
	s := store.New()
	s.Set("k", verBlob("v", vclock.Clock{"a": 1}))
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(getCmd("k"))); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("read with R=2 and both peers down = %q, want a -ERR", got)
	}
}

// TestCoordinator_ReadMiss: an absent key with R=1 takes the fast path (no peers) and
// returns a null bulk.
func TestCoordinator_ReadMiss(t *testing.T) {
	peer := newFakeReplicator()
	c := newTestCoordinator(store.New(), peer, 3, 1, 2)

	if got := encodeReply(t, c.coordinate(getCmd("missing"))); got != "$-1\r\n" {
		t.Errorf("read of an absent key = %q, want a null bulk $-1", got)
	}
}
