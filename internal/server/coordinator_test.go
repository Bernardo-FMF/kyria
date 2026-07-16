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
	return NewCoordinator("a", router, s, peer, NewHintStore(), n, r, w)
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

// TestCoordinator_WriteParksHintForDownReplica: a write that meets quorum with one
// replica down still returns +OK, and the unreachable replica gets a parked hint so the
// write isn't lost — while the replica that acked gets none. Parking happens inside a
// fan-out goroutine (async vs the reply), so the hint is observed by polling.
func TestCoordinator_WriteParksHintForDownReplica(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true // b is down; self + c still make W=2
	c := newTestCoordinator(store.New(), peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(setCmd("k", "v"))); got != "+OK\r\n" {
		t.Fatalf("write with one replica down = %q, want +OK", got)
	}

	// The failed replica's hint is parked asynchronously — poll until it lands.
	deadline := time.Now().Add(time.Second)
	for {
		snap := c.hints.snapshot()
		if blob, ok := snap["b"]["k"]; ok {
			if vs, _ := version.Decode(blob); len(vs) != 1 || string(vs[0].Value) != "v" {
				t.Errorf("parked hint for b = %v, want a single version [v]", vs)
			}
			if _, hinted := snap["c"]; hinted {
				t.Error("replica c acked but was still given a hint")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no hint parked for the down replica b: %v", snap)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCoordinator_DeleteQuorumMet: DEL of a live key with W met replies :1 and, crucially, does
// NOT hard-remove the key — it writes a tombstone in its place. The local blob afterwards is a
// single "gone" version whose clock ({a:2}) supersedes the value it buried ({a:1}), with nothing
// left Live. That surviving tombstone is what stops a replica which missed the delete from
// resurrecting the value via read-repair / anti-entropy.
func TestCoordinator_DeleteQuorumMet(t *testing.T) {
	peer := newFakeReplicator()
	s := store.New()
	s.Set("k", verBlob("v", vclock.Clock{"a": 1})) // a live versioned value, as write() would store it
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(delCmd("k"))); got != ":1\r\n" {
		t.Errorf("DEL of a live key = %q, want :1", got)
	}

	// The key is still present locally — but as a tombstone, not a value.
	blob, ok := s.Get("k")
	if !ok {
		t.Fatal("key was hard-removed after DEL; a tombstone should remain to bury the value")
	}
	vs, err := version.Decode(blob)
	if err != nil || len(vs) != 1 {
		t.Fatalf("local set after DEL = %v (err %v), want a single tombstone", vs, err)
	}
	if !vs[0].Deleted {
		t.Errorf("local version after DEL = %+v, want Deleted", vs[0])
	}
	if live := version.Live(vs); len(live) != 0 {
		t.Errorf("Live after DEL = %v, want none (the value is buried)", live)
	}
	// The tombstone must causally supersede the value it replaced ({a:1} → {a:2}).
	if vs[0].Clock.Compare(vclock.Clock{"a": 1}) != vclock.After {
		t.Errorf("tombstone clock = %v, want it to supersede the value's {a:1}", vs[0].Clock)
	}
}

// TestCoordinator_DeleteAbsentKey: DEL of a key with nothing live replies :0 — but still lays a
// tombstone locally. A delete records the "gone" marker even when this node never held the value,
// so the marker can bury a copy that lives on another replica or arrives later.
func TestCoordinator_DeleteAbsentKey(t *testing.T) {
	peer := newFakeReplicator()
	s := store.New()
	c := newTestCoordinator(s, peer, 3, 2, 2)

	if got := encodeReply(t, c.coordinate(delCmd("missing"))); got != ":0\r\n" {
		t.Errorf("DEL of an absent key = %q, want :0", got)
	}
	blob, ok := s.Get("missing")
	if !ok {
		t.Fatal("no tombstone laid for an absent key; a delete must still record the marker")
	}
	if vs, err := version.Decode(blob); err != nil || len(vs) != 1 || !vs[0].Deleted {
		t.Errorf("local set after DEL of absent key = %v (err %v), want a single tombstone", vs, err)
	}
}

// TestCoordinator_DeleteQuorumNotMet: W=3 needs every replica, but one is down, so the delete can't
// reach write quorum and returns a -ERR — not a silent :0. Same durability contract as a write.
func TestCoordinator_DeleteQuorumNotMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	s := store.New()
	s.Set("k", verBlob("v", vclock.Clock{"a": 1}))
	c := newTestCoordinator(s, peer, 3, 3, 3)

	if got := encodeReply(t, c.coordinate(delCmd("k"))); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("DEL with W=3 and one replica down = %q, want a -ERR", got)
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

// TestCoordinator_ReadTriggersReadRepair: a real read (not a direct readRepair call)
// must kick off the async repair. Self holds a stale version, peer b holds the newer
// one, peer c responds with an empty set (a miss). With R=3 the read waits for all
// three, reconciles to "new", and then — off the read path — heals the laggards: self
// via its local store, the stale peer c via an RSET, while the current peer b is skipped.
func TestCoordinator_ReadTriggersReadRepair(t *testing.T) {
	peer := newFakeReplicator()
	peer.blobs["b"] = verBlob("new", vclock.Clock{"a": 2}) // b already current
	// c is absent from blobs → Get returns an empty set: a responder that's behind.
	// A ShardedStore (not a bare MapStore) because the async readRepair writes the
	// store while this test's polling loop reads it — per-shard locks make that safe.
	s := store.NewSharded(4)
	s.Set("k", verBlob("old", vclock.Clock{"a": 1})) // self is stale
	c := newTestCoordinator(s, peer, 3, 3, 2)        // R=3 → the read waits for all three

	if got := encodeReply(t, c.coordinate(getCmd("k"))); got != "$3\r\nnew\r\n" {
		t.Fatalf("read = %q, want the reconciled $3\\r\\nnew", got)
	}

	// Read-repair runs in a goroutine, so poll (with a timeout) until it converges:
	// the stale peer c pushed an RSET and self's local store healed to "new".
	deadline := time.Now().Add(time.Second)
	for {
		peer.mu.Lock()
		pushedB, pushedC := peer.replicated["b"], peer.replicated["c"]
		peer.mu.Unlock()

		blob, _ := s.Get("k")
		vs, _ := version.Decode(blob)
		selfHealed := len(vs) == 1 && string(vs[0].Value) == "new"

		if pushedC > 0 && selfHealed {
			if pushedB != 0 {
				t.Errorf("up-to-date peer b was repaired %d times, want 0", pushedB)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("read-repair never converged: c pushed=%d, self healed=%v", pushedC, selfHealed)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCoordinator_ReadRepair: readRepair pushes the reconciled set back to replicas
// that were behind — the local store gets updated, a stale peer gets an RSET — and it
// skips a replica that already holds the result. (Called synchronously here; in the
// read path it runs in a goroutine.)
func TestCoordinator_ReadRepair(t *testing.T) {
	peer := newFakeReplicator()
	s := store.New()
	s.Set("k", verBlob("old", vclock.Clock{"a": 1})) // self locally holds the stale version
	c := newTestCoordinator(s, peer, 3, 2, 2)

	merged := []version.Version{{Value: []byte("new"), Clock: vclock.Clock{"a": 2}}}
	responders := map[string][]version.Version{
		"a": {{Value: []byte("old"), Clock: vclock.Clock{"a": 1}}}, // self, stale
		"b": {{Value: []byte("new"), Clock: vclock.Clock{"a": 2}}}, // already current → skip
		"c": nil,                                                   // never had it → stale
	}

	c.readRepair("k", merged, responders)

	// self (a) was stale → its local store now holds the reconciled version.
	blob, ok := s.Get("k")
	if !ok {
		t.Fatal("self was not repaired")
	}
	if vs, _ := version.Decode(blob); len(vs) != 1 || string(vs[0].Value) != "new" {
		t.Errorf("self after repair = %v, want the reconciled [new]", vs)
	}
	// b was current → not pushed; c was stale → pushed.
	if peer.replicated["b"] != 0 {
		t.Errorf("up-to-date peer b was repaired %d times, want 0", peer.replicated["b"])
	}
	if peer.replicated["c"] == 0 {
		t.Error("stale peer c was not repaired")
	}
}
