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
)

// fakeReplicator is a deterministic stand-in for *Peer: ops to any addr in fail
// return an error, everything else acks. It lets the quorum logic be tested without
// real sockets.
type fakeReplicator struct {
	mu         sync.Mutex
	fail       map[string]bool
	replicated map[string]int // Replicate calls seen, per addr
}

func newFakeReplicator() *fakeReplicator {
	return &fakeReplicator{fail: map[string]bool{}, replicated: map[string]int{}}
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
	return []byte("v"), true, nil
}

// newTestCoordinator builds a Coordinator over a fixed 3-node cluster (self "a",
// peers "b"/"c"). With n=3 and three nodes, Owners returns all three for every key,
// so the replica set — and thus the fan-out to {b, c} — is deterministic.
func newTestCoordinator(peer replicator, n, r, w int) *Coordinator {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{
		{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1},
		{ID: "c", Addr: "c", State: cluster.Alive, Incarnation: 1},
	}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)
	return NewCoordinator("a", router, peer, n, r, w)
}

func setCmd(key, value string) protocol.Command {
	return protocol.Command{Name: "SET", Args: [][]byte{[]byte(key), []byte(value)}}
}

func getCmd(key string) protocol.Command {
	return protocol.Command{Name: "GET", Args: [][]byte{[]byte(key)}}
}

func encodeReply(t *testing.T, v protocol.Value) string {
	t.Helper()
	var buf bytes.Buffer
	if err := v.Encode(&buf); err != nil {
		t.Fatalf("Encode reply: %v", err)
	}
	return buf.String()
}

// TestCoordinator_WriteQuorumMet: W=2 with one replica down — local + the healthy
// peer make quorum, so the local +OK passes through.
func TestCoordinator_WriteQuorumMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	c := newTestCoordinator(peer, 3, 2, 2)

	reply := c.coordinate(setCmd("k", "v"), protocol.SimpleString("OK"))
	if got := encodeReply(t, reply); got != "+OK\r\n" {
		t.Errorf("write with W=2 and one replica down = %q, want +OK", got)
	}
}

// TestCoordinator_WriteQuorumNotMet: W=3 needs every replica, but one is down, so the
// write fails with a RESP error rather than the local +OK.
func TestCoordinator_WriteQuorumNotMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	c := newTestCoordinator(peer, 3, 3, 3)

	reply := c.coordinate(setCmd("k", "v"), protocol.SimpleString("OK"))
	if got := encodeReply(t, reply); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("write with W=3 and one replica down = %q, want a -ERR", got)
	}
}

// TestCoordinator_LocalErrorNotReplicated: a write that failed locally is returned
// as-is and never fanned out.
func TestCoordinator_LocalErrorNotReplicated(t *testing.T) {
	peer := newFakeReplicator()
	c := newTestCoordinator(peer, 3, 2, 2)

	reply := c.coordinate(setCmd("k", "v"), protocol.Error("ERR value too large"))
	if got := encodeReply(t, reply); got != "-ERR value too large\r\n" {
		t.Errorf("coordinate of a failed local write = %q, want the local error unchanged", got)
	}
	if len(peer.replicated) != 0 {
		t.Errorf("a failed local write was replicated to %v, want no fan-out", peer.replicated)
	}
}

// TestCoordinator_ReadQuorumMet: R=2 with one replica down — local read plus the
// healthy peer meet the quorum, so the local value is returned.
func TestCoordinator_ReadQuorumMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	c := newTestCoordinator(peer, 3, 2, 2)

	reply := c.coordinate(getCmd("k"), protocol.BulkString([]byte("v")))
	if got := encodeReply(t, reply); got != "$1\r\nv\r\n" {
		t.Errorf("read with R=2 met = %q, want the local value", got)
	}
}

// TestCoordinator_ReadQuorumNotMet: R=2 but both peers are down, so only the local
// read responds and the read fails.
func TestCoordinator_ReadQuorumNotMet(t *testing.T) {
	peer := newFakeReplicator()
	peer.fail["b"] = true
	peer.fail["c"] = true
	c := newTestCoordinator(peer, 3, 2, 2)

	reply := c.coordinate(getCmd("k"), protocol.BulkString([]byte("v")))
	if got := encodeReply(t, reply); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("read with R=2 and both replicas down = %q, want a -ERR", got)
	}
}
