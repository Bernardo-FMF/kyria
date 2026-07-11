package server

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// reply dispatches a command against s and returns the RESP-encoded reply as a
// string. Tests assert on the wire bytes because protocol.Value's fields are
// unexported — the encoded output is the observable behavior anyway.
func reply(t *testing.T, s store.Store, name string, args ...string) string {
	t.Helper()
	byteArgs := make([][]byte, len(args))
	for i, a := range args {
		byteArgs[i] = []byte(a)
	}

	v := NewHandler(s, nil, nil, nil).Handle(protocol.Command{Name: name, Args: byteArgs})

	var buf bytes.Buffer
	if err := v.Encode(&buf); err != nil {
		t.Fatalf("Encode reply: %v", err)
	}
	return buf.String()
}

func TestHandle_Ping(t *testing.T) {
	if got := reply(t, store.New(), "PING"); got != "+PONG\r\n" {
		t.Errorf("PING = %q, want %q", got, "+PONG\r\n")
	}
}

func TestHandle_Get(t *testing.T) {
	s := store.New()
	if _, err := s.Set("foo", []byte("bar")); err != nil {
		t.Fatal(err)
	}

	if got := reply(t, s, "GET", "foo"); got != "$3\r\nbar\r\n" {
		t.Errorf("GET foo = %q, want %q", got, "$3\r\nbar\r\n")
	}
	if got := reply(t, s, "GET", "missing"); got != "$-1\r\n" {
		t.Errorf("GET missing = %q, want %q (null bulk)", got, "$-1\r\n")
	}
}

func TestHandle_Set(t *testing.T) {
	s := store.New()

	if got := reply(t, s, "SET", "k", "v"); got != "+OK\r\n" {
		t.Errorf("SET = %q, want %q", got, "+OK\r\n")
	}
	// The reply is only meaningful if the value actually landed in the store.
	if got, ok := s.Get("k"); !ok || string(got) != "v" {
		t.Errorf("after SET, Get = %q, %v; want \"v\", true", got, ok)
	}
}

func TestHandle_Del(t *testing.T) {
	s := store.New()
	if _, err := s.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	if got := reply(t, s, "DEL", "k"); got != ":1\r\n" {
		t.Errorf("DEL existing = %q, want %q", got, ":1\r\n")
	}
	if got := reply(t, s, "DEL", "k"); got != ":0\r\n" {
		t.Errorf("DEL missing = %q, want %q", got, ":0\r\n")
	}
}

func TestHandle_CaseInsensitive(t *testing.T) {
	if got := reply(t, store.New(), "ping"); got != "+PONG\r\n" {
		t.Errorf("lowercase ping = %q, want +PONG", got)
	}
}

func TestHandle_Errors(t *testing.T) {
	tests := []struct {
		name string
		cmd  string
		args []string
	}{
		{"unknown command", "BOGUS", nil},
		{"GET wrong arity", "GET", nil},
		{"GET too many args", "GET", []string{"a", "b"}},
		{"SET wrong arity", "SET", []string{"k"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := reply(t, store.New(), tc.cmd, tc.args...); !strings.HasPrefix(got, "-ERR") {
				t.Errorf("reply = %q, want a -ERR error", got)
			}
		})
	}
}

// TestHandle_SetWithTTL: SET key value <EX seconds | PX milliseconds> stores the
// value with an expiry and replies +OK. The option keyword is case-insensitive.
func TestHandle_SetWithTTL(t *testing.T) {
	s := store.New()

	if got := reply(t, s, "SET", "k", "v", "PX", "10000"); got != "+OK\r\n" {
		t.Errorf("SET k v PX 10000 = %q, want %q", got, "+OK\r\n")
	}
	if got, ok := s.Get("k"); !ok || string(got) != "v" {
		t.Errorf("after SET PX, Get = %q, %v; want \"v\", true", got, ok)
	}

	if got := reply(t, s, "SET", "k2", "v2", "EX", "100"); got != "+OK\r\n" {
		t.Errorf("SET k2 v2 EX 100 = %q, want %q", got, "+OK\r\n")
	}
	if got, ok := s.Get("k2"); !ok || string(got) != "v2" {
		t.Errorf("after SET EX, Get = %q, %v; want \"v2\", true", got, ok)
	}

	if got := reply(t, s, "SET", "k3", "v3", "px", "5000"); got != "+OK\r\n" {
		t.Errorf("SET k3 v3 px 5000 (lowercase option) = %q, want %q", got, "+OK\r\n")
	}
}

// TestHandle_SetWithTTL_Errors: malformed TTL options are rejected with -ERR,
// and the argument count is bounded (SET takes 2 or 4 args).
func TestHandle_SetWithTTL_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"non-integer ttl", []string{"k", "v", "PX", "abc"}},
		{"unknown option", []string{"k", "v", "XY", "100"}},
		{"zero ttl", []string{"k", "v", "PX", "0"}},
		{"negative ttl", []string{"k", "v", "EX", "-5"}},
		{"missing ttl value", []string{"k", "v", "PX"}},
		{"too many args", []string{"k", "v", "PX", "100", "extra"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := reply(t, store.New(), "SET", tc.args...); !strings.HasPrefix(got, "-ERR") {
				t.Errorf("SET %v = %q, want a -ERR error", tc.args, got)
			}
		})
	}
}

// TestHandle_Nodes: with a cluster, NODES replies with one entry per alive member
// (order is unspecified, so we assert on membership, not exact bytes).
func TestHandle_Nodes(t *testing.T) {
	members := cluster.NewMembers(cluster.Node{ID: "self", Addr: "127.0.0.1:7001", State: cluster.Alive, Incarnation: 1})
	members.Merge([]cluster.Node{
		{ID: "peer", Addr: "127.0.0.1:7002", State: cluster.Alive, Incarnation: 1},
	}, time.Now())

	var buf bytes.Buffer
	v := NewHandler(store.New(), members, nil, nil).Handle(protocol.Command{Name: "NODES"})
	if err := v.Encode(&buf); err != nil {
		t.Fatalf("Encode reply: %v", err)
	}
	got := buf.String()

	if !strings.HasPrefix(got, "*2\r\n") {
		t.Errorf("NODES reply = %q, want an array of 2 members", got)
	}
	for _, want := range []string{"self", "127.0.0.1:7001", "peer", "127.0.0.1:7002"} {
		if !strings.Contains(got, want) {
			t.Errorf("NODES reply %q is missing %q", got, want)
		}
	}
}

// TestHandle_Nodes_Disabled: without a cluster (standalone node), NODES is an error.
func TestHandle_Nodes_Disabled(t *testing.T) {
	if got := reply(t, store.New(), "NODES"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("NODES with no cluster = %q, want a -ERR error", got)
	}
}

// TestHandle_Redirect: in a cluster, a keyed command for a key this node does not
// own is answered with a -MOVED redirect to the owner; a key this node does own is
// served locally. The node ID is the owner's client (TCP) address, so the ID that
// the ring returns is exactly what goes into -MOVED.
func TestHandle_Redirect(t *testing.T) {
	// Two-node cluster; this node is "a", peer is "b".
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1}}, time.Now())
	// A long interval + no Start: NewRouter builds the ring once and we never rebuild.
	router := cluster.NewRouter(m, 50, time.Hour)

	h := NewHandler(store.New(), m, router, nil)

	send := func(key string) string {
		var buf bytes.Buffer
		v := h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte(key)}})
		if err := v.Encode(&buf); err != nil {
			t.Fatalf("Encode reply: %v", err)
		}
		return buf.String()
	}

	// firstKey returns the first key-N whose ownership matches want-local.
	firstKey := func(local bool) string {
		for i := 0; ; i++ {
			k := fmt.Sprintf("key-%d", i)
			if router.IsLocal(k) == local {
				return k
			}
		}
	}

	// A key owned by the peer → -MOVED b (redirected, not served here).
	if got := send(firstKey(false)); !strings.HasPrefix(got, "-MOVED ") {
		t.Errorf("GET of a non-owned key = %q, want a -MOVED redirect", got)
	}

	// A key owned by this node → served locally (a miss, so a null bulk — not a redirect).
	if got := send(firstKey(true)); got != "$-1\r\n" {
		t.Errorf("GET of an owned (absent) key = %q, want $-1 served locally", got)
	}
}

// TestHandle_InternalOps: the internal replica verbs (RSET/RGET/RDEL) act on the
// LOCAL store and bypass routing — a coordinator uses them to place a copy on a
// node that isn't the key's owner. A client GET of the same key redirects, but RSET
// stores it right here.
func TestHandle_InternalOps(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1}}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)
	h := NewHandler(store.New(), m, router, nil)

	send := func(name string, args ...string) string {
		byteArgs := make([][]byte, len(args))
		for i, a := range args {
			byteArgs[i] = []byte(a)
		}
		var buf bytes.Buffer
		if err := h.Handle(protocol.Command{Name: name, Args: byteArgs}).Encode(&buf); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return buf.String()
	}

	// A key this node does not own — the premise: a client op here would redirect.
	var remoteKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if !router.IsLocal(k) {
			remoteKey = k
			break
		}
	}
	if got := send("GET", remoteKey); !strings.HasPrefix(got, "-MOVED") {
		t.Fatalf("client GET of a non-owned key = %q, want -MOVED (test premise)", got)
	}

	// The internal verbs act locally regardless of ownership.
	if got := send("RSET", remoteKey, "val"); got != "+OK\r\n" {
		t.Errorf("RSET of a non-owned key = %q, want +OK (stored locally, no redirect)", got)
	}
	if got := send("RGET", remoteKey); got != "$3\r\nval\r\n" {
		t.Errorf("RGET after RSET = %q, want the stored value $3\\r\\nval", got)
	}
	if got := send("RDEL", remoteKey); got != ":1\r\n" {
		t.Errorf("RDEL of the stored key = %q, want :1", got)
	}
	if got := send("RGET", remoteKey); got != "$-1\r\n" {
		t.Errorf("RGET after RDEL = %q, want a null bulk $-1", got)
	}
}

// TestHandle_CoordinatesLocalWrite: when this node owns the key and a coordinator is
// present, Handle applies the write locally AND drives the quorum — the value lands
// in the local store and is fanned out to the peers. (W=3 makes gather wait for both
// peers, so the replication counts are race-free to assert.)
func TestHandle_CoordinatesLocalWrite(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{
		{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1},
		{ID: "c", Addr: "c", State: cluster.Alive, Incarnation: 1},
	}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)

	peer := newFakeReplicator() // every replica acks
	coord := NewCoordinator("a", router, peer, 3, 2, 3)
	st := store.New()
	h := NewHandler(st, m, router, coord)

	// A key this node owns, so Handle coordinates instead of redirecting.
	var localKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if router.IsLocal(k) {
			localKey = k
			break
		}
	}

	var buf bytes.Buffer
	if err := h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte(localKey), []byte("v")}}).Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := buf.String(); got != "+OK\r\n" {
		t.Errorf("SET of an owned key = %q, want +OK (quorum met)", got)
	}
	// Applied locally.
	if v, ok := st.Get(localKey); !ok || string(v) != "v" {
		t.Errorf("local store after SET = (%q, %v), want (\"v\", true)", v, ok)
	}
	// Fanned out to both peers.
	if peer.replicated["b"] != 1 || peer.replicated["c"] != 1 {
		t.Errorf("replicated = b:%d c:%d, want 1 each (fanned out to both peers)", peer.replicated["b"], peer.replicated["c"])
	}
}
