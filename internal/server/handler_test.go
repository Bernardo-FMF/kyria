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
	"github.com/Bernardo-FMF/kyria/internal/vclock"
	"github.com/Bernardo-FMF/kyria/internal/version"
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

// TestHandle_VersionedReplicaVerbs: the internal verbs bypass routing (a client GET
// of a non-owned key redirects, but RSET/RGET act locally) and store VERSIONED blobs
// — RSET reconciles an incoming version into the sibling set, RGET returns the encoded
// set, a higher-clock RSET supersedes the earlier version, and RDEL drops the key.
func TestHandle_VersionedReplicaVerbs(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1}}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)
	h := NewHandler(store.New(), m, router, nil)

	send := func(name string, args ...[]byte) protocol.Value {
		return h.Handle(protocol.Command{Name: name, Args: args})
	}
	wire := func(v protocol.Value) string {
		var buf bytes.Buffer
		if err := v.Encode(&buf); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return buf.String()
	}
	blobOf := func(value string, clock vclock.Clock) []byte {
		return version.Encode([]version.Version{{Value: []byte(value), Clock: clock}})
	}
	siblings := func(key string) []version.Version {
		blob, ok := send("RGET", []byte(key)).AsBulk()
		if !ok {
			t.Fatalf("RGET %q returned no blob", key)
		}
		vs, err := version.Decode(blob)
		if err != nil {
			t.Fatalf("Decode RGET blob: %v", err)
		}
		return vs
	}

	// A key this node does not own — a client GET of it redirects...
	var remoteKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if !router.IsLocal(k) {
			remoteKey = k
			break
		}
	}
	if got := wire(send("GET", []byte(remoteKey))); !strings.HasPrefix(got, "-MOVED") {
		t.Fatalf("client GET of a non-owned key = %q, want -MOVED (test premise)", got)
	}

	// ...but RSET of a versioned blob is stored locally, no redirect.
	if got := wire(send("RSET", []byte(remoteKey), blobOf("v1", vclock.Clock{"a": 1}))); got != "+OK\r\n" {
		t.Errorf("RSET v1 = %q, want +OK", got)
	}
	if vs := siblings(remoteKey); len(vs) != 1 || string(vs[0].Value) != "v1" {
		t.Errorf("after RSET v1, RGET = %v, want [v1]", vs)
	}

	// A higher clock supersedes — still one version, now v2.
	if got := wire(send("RSET", []byte(remoteKey), blobOf("v2", vclock.Clock{"a": 2}))); got != "+OK\r\n" {
		t.Errorf("RSET v2 = %q, want +OK", got)
	}
	if vs := siblings(remoteKey); len(vs) != 1 || string(vs[0].Value) != "v2" {
		t.Errorf("after superseding RSET, RGET = %v, want just [v2]", vs)
	}

	// RDEL drops the key entirely.
	if got := wire(send("RDEL", []byte(remoteKey))); got != ":1\r\n" {
		t.Errorf("RDEL = %q, want :1", got)
	}
	if got := wire(send("RGET", []byte(remoteKey))); got != "$-1\r\n" {
		t.Errorf("RGET after RDEL = %q, want a null bulk", got)
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
	st := store.New()
	coord := NewCoordinator("a", router, st, peer, 3, 2, 3)
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
	// Applied locally as a versioned blob.
	blob, ok := st.Get(localKey)
	if !ok {
		t.Fatal("local store missing the key after SET")
	}
	if vs, err := version.Decode(blob); err != nil || len(vs) != 1 || string(vs[0].Value) != "v" {
		t.Errorf("local version after SET = %v (err %v), want a single version [v]", vs, err)
	}
	// Fanned out to both peers.
	if peer.replicated["b"] != 1 || peer.replicated["c"] != 1 {
		t.Errorf("replicated = b:%d c:%d, want 1 each (fanned out to both peers)", peer.replicated["b"], peer.replicated["c"])
	}
}
