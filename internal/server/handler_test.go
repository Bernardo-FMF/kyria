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
	"github.com/Bernardo-FMF/kyria/internal/telemetry"
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

	v := NewHandler(s, nil, nil, nil, nil).Handle(protocol.Command{Name: name, Args: byteArgs})

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
	v := NewHandler(store.New(), members, nil, nil, nil).Handle(protocol.Command{Name: "NODES"})
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

	h := NewHandler(store.New(), m, router, nil, nil)

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
// set, a higher-clock RSET supersedes the earlier version, and a tombstone RSET buries
// it (the replica keeps the tombstone rather than dropping the key).
func TestHandle_VersionedReplicaVerbs(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1}}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)
	h := NewHandler(store.New(), m, router, nil, nil)

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

	// A delete arrives as an RSET of a tombstone with a superseding clock. The replica STORES
	// it (it does not drop the key), so the marker buries v2 and survives to re-bury any copy
	// that resurfaces — RGET still returns a blob, now a single Deleted version.
	tomb := version.Encode([]version.Version{version.Tombstone(vclock.Clock{"a": 3}, 0)})
	if got := wire(send("RSET", []byte(remoteKey), tomb)); got != "+OK\r\n" {
		t.Errorf("RSET tombstone = %q, want +OK", got)
	}
	if vs := siblings(remoteKey); len(vs) != 1 || !vs[0].Deleted {
		t.Errorf("after RSET tombstone, RGET = %v, want a single tombstone (Deleted)", vs)
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
	coord := NewCoordinator("a", router, st, peer, NewHintStore(), CoordinatorOptions{N: 3, R: 2, W: 3})
	h := NewHandler(st, m, router, coord, nil)

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

// commandSnap returns the full telemetry entry for command, failing the test if there is none.
func commandSnap(t *testing.T, s telemetry.Snapshot, command string) telemetry.CommandSnapshot {
	t.Helper()
	for _, c := range s.Commands {
		if c.Command == command {
			return c
		}
	}
	t.Fatalf("no telemetry entry for %q (have %+v)", command, s.Commands)
	return telemetry.CommandSnapshot{}
}

// commandTotal returns just the recorded total for command.
func commandTotal(t *testing.T, s telemetry.Snapshot, command string) int64 {
	t.Helper()
	return commandSnap(t, s, command).Total
}

// TestHandle_TelemetryCountsCommands: Handle records client GET/SET/DEL against their own counters,
// and an internal RGET is not counted — it is never registered, so telemetry ignores it.
func TestHandle_TelemetryCountsCommands(t *testing.T) {
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(store.New(), nil, nil, nil, tel)

	h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte("k"), []byte("v")}})
	h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte("k2"), []byte("v")}})
	h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte("k")}})
	h.Handle(protocol.Command{Name: "DEL", Args: [][]byte{[]byte("k")}})
	h.Handle(protocol.Command{Name: "RGET", Args: [][]byte{[]byte("k2")}}) // internal verb — must not count

	s := tel.Snapshot()
	if got := commandTotal(t, s, get); got != 1 {
		t.Errorf("GET total = %d, want 1 (the RGET must not be counted)", got)
	}
	if got := commandTotal(t, s, set); got != 2 {
		t.Errorf("SET total = %d, want 2", got)
	}
	if got := commandTotal(t, s, del); got != 1 {
		t.Errorf("DEL total = %d, want 1", got)
	}
}

// TestHandle_TelemetryCountsClusteredOps: a command that Handle hands to the coordinator (and returns
// early) is still counted — this guards the recorder's placement above the routing block. Under the
// old placement (below routing) a coordinated op returned before the counter and was never recorded.
func TestHandle_TelemetryCountsClusteredOps(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{
		{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1},
		{ID: "c", Addr: "c", State: cluster.Alive, Incarnation: 1},
	}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)

	peer := newFakeReplicator() // every replica acks
	st := store.New()
	coord := NewCoordinator("a", router, st, peer, NewHintStore(), CoordinatorOptions{N: 3, R: 2, W: 3})
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(st, m, router, coord, tel)

	// A key this node owns, so Handle takes the coordinator path (returns before spec.run).
	var localKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if router.IsLocal(k) {
			localKey = k
			break
		}
	}

	h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte(localKey), []byte("v")}})

	if got := commandTotal(t, tel.Snapshot(), set); got != 1 {
		t.Errorf("coordinated SET total = %d, want 1 (clustered ops must be counted)", got)
	}
}

// TestHandle_StatsReportsPerCommandRows: STATS returns an uptime line plus one row per REGISTERED
// command (including ones never used, which report zeros). An internal verb like RGET is unregistered,
// so it gets no row and does not inflate another command's counters.
func TestHandle_StatsReportsPerCommandRows(t *testing.T) {
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(store.New(), nil, nil, nil, tel)

	h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte("k"), []byte("v")}})
	h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte("k2"), []byte("v")}})
	h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte("k")}})
	h.Handle(protocol.Command{Name: "RGET", Args: [][]byte{[]byte("k")}}) // never registered

	var buf bytes.Buffer
	if err := h.Handle(protocol.Command{Name: "STATS"}).Encode(&buf); err != nil {
		t.Fatalf("Encode STATS: %v", err)
	}
	got := buf.String()

	// Anchored on the trailing " p50=" so the counters are pinned exactly while the percentile values
	// (which depend on how fast the machine actually ran) stay out of the assertion.
	for _, want := range []string{
		"uptime_seconds:",
		"GET total=1 hits=1 misses=0 errors=0 p50=", // the GET found "k", so it is a hit
		"SET total=2 hits=0 misses=0 errors=0 p50=",
		"DEL total=0 hits=0 misses=0 errors=0 p50=", // registered but unused
	} {
		if !strings.Contains(got, want) {
			t.Errorf("STATS reply %q is missing %q", got, want)
		}
	}
	if strings.Contains(got, "RGET") {
		t.Errorf("STATS reply %q includes RGET; internal verbs must never be reported", got)
	}
}

// TestHandle_RecordsLatency: a served command has its duration observed, so the command reports a
// positive percentile while an untouched command stays at zero. This is the end-to-end proof that the
// timing site in Handle actually reaches the per-command histogram.
func TestHandle_RecordsLatency(t *testing.T) {
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(store.New(), nil, nil, nil, tel)

	h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte("k")}})

	s := tel.Snapshot()
	// Any served command lands in some bucket, so p50 is that bucket's bound — positive regardless of
	// how fast the machine is. Asserting ">0" rather than an exact bound keeps this from flaking.
	if got := commandSnap(t, s, get).P50; got <= 0 {
		t.Errorf("GET p50 = %v, want a positive duration after a served command", got)
	}
	// DEL was never served, so its histogram has no observations and reports zero, not a bogus bound.
	if got := commandSnap(t, s, del).P50; got != 0 {
		t.Errorf("DEL p50 = %v, want 0 (never served)", got)
	}
}

// TestHandle_StatsFollowsRegistration: the rows come from whatever was registered — stats itself has
// no per-command knowledge, so a different registration changes the report with no code change.
func TestHandle_StatsFollowsRegistration(t *testing.T) {
	tel := telemetry.New(ping) // only PING is tracked here
	h := NewHandler(store.New(), nil, nil, nil, tel)

	h.Handle(protocol.Command{Name: "PING"})
	h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte("k")}}) // handled, but not registered

	var buf bytes.Buffer
	if err := h.Handle(protocol.Command{Name: "STATS"}).Encode(&buf); err != nil {
		t.Fatalf("Encode STATS: %v", err)
	}
	got := buf.String()
	if !strings.Contains(got, "PING total=1") {
		t.Errorf("STATS reply %q is missing the registered PING row", got)
	}
	if strings.Contains(got, "GET total=") {
		t.Errorf("STATS reply %q reports GET, which was never registered", got)
	}
}

// TestHandle_StatsWithoutTelemetry: a Handler built with nil telemetry (what ServerOptions{} yields)
// still answers STATS instead of panicking — Snapshot is nil-safe, so the reply is the uptime line
// with no command rows.
func TestHandle_StatsWithoutTelemetry(t *testing.T) {
	h := NewHandler(store.New(), nil, nil, nil, nil)

	var buf bytes.Buffer
	if err := h.Handle(protocol.Command{Name: "STATS"}).Encode(&buf); err != nil {
		t.Fatalf("Encode STATS: %v", err)
	}
	if got := buf.String(); !strings.Contains(got, "uptime_seconds:0\r\n") {
		t.Errorf("STATS without telemetry = %q, want just the uptime line", got)
	}
}

// TestHandle_RecordsHitsAndMisses: a GET that finds a value counts a hit, one that finds nothing
// counts a miss, and an internal RGET counts neither — the outcome keys on the command NAME, not on
// the method, and RGET shares (*Handler).get.
func TestHandle_RecordsHitsAndMisses(t *testing.T) {
	tel := telemetry.New(ClientCommands...)
	s := store.New()
	if _, err := s.Set("present", []byte("v")); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s, nil, nil, nil, tel)

	h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte("present")}})  // hit
	h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte("absent")}})   // miss
	h.Handle(protocol.Command{Name: "RGET", Args: [][]byte{[]byte("present")}}) // neither

	got := commandSnap(t, tel.Snapshot(), get)
	if got.Total != 2 || got.Hits != 1 || got.Misses != 1 {
		t.Errorf("GET = %+v, want {Total:2 Hits:1 Misses:1} (the RGET must not count)", got)
	}
}

// TestHandle_RecordsErrors: a command whose reply is a RESP error counts an error for that command —
// RED's E, which the traffic counter alone can't show.
func TestHandle_RecordsErrors(t *testing.T) {
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(store.New(), nil, nil, nil, tel)

	// 3 args clears the arity check (SET takes 2..4) and lands in set's syntax-error branch.
	h.Handle(protocol.Command{Name: "SET", Args: [][]byte{[]byte("k"), []byte("v"), []byte("BADARG")}})

	got := commandSnap(t, tel.Snapshot(), set)
	if got.Total != 1 || got.Errors != 1 {
		t.Errorf("SET = %+v, want {Total:1 Errors:1}", got)
	}
}

// TestHandle_MovedIsTrafficNotError: a -MOVED redirect still counts as a received command but must NOT
// count as an error — it is a routing outcome, not a failure. Counting it would inflate the error rate
// every time the ring shifts.
func TestHandle_MovedIsTrafficNotError(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1}}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(store.New(), m, router, nil, tel)

	// A key this node does NOT own, so Handle answers with -MOVED.
	var remoteKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if !router.IsLocal(k) {
			remoteKey = k
			break
		}
	}

	var buf bytes.Buffer
	if err := h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte(remoteKey)}}).Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := buf.String(); !strings.HasPrefix(got, "-MOVED") {
		t.Fatalf("GET of a non-owned key = %q, want -MOVED (test premise)", got)
	}

	got := commandSnap(t, tel.Snapshot(), get)
	if got.Total != 1 {
		t.Errorf("GET total = %d, want 1 (a redirect is still traffic this node received)", got.Total)
	}
	if got.Errors != 0 || got.Hits != 0 || got.Misses != 0 {
		t.Errorf("GET = %+v, want no error/hit/miss recorded for a redirect", got)
	}
}

// TestHandle_ErroredGetIsNeitherHitNorMiss: a GET that fails (read quorum unreachable) counts an
// error and is recorded as neither a hit nor a miss — so hits+misses stay a count of SUCCESSFUL
// lookups and the hit ratio derived from them stays meaningful.
func TestHandle_ErroredGetIsNeitherHitNorMiss(t *testing.T) {
	m := cluster.NewMembers(cluster.Node{ID: "a", Addr: "a", State: cluster.Alive, Incarnation: 1})
	m.Merge([]cluster.Node{
		{ID: "b", Addr: "b", State: cluster.Alive, Incarnation: 1},
		{ID: "c", Addr: "c", State: cluster.Alive, Incarnation: 1},
	}, time.Now())
	router := cluster.NewRouter(m, 50, time.Hour)

	peer := newFakeReplicator()
	peer.fail["b"] = true // both peers down, so R=3 can never be met
	peer.fail["c"] = true
	st := store.New()
	coord := NewCoordinator("a", router, st, peer, NewHintStore(), CoordinatorOptions{N: 3, R: 3, W: 3})
	tel := telemetry.New(ClientCommands...)
	h := NewHandler(st, m, router, coord, tel)

	var localKey string
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if router.IsLocal(k) {
			localKey = k
			break
		}
	}

	var buf bytes.Buffer
	if err := h.Handle(protocol.Command{Name: "GET", Args: [][]byte{[]byte(localKey)}}).Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := buf.String(); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("GET with R=3 and both peers down = %q, want a -ERR (test premise)", got)
	}

	got := commandSnap(t, tel.Snapshot(), get)
	if got.Errors != 1 {
		t.Errorf("GET errors = %d, want 1", got.Errors)
	}
	if got.Hits != 0 || got.Misses != 0 {
		t.Errorf("GET = %+v, want neither a hit nor a miss for a failed lookup", got)
	}
}
