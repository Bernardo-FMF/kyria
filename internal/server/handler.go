// Package server is kyria's TCP adapter: it reads RESP commands off a connection
// and dispatches them to a store.Store. This file is the dispatch itself — pure
// logic with no sockets, so it is unit-tested directly.
package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/merkle"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/telemetry"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// Command words kyria understands. Handle matches them case-insensitively.
const (
	ping  = "PING"
	get   = "GET"
	set   = "SET"
	del   = "DEL"
	nodes = "NODES"

	// Internal replica verbs — a coordinator sends these to a replica to store or read
	// a key on its LOCAL store, bypassing routing and replication. (A delete is a store:
	// the coordinator fans its tombstone out as an rset.)
	rget = "RGET"
	rset = "RSET"

	// rtree is the anti-entropy verb: RTREE <leafCount> asks a node to build a Merkle tree
	// over its local store at that leaf count and reply with tree.Encode().
	rtree = "RTREE"

	// rbucket is the anti-entropy fetch verb: RBUCKET <leafCount> <encodedBuckets> returns the
	// (key, blob) entries whose bucket is in that set — the entries a diff flagged for repair.
	rbucket = "RBUCKET"

	// stats is the STATS admin verb: it reports this node's telemetry counters (uptime plus the
	// GET/SET/DEL totals) as an INFO-style bulk reply. Keyless and node-local, so it is never routed.
	stats = "STATS"
)

var ClientCommands = []string{get, set, del}

const (
	ttlEx = "EX"
	ttlPx = "PX"
)

// commandSpec describes one command: how many arguments it requires and the
// method that runs it. run is a method expression — (*Handler).get and friends
// all have type func(*Handler, [][]byte) protocol.Value — so commands of
// different real arities share one signature and fit in a single table.
type commandSpec struct {
	minArgs int // minimum number of args
	maxArgs int // maximum number of args
	// clusteredOp is true when args[0] names a key that may be owned by another
	// node (get/set/del), false for keyless commands (ping/nodes). Handle reads it
	// to decide whether the command is subject to routing (a possible -MOVED).
	clusteredOp bool
	run         func(*Handler, [][]byte) protocol.Value // method expression that runs it
}

// commands is the dispatch table: Handle looks the command word up here, checks
// arity, then calls run. Adding a command is one entry plus its method. The bool
// is clusteredOp — set it for any command whose first arg is a routable key.
var commands = map[string]commandSpec{
	ping:  {0, 0, false, (*Handler).ping},
	get:   {1, 1, true, (*Handler).get},
	set:   {2, 4, true, (*Handler).set},
	del:   {1, 1, true, (*Handler).del},
	nodes: {0, 0, false, (*Handler).nodes},

	// Internal replica verbs (clusteredOp:false → Handle skips routing). rset carries
	// a VERSIONED blob {key, versionBlob} and reconciles it into the replica's sibling
	// set — its own method. rget reuses get: it just touches the raw stored bytes, since
	// it's the coordinator (not the read verb) that decodes + reconciles.
	rget: {1, 1, false, (*Handler).get},
	rset: {2, 2, false, (*Handler).rset},

	// rtree serves this node's local Merkle tree for anti-entropy; keyless, so no routing.
	rtree: {1, 1, false, (*Handler).rtree},

	// rbucket serves the entries in a requested bucket set for anti-entropy; keyless, no routing.
	rbucket: {2, 2, false, (*Handler).rbucket},

	// stats reports this node's telemetry counters; keyless, so no routing.
	stats: {0, 0, false, (*Handler).stats},
}

// Handler executes parsed commands against a store and returns RESP replies. It
// holds no connection state — the server owns the socket and calls Handle once
// per decoded command — so it is pure logic, unit-tested directly.
type Handler struct {
	store   store.Store
	members *cluster.Members
	// router is the consistent-hash routing table. Like members it is nil in
	// standalone mode, where Handle serves every key locally (no routing).
	router *cluster.Router
	// coordinator drives N/R/W replication for clustered ops. nil in standalone (or
	// when replication is off), where Handle serves clustered ops locally, no quorum.
	coordinator *Coordinator
	// telemetry records per-command counters for the STATS verb. May be nil — the Record calls are
	// no-ops on a nil receiver — which is how standalone construction and tests skip instrumentation.
	telemetry *telemetry.Telemetry
}

// NewHandler returns a Handler backed by s. members, router, and coordinator may be
// nil, which disables NODES, key routing, and replication respectively (standalone).
func NewHandler(store store.Store, members *cluster.Members, router *cluster.Router, coordinator *Coordinator, telemetry *telemetry.Telemetry) *Handler {
	return &Handler{
		store:       store,
		members:     members,
		router:      router,
		coordinator: coordinator,
		telemetry:   telemetry,
	}
}

// Handle runs one parsed command and returns its reply. It looks cmd.Name up in
// the command table (case-insensitively) and rejects an unknown command or a
// wrong argument count with a RESP error before dispatching to the method.
func (h *Handler) Handle(cmd protocol.Command) protocol.Value {
	name := strings.ToUpper(cmd.Name)
	spec, ok := commands[name]
	if !ok {
		return protocol.Error("ERR unknown command '" + cmd.Name + "'")
	}

	if len(cmd.Args) < spec.minArgs || len(cmd.Args) > spec.maxArgs {
		return protocol.Error(fmt.Sprintf("ERR wrong number of arguments for '%s'", cmd.Name))
	}

	h.telemetry.RecordCommand(name)

	// Routing: in a cluster, a command whose key this node does not own is answered
	// with a -MOVED redirect to the owner instead of served here. owner is the
	// owner's client address (the node ID is its TCP addr), so it drops straight
	// into the reply for the client to reconnect. MOVED is its own RESP error code,
	// not an ERR. Standalone nodes (router == nil) and keyless commands skip this
	// and always serve locally; an empty ring (!ok) falls through and serves locally.
	if h.router != nil && spec.clusteredOp {
		key := string(cmd.Args[0])
		if !h.router.IsLocal(key) {
			if owner, ok := h.router.Owner(key); ok {
				return protocol.Error("MOVED " + owner)
			}
		}
		// We own this key (no -MOVED above). If replication is on, hand the whole op
		// to the coordinator: it does the versioned local apply AND drives the N/R/W
		// quorum across the replica set. (Internal verbs are clusteredOp==false, so
		// they never reach here — they fall through to the plain local spec.run below,
		// which is what keeps a replicated write from re-replicating.)
		if h.coordinator != nil {
			return h.coordinator.coordinate(cmd)
		}
	}

	return spec.run(h, cmd.Args)
}

// ping replies +PONG.
func (h *Handler) ping(args [][]byte) protocol.Value {
	return protocol.SimpleString("PONG")
}

// get replies with the value as a bulk string, or a null bulk if the key is absent.
func (h *Handler) get(args [][]byte) protocol.Value {
	key := args[0]
	value, found := h.store.Get(string(key))
	if !found {
		return protocol.BulkString(nil)
	}
	return protocol.BulkString(value)
}

// set handles SET key value [EX seconds | PX milliseconds]: it stores the value
// (optionally with an expiry) and replies +OK, or a RESP error if the arguments
// are malformed or the store rejects the write.
func (h *Handler) set(args [][]byte) protocol.Value {
	key := string(args[0])
	value := args[1]

	// Each branch just sets err from its store call; the shared reply is below.
	// (ignore admitted for now)
	var err error
	switch len(args) {
	case 2:
		_, err = h.store.Set(key, value)
	case 4:
		n, convErr := strconv.Atoi(string(args[3]))
		if convErr != nil {
			return protocol.Error("ERR value is not an integer or out of range")
		}

		var ttl time.Duration
		switch strings.ToUpper(string(args[2])) {
		case ttlEx:
			ttl = time.Duration(n) * time.Second
		case ttlPx:
			ttl = time.Duration(n) * time.Millisecond
		default:
			return protocol.Error("ERR syntax error")
		}

		_, err = h.store.SetWithTTL(key, value, ttl)
	default:
		return protocol.Error("ERR syntax error") // e.g. len 3
	}

	if err != nil {
		return protocol.Error("ERR " + err.Error())
	}
	return protocol.SimpleString("OK")
}

// del removes the key, replying :1 if it existed or :0 otherwise.
func (h *Handler) del(args [][]byte) protocol.Value {
	key := args[0]
	deleted := h.store.Delete(string(key))

	intVal := 0
	if deleted {
		intVal = 1
	}

	return protocol.Integer(int64(intVal))
}

// rset applies a replicated write: args are [key, versionBlob], where versionBlob
// encodes the single incoming Version (value + clock) the coordinator computed. It
// reconciles that version into the key's stored sibling set under the store's lock,
// never re-incrementing the clock or re-replicating. Reply +OK.
func (h *Handler) rset(args [][]byte) protocol.Value {
	incoming, err := version.Decode(args[1])
	if err != nil || len(incoming) != 1 {
		return protocol.Error("ERR malformed version")
	}

	_, err = h.store.Update(string(args[0]), func(old []byte) []byte {
		existing, _ := version.Decode(old)
		return version.Encode(version.Reconcile(existing, incoming[0]))
	})

	if err != nil {
		return protocol.Error("ERR " + err.Error())
	}

	return protocol.SimpleString("OK")
}

// nodes replies with the cluster's live membership — one bulk string per alive
// member — or a RESP error when this node isn't part of a cluster.
func (h *Handler) nodes(args [][]byte) protocol.Value {
	if h.members == nil {
		return protocol.Error("ERR clustering is disabled")
	}
	alive := h.members.Alive()
	elems := make([]protocol.Value, 0, len(alive))
	for _, n := range alive {
		elems = append(elems, protocol.BulkString(fmt.Appendf(nil, "%s %s", n.ID, n.Addr)))
	}
	return protocol.Array(elems...)
}

// rtree serves the anti-entropy RTREE verb: it builds a Merkle tree over this node's local
// store at the requested leaf count and returns it encoded. The responder never diffs — it
// just serves its tree; the requesting node decodes it and runs the comparison.
func (h *Handler) rtree(args [][]byte) protocol.Value {
	leafCount, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.Error("ERR failed to parse tree leaf count")
	}

	t := merkle.New(leafCount)
	h.store.Range(func(key string, value []byte) {
		t.Add(key, value)
	})

	return protocol.BulkString(t.Encode())
}

// rbucket serves the RBUCKET verb: given a leaf count and an encoded bucket set, it returns this
// node's (key, blob) entries whose bucket is in that set. It scans the store ONCE, filtering by
// membership in the requested set — which is why the request carries the whole set of differing
// buckets rather than one bucket per call.
func (h *Handler) rbucket(args [][]byte) protocol.Value {
	leafCount, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.Error("ERR failed to parse tree leaf count")
	}

	buckets, err := decodeBuckets(args[1])
	if err != nil {
		return protocol.Error("ERR malformed bucket set")
	}

	want := map[int]bool{}
	for _, b := range buckets {
		want[b] = true
	}

	tree := merkle.New(leafCount)
	entries := map[string][]byte{}
	h.store.Range(func(key string, value []byte) {
		if want[tree.Bucket(key)] {
			entries[key] = value
		}
	})

	return protocol.BulkString(encodeEntries(entries))
}

// stats replies with this node's telemetry as an INFO-style bulk string — one `key:value` line per
// counter (uptime plus the GET/SET/DEL totals).
func (h *Handler) stats(args [][]byte) protocol.Value {
	//TODO
	return protocol.Value{}
}
