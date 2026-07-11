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
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// Command words kyria understands. Handle matches them case-insensitively.
const (
	ping  = "PING"
	get   = "GET"
	set   = "SET"
	del   = "DEL"
	nodes = "NODES"

	// Internal replica verbs — a coordinator sends these to a replica to store, read,
	// or delete a key on its LOCAL store, bypassing routing and replication.
	rget = "RGET"
	rset = "RSET"
	rdel = "RDEL"
)

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

	// The internal replica verbs reuse the get/set/del methods (identical local-store
	// ops) but with clusteredOp:false, so Handle skips routing — no -MOVED bounce back
	// to the coordinator, no re-replication.
	rget: {1, 1, false, (*Handler).get},
	rset: {2, 4, false, (*Handler).set},
	rdel: {1, 1, false, (*Handler).del},
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
}

// NewHandler returns a Handler backed by s. members, router, and coordinator may be
// nil, which disables NODES, key routing, and replication respectively (standalone).
func NewHandler(store store.Store, members *cluster.Members, router *cluster.Router, coordinator *Coordinator) *Handler {
	return &Handler{
		store:       store,
		members:     members,
		router:      router,
		coordinator: coordinator,
	}
}

// Handle runs one parsed command and returns its reply. It looks cmd.Name up in
// the command table (case-insensitively) and rejects an unknown command or a
// wrong argument count with a RESP error before dispatching to the method.
func (h *Handler) Handle(cmd protocol.Command) protocol.Value {
	spec, ok := commands[strings.ToUpper(cmd.Name)]
	if !ok {
		return protocol.Error("ERR unknown command '" + cmd.Name + "'")
	}

	if len(cmd.Args) < spec.minArgs || len(cmd.Args) > spec.maxArgs {
		return protocol.Error(fmt.Sprintf("ERR wrong number of arguments for '%s'", cmd.Name))
	}

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
		// We own this key (no -MOVED above). If replication is on, apply the op to
		// the local store and let the coordinator drive the N/R/W quorum across the
		// replica set. (Internal verbs are clusteredOp==false, so they never reach
		// here — they fall through to the plain local spec.run below, which is what
		// keeps a replicated write from re-replicating.)
		if h.coordinator != nil {
			local := spec.run(h, cmd.Args)
			return h.coordinator.coordinate(cmd, local)
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
