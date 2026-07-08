// Package server is kyria's TCP adapter: it reads RESP commands off a connection
// and dispatches them to a store.Store. This file is the dispatch itself — pure
// logic with no sockets, so it is unit-tested directly.
package server

import (
	"fmt"
	"strings"

	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// Command words kyria understands. Handle matches them case-insensitively.
const (
	ping = "PING"
	get  = "GET"
	set  = "SET"
	del  = "DEL"
)

// commandSpec describes one command: how many arguments it requires and the
// method that runs it. run is a method expression — (*Handler).get and friends
// all have type func(*Handler, [][]byte) protocol.Value — so commands of
// different real arities share one signature and fit in a single table.
type commandSpec struct {
	arity int                                     // required number of args
	run   func(*Handler, [][]byte) protocol.Value // method expression that runs it
}

// commands is the dispatch table: Handle looks the command word up here, checks
// arity, then calls run. Adding a command is one entry plus its method.
var commands = map[string]commandSpec{
	ping: {0, (*Handler).ping},
	get:  {1, (*Handler).get},
	set:  {2, (*Handler).set},
	del:  {1, (*Handler).del},
}

// Handler executes parsed commands against a store and returns RESP replies. It
// holds no connection state — the server owns the socket and calls Handle once
// per decoded command — so it is pure logic, unit-tested directly.
type Handler struct {
	store store.Store
}

// NewHandler returns a Handler backed by s.
func NewHandler(s store.Store) *Handler {
	return &Handler{
		store: s,
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

	err := validateArgs(cmd.Args, spec.arity, cmd.Name)
	if err != nil {
		return protocol.Error(err.Error())
	}
	return spec.run(h, cmd.Args)
}

func validateArgs(args [][]byte, expectedSize int, name string) error {
	if len(args) == expectedSize {
		return nil
	}
	return fmt.Errorf("ERR wrong number of arguments for '%s'", name)
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

// set stores the value and replies +OK, or a RESP error if the store rejects it.
func (h *Handler) set(args [][]byte) protocol.Value {
	key := args[0]
	value := args[1]
	// ignore admitted for now
	_, err := h.store.Set(string(key), value)
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
