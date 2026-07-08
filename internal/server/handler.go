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

// TODO(Handler + Handle): implement command dispatch.
//
// Imports you'll need: "strings", the protocol package, and the store package
// (github.com/Bernardo-FMF/kyria/internal/{protocol,store}).
//
// Handler holds the store that commands run against:
//
//	type Handler struct { store store.Store }
//	func NewHandler(s store.Store) *Handler { ... }
//
// Handle runs one parsed command and returns its RESP reply:
//
//	func (h *Handler) Handle(cmd protocol.Command) protocol.Value
//
// Switch on strings.ToUpper(cmd.Name). Each case first checks its argument count
// (on a mismatch, reply protocol.Error("ERR wrong number of arguments for '" +
// cmd.Name + "'")), then does its work. Remember cmd.Args is [][]byte — a key is
// string(args[i]); a value stays []byte.
//
//	PING (0 args) → protocol.SimpleString("PONG").
//	GET  (1 arg)  → h.store.Get(string(args[0])). Found → protocol.BulkString(val);
//	                missing → protocol.BulkString(nil) (RESP null = "no such key").
//	SET  (2 args) → h.store.Set(string(args[0]), args[1]). On error (a size-limit
//	                violation) → protocol.Error("ERR " + err.Error()); otherwise
//	                protocol.SimpleString("OK"). Set also returns an "admitted"
//	                bool — ignore it for now (we can surface admission later).
//	DEL  (1 arg)  → h.store.Delete(string(args[0])) reports whether it existed;
//	                protocol.Integer(1) if so, else protocol.Integer(0).
//	default       → protocol.Error("ERR unknown command '" + cmd.Name + "'").
//
// A tiny helper for the arity check (name, got, want → *ProtocolError-style
// reply Value) keeps the cases uncluttered, but a per-case if is fine too.
const (
	ping   = "PING"
	get    = "GET"
	set    = "SET"
	delete = "DEL"
)

type Handler struct {
	store store.Store
}

func NewHandler(s store.Store) *Handler {
	return &Handler{
		store: s,
	}
}

func (h *Handler) Handle(cmd protocol.Command) protocol.Value {
	switch strings.ToUpper(cmd.Name) {
	case ping:
		return h.ping()
	case get:
		err := validateArgs(cmd.Args, 1, cmd.Name)
		if err != nil {
			return protocol.Error(err.Error())
		}
		return h.get(cmd.Args[0])
	case set:
		err := validateArgs(cmd.Args, 2, cmd.Name)
		if err != nil {
			return protocol.Error(err.Error())
		}
		return h.set(cmd.Args[0], cmd.Args[1])
	case delete:
		err := validateArgs(cmd.Args, 1, cmd.Name)
		if err != nil {
			return protocol.Error(err.Error())
		}
		return h.delete(cmd.Args[0])
	default:
		return protocol.Error("ERR unknown command '" + cmd.Name + "'")
	}
}

func validateArgs(args [][]byte, expectedSize int, name string) error {
	if len(args) == expectedSize {
		return nil
	}
	return fmt.Errorf("ERR wrong number of arguments for '%s'", name)
}

func (h *Handler) ping() protocol.Value {
	return protocol.SimpleString("PONG")
}

func (h *Handler) get(key []byte) protocol.Value {
	value, found := h.store.Get(string(key))
	if !found {
		return protocol.BulkString(nil)
	}
	return protocol.BulkString(value)
}

func (h *Handler) set(key []byte, value []byte) protocol.Value {
	// ignore admitted for now
	_, err := h.store.Set(string(key), value)
	if err != nil {
		return protocol.Error("ERR " + err.Error())
	}
	return protocol.SimpleString("OK")
}

func (h *Handler) delete(key []byte) protocol.Value {
	deleted := h.store.Delete(string(key))

	intVal := 0
	if deleted {
		intVal = 1
	}

	return protocol.Integer(int64(intVal))
}
