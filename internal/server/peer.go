package server

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/protocol"
)

// Peer is a small RESP client for node-to-node replication: the coordinator dials a
// replica's client port and issues the internal RSET/RGET/RDEL verbs, reusing the
// same protocol codec the server already speaks. It opens a fresh TCP connection per
// call — a connection pool is a later optimization (backlog).
type Peer struct {
	timeout time.Duration // dial + I/O deadline per call
}

// NewPeer returns a Peer whose every call dials, writes, and reads within timeout.
func NewPeer(timeout time.Duration) *Peer {
	return &Peer{timeout: timeout}
}

// Set replicates a write to the replica at addr via RSET, propagating a TTL as PX
// milliseconds when ttl > 0. It returns nil on the replica's +OK, or an error if the
// dial/IO fails or the replica replies -ERR.
func (p *Peer) Set(addr, key string, value []byte, ttl time.Duration) error {
	args := [][]byte{[]byte(rset), []byte(key), value}
	if ttl > 0 {
		args = append(args, []byte(ttlPx), []byte(strconv.FormatInt(ttl.Milliseconds(), 10)))
	}

	reply, err := p.do(addr, args...)
	if err != nil {
		return err
	}
	msg, ok := reply.AsError()
	if ok {
		return errors.New(msg)
	}

	return nil
}

// Get reads key from the replica at addr via RGET, returning (value, found). A null
// bulk (the replica's miss) is (nil, false, nil); an -ERR reply is a non-nil error.
func (p *Peer) Get(addr, key string) ([]byte, bool, error) {
	args := [][]byte{[]byte(rget), []byte(key)}
	reply, err := p.do(addr, args...)
	if err != nil {
		return nil, false, err
	}
	msg, ok := reply.AsError()
	if ok {
		return nil, false, errors.New(msg)
	}

	value, ok := reply.AsBulk()

	return value, ok, nil
}

// Del deletes key on the replica at addr via RDEL. It returns nil on success, or an
// error on a dial/IO failure or an -ERR reply. (The :0/:1 count isn't inspected —
// the coordinator only needs the ack.)
func (p *Peer) Del(addr, key string) error {
	args := [][]byte{[]byte(rdel), []byte(key)}
	reply, err := p.do(addr, args...)
	if err != nil {
		return err
	}
	msg, ok := reply.AsError()
	if ok {
		return errors.New(msg)
	}
	return nil
}

// do dials addr, sends args as a RESP array of bulk strings (a command), and returns
// the decoded reply. A fresh connection per call, bounded by p.timeout.
func (p *Peer) do(addr string, args ...[]byte) (protocol.Value, error) {
	conn, err := net.DialTimeout("tcp", addr, p.timeout)
	if err != nil {
		return protocol.Value{}, err
	}
	defer conn.Close()
	err = conn.SetDeadline(time.Now().Add(p.timeout))
	if err != nil {
		return protocol.Value{}, err
	}

	elems := make([]protocol.Value, len(args))
	for i, a := range args {
		elems[i] = protocol.BulkString(a)
	}

	err = protocol.Array(elems...).Encode(conn)
	if err != nil {
		return protocol.Value{}, err
	}

	return protocol.Decode(bufio.NewReader(conn))
}
