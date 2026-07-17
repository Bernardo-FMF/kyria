package server

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/merkle"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
)

// maxIdlePerPeer caps how many warm connections we keep per peer; a call that would
// return one beyond this closes it instead, so an idle cluster doesn't hold sockets
// open forever.
const maxIdlePerPeer = 8

// Peer is a small RESP client for node-to-node replication: the coordinator dials a
// replica's client port and issues the internal RSET/RGET verbs, reusing the same
// protocol codec the server already speaks. It keeps a small pool of warm connections
// per peer so a fan-out write doesn't pay a TCP handshake every time.
type Peer struct {
	timeout time.Duration // dial + I/O deadline per call

	mu   sync.Mutex               // guards idle
	idle map[string][]*pooledConn // warm connections, keyed by peer addr
}

// pooledConn is a connection kept warm in the pool. It pairs the socket with a
// persistent bufio.Reader: a fresh reader per call would drop any bytes bufio read
// past one reply, desyncing the next call on the reused connection.
type pooledConn struct {
	net.Conn
	r *bufio.Reader
}

// NewPeer returns a Peer whose every call dials, writes, and reads within timeout.
func NewPeer(timeout time.Duration) *Peer {
	return &Peer{
		timeout: timeout,
		idle:    make(map[string][]*pooledConn),
	}
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

// Replicate sends the write command [verb, args...] to the replica at addr and
// returns nil on its ack, or an error on a dial/IO failure or an -ERR reply. The
// coordinator uses it to fan a client write out as the internal RSET verb (a delete
// rides the same path, as a tombstone), forwarding the client's args verbatim.
func (p *Peer) Replicate(addr, verb string, args [][]byte) error {
	all := append([][]byte{[]byte(verb)}, args...)
	reply, err := p.do(addr, all...)
	if err != nil {
		return err
	}
	msg, ok := reply.AsError()
	if ok {
		return errors.New(msg)
	}
	return nil
}

// Tree fetches the peer at addr's Merkle tree via RTREE and decodes it into a *merkle.Tree
// ready to Diff. leaves is the cluster's fixed leaf count, forwarded so the peer builds a tree
// comparable to the caller's local one. An -ERR reply or a malformed tree becomes an error.
func (p *Peer) Tree(addr string, leaves int) (*merkle.Tree, error) {
	args := [][]byte{[]byte(rtree), []byte(strconv.Itoa(leaves))}

	reply, err := p.do(addr, args...)
	if err != nil {
		return nil, err
	}

	if msg, ok := reply.AsError(); ok {
		return nil, errors.New(msg)
	}

	blob, _ := reply.AsBulk()
	return merkle.Decode(blob)
}

// BucketEntries fetches the peer at addr's (key, blob) entries in the given buckets via RBUCKET,
// decoding the reply into a map. leaves is the cluster's fixed leaf count; buckets are the ones a
// Diff flagged. An -ERR reply or a malformed body becomes an error. The caller reconciles each
// returned entry into its own store.
func (p *Peer) BucketEntries(addr string, leaves int, buckets []int) (map[string][]byte, error) {
	args := [][]byte{[]byte(rbucket), []byte(strconv.Itoa(leaves)), encodeBuckets(buckets)}

	reply, err := p.do(addr, args...)
	if err != nil {
		return nil, err
	}

	if msg, ok := reply.AsError(); ok {
		return nil, errors.New(msg)
	}

	entries, _ := reply.AsBulk()
	return decodeEntries(entries)
}

// do sends args as a RESP command to addr and returns the decoded reply. It tries a warm pooled
// connection first; if that fails — most often because the peer closed it while idle — the conn is
// discarded and do retries ONCE on a fresh dial. A failure on the fresh dial is real and returned.
// The retry is safe because the replica verbs are idempotent (a repeated RSET re-reconciles the same
// version to a no-op; reads have no side effects). A healthy conn always goes back to the pool.
func (p *Peer) do(addr string, args ...[]byte) (protocol.Value, error) {
	conn := p.getIdle(addr)
	if conn != nil {
		reply, err := p.roundtrip(conn, args...)
		if err == nil {
			p.put(addr, conn)
			return reply, nil
		}
		conn.Close()
	}

	conn, err := p.dial(addr)
	if err != nil {
		return protocol.Value{}, err
	}
	reply, err := p.roundtrip(conn, args...)
	if err != nil {
		conn.Close()
		return protocol.Value{}, err
	}
	p.put(addr, conn)

	return reply, nil
}

// getIdle pops a warm connection to addr from the pool, or returns nil when none is pooled. It never
// dials, so do can tell a reused conn (worth a retry if it fails) from a fresh one.
func (p *Peer) getIdle(addr string) *pooledConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	conns := p.idle[addr]
	if len(conns) > 0 {
		last := conns[len(conns)-1]
		p.idle[addr] = conns[:len(conns)-1]
		return last
	}
	return nil
}

// dial opens a fresh connection to addr, pairing it with a persistent bufio.Reader. It takes no
// lock — a slow connect must not stall other peer ops holding the pool mutex.
func (p *Peer) dial(addr string) (*pooledConn, error) {
	newConn, err := net.DialTimeout("tcp", addr, p.timeout)
	if err != nil {
		return nil, err
	}
	return &pooledConn{Conn: newConn, r: bufio.NewReader(newConn)}, nil
}

// put returns a healthy conn to the pool, or closes it when addr already holds
// maxIdlePerPeer — bounding how many idle sockets we keep per peer.
func (p *Peer) put(addr string, pc *pooledConn) {
	p.mu.Lock()
	if len(p.idle[addr]) >= maxIdlePerPeer {
		p.mu.Unlock()
		pc.Close()
		return
	}

	p.idle[addr] = append(p.idle[addr], pc)
	p.mu.Unlock()
}

// roundtrip runs one request/reply on pc: set a fresh deadline, send the command,
// read one reply through pc's PERSISTENT reader.
func (p *Peer) roundtrip(pc *pooledConn, args ...[]byte) (protocol.Value, error) {
	err := pc.SetDeadline(time.Now().Add(p.timeout))
	if err != nil {
		return protocol.Value{}, err
	}

	elems := make([]protocol.Value, len(args))
	for i, a := range args {
		elems[i] = protocol.BulkString(a)
	}

	err = protocol.Array(elems...).Encode(pc)
	if err != nil {
		return protocol.Value{}, err
	}

	return protocol.Decode(pc.r)
}

// Close closes every pooled connection; call it on shutdown so warm sockets don't
// leak.
func (p *Peer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, idle := range p.idle {
		for _, conn := range idle {
			conn.Close()
		}
	}
	clear(p.idle)
}
