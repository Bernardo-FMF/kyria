package server

import (
	"fmt"
	"strings"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
)

// replicator is the slice of Peer the Coordinator needs. Depending on an interface
// (rather than *Peer) lets the quorum logic be unit-tested against a fake with
// deterministic acks and failures, no real sockets.
type replicator interface {
	Replicate(addr, verb string, args [][]byte) error
	Get(addr, key string) ([]byte, bool, error)
}

// Coordinator drives Dynamo-style N/R/W quorum replication. The node a client reaches
// (the primary, via -MOVED) applies the op locally, then the Coordinator fans it out
// to the other replicas and waits for a quorum: W acks for a write, R responses for a
// read. It resolves the replica set from the ring and talks to peers through a Peer.
type Coordinator struct {
	self    string          // this node's ID/addr, excluded from the peer fan-out
	router  *cluster.Router // resolves a key to its N-node replica set
	peer    replicator      // sends internal ops to the other replicas
	n, r, w int             // replication factor, read quorum, write quorum
}

// NewCoordinator returns a Coordinator for the given replica set size (n) and read/
// write quorums (r, w). self is this node's client address, excluded from fan-out.
func NewCoordinator(self string, router *cluster.Router, peer replicator, n, r, w int) *Coordinator {
	return &Coordinator{self: self, router: router, peer: peer, n: n, r: r, w: w}
}

// coordinate finishes a clustered op the primary has already applied locally: it
// drives the quorum and returns either the local reply (quorum met) or a RESP error.
func (c *Coordinator) coordinate(cmd protocol.Command, local protocol.Value) protocol.Value {
	msg, ok := local.AsError()
	if ok {
		return protocol.Error(msg)
	}

	key := string(cmd.Args[0])
	switch strings.ToUpper(cmd.Name) {
	case set:
		return c.write(key, rset, cmd.Args, local)
	case del:
		return c.write(key, rdel, cmd.Args, local)
	case get:
		return c.read(key, local)
	default:
		return local
	}
}

// write fans the client write out to the other replicas as verb and returns local
// once W acks (the local one included) have landed, else a RESP error.
func (c *Coordinator) write(key, verb string, args [][]byte, local protocol.Value) protocol.Value {
	replicas := c.router.Owners(key, c.n)
	need := min(c.w, len(replicas))
	op := func(addr string) bool {
		return c.peer.Replicate(addr, verb, args) == nil
	}
	acks := c.gather(peersExcept(replicas, c.self), need, op)
	if acks >= need {
		return local
	}

	return quorumError("write", acks, need)
}

// read gathers R responses for key across the replica set (the local read counts as
// one) and returns local when the quorum is met, else a RESP error.
func (c *Coordinator) read(key string, local protocol.Value) protocol.Value {
	replicas := c.router.Owners(key, c.n)
	need := min(c.r, len(replicas))
	if need <= 1 {
		return local
	}

	op := func(addr string) bool {
		_, _, err := c.peer.Get(addr, key)
		return err == nil
	}
	acks := c.gather(peersExcept(replicas, c.self), need, op)
	if acks >= need {
		return local
	}

	return quorumError("read", acks, need)
}

// gather runs op against each peer concurrently and returns the ack count — starting
// at 1 for the local replica — stopping as soon as need is reached, so one slow or
// dead replica can't hold up an already-met quorum.
func (c *Coordinator) gather(peers []string, need int, op func(addr string) bool) int {
	results := make(chan bool, len(peers))
	for _, peer := range peers {
		go func(addr string) {
			results <- op(addr)
		}(peer)
	}

	acks := 1
	for range peers {
		if <-results {
			acks++
		}
		if acks >= need {
			break
		}
	}
	return acks
}

// peersExcept returns replicas with self removed — the nodes to fan out to.
func peersExcept(replicas []string, self string) []string {
	peers := make([]string, 0, len(replicas))
	for _, addr := range replicas {
		if addr != self {
			peers = append(peers, addr)
		}
	}
	return peers
}

// quorumError builds the RESP error returned when a read/write falls short.
func quorumError(op string, got, need int) protocol.Value {
	return protocol.Error(fmt.Sprintf("ERR %s quorum not met: %d of %d replicas", op, got, need))
}
