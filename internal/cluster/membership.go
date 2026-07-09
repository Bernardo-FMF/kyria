// Package cluster turns a set of kyria nodes into a cluster. This file is the
// membership state: each node's local view of who is in the cluster and whether
// they're alive, kept eventually-consistent by gossip. It is pure — no sockets,
// no goroutines — so the merge and failure-detection logic is unit-tested
// directly. The gossip loop and UDP transport that drive it live elsewhere (6b).
package cluster

import (
	"sync"
	"time"
)

// NodeState is a member's liveness as carried in the gossiped view. The values
// are ordered by "deadness" so a merge can break incarnation ties by taking the
// higher (deader) state. Suspect is reserved for the later SWIM phase and is not
// produced by the simplified gossip detector yet.
type NodeState uint8

const (
	Alive NodeState = iota
	Suspect
	Dead
)

// Node is one member's gossiped state — the unit peers exchange. A higher
// Incarnation is fresher information about that node; a node is the only one that
// raises its own Incarnation (each gossip round, and to refute a false
// Suspect/Dead claim about itself). Keeping this version field now is what makes
// the later SWIM upgrade a small change rather than a refactor.
type Node struct {
	ID          string
	Addr        string // UDP gossip address, "host:port"
	State       NodeState
	Incarnation uint64
}

// entry is the local bookkeeping around a Node: its gossiped state plus the local
// time we last learned something newer about it, used for timeout failure detection.
type entry struct {
	node     Node
	lastSeen time.Time
}

// Members is this node's view of the cluster: a concurrency-safe roster keyed by
// node ID (the gossip loop and the receive path touch it from different
// goroutines) plus the ID of self.
type Members struct {
	mu    sync.Mutex
	self  string
	nodes map[string]*entry
}

// NewMembers returns a roster seeded with self as its only member, forced to
// state Alive. Every method that touches the roster holds m.mu.
func NewMembers(self Node) *Members {
	nodes := make(map[string]*entry)

	self.State = Alive
	nodes[self.ID] = &entry{
		node:     self,
		lastSeen: time.Now(),
	}

	return &Members{
		self:  self.ID,
		nodes: nodes,
	}
}

// Bump is the self-heartbeat: it raises self's incarnation and refreshes lastSeen.
// The gossip loop calls it each round so peers keep seeing self advance — a self
// whose incarnation stops climbing is how others come to detect it as failed.
func (m *Members) Bump(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// self is the *entry in the map, so writing through it mutates the roster.
	self := m.nodes[m.self]
	self.lastSeen = now
	self.node.Incarnation++
}

// Merge folds a peer's gossiped view into the local one (anti-entropy). It is the
// convergence rule of the whole protocol — commutative and idempotent, so peers
// exchanging views in any order settle on the same state. Per remote node: a
// strictly higher incarnation is adopted wholesale; on an incarnation tie the
// deader state wins; a claim that self is not alive is refuted by out-incarnating
// it; and other news about self is ignored.
func (m *Members) Merge(remote []Node, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, r := range remote {
		local := m.nodes[r.ID]

		if local == nil { // unknown → insert
			m.nodes[r.ID] = &entry{node: r, lastSeen: now}
			continue
		}

		if r.ID == m.self {
			if r.State != Alive && r.Incarnation >= local.node.Incarnation {
				local.node.Incarnation = r.Incarnation + 1
				local.node.State = Alive
				local.lastSeen = now
			}
			continue
		}

		switch {
		case r.Incarnation > local.node.Incarnation:
			local.node = r
			local.lastSeen = now
		case r.Incarnation == local.node.Incarnation && r.State > local.node.State:
			local.node.State = r.State
			local.lastSeen = now
		}
	}
}

// DetectFailures marks every alive non-self node whose last update is older than
// timeout as Dead; a revived node later refutes with a higher incarnation. Taking
// now as a parameter keeps it a pure, sleep-free predicate, like the store's
// expired(now).
func (m *Members) DetectFailures(now time.Time, timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, l := range m.nodes {
		if l.node.ID == m.self {
			continue
		}

		if l.node.State == Alive && now.Sub(l.lastSeen) > timeout {
			l.node.State = Dead
		}
	}
}

// Snapshot returns a copy of every member's Node — the payload the gossip loop
// ships to peers.
func (m *Members) Snapshot() []Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodes := make([]Node, 0, len(m.nodes))
	for _, l := range m.nodes {
		nodes = append(nodes, l.node)
	}

	return nodes
}

// Alive returns a copy of the members currently in state Alive — the set the ring
// will place keys across. Order is unspecified.
func (m *Members) Alive() []Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	nodes := make([]Node, 0, len(m.nodes))
	for _, l := range m.nodes {
		if l.node.State == Alive {
			nodes = append(nodes, l.node)
		}
	}

	return nodes
}

func (m *Members) Self() Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.nodes[m.self].node
}
