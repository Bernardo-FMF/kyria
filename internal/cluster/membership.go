package cluster

import (
	"log/slog"
	"sync"
	"time"
)

// NodeState is a member's liveness as carried in the gossiped view. The values
// are ordered by "deadness" so a merge can break incarnation ties by taking the
// higher (deader) state.
type NodeState uint8

const (
	Alive NodeState = iota
	Suspect
	Dead
)

// Node is one member's gossiped state.
// A higher incarnation is fresher information about that node; a node is the only
// one that raises its own incarnation (each gossip round, and to refute a false
// Suspect/Dead claim about itself).
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
// node ID plus the ID of self.
type Members struct {
	mu     sync.Mutex
	self   string
	nodes  map[string]*entry
	logger *slog.Logger
}

// NewMembers returns a roster seeded with self as its only member, forced to
// state Alive. Every method that touches the roster holds the lock.
func NewMembers(self Node, logger *slog.Logger) *Members {
	nodes := make(map[string]*entry)

	self.State = Alive
	nodes[self.ID] = &entry{
		node:     self,
		lastSeen: time.Now(),
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Members{
		self:   self.ID,
		nodes:  nodes,
		logger: logger.With("component", "membership"),
	}
}

// Bump is the self-heartbeat: it raises self's incarnation and refreshes lastSeen.
// The gossip loop calls it each round so peers keep seeing self advance, otherwise
// the remaining nodes would detect self as failed.
func (m *Members) Bump(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// self is the *entry in the map, so writing through it mutates the roster.
	self := m.nodes[m.self]
	self.lastSeen = now
	self.node.Incarnation++
}

// Merge folds a peer's gossiped view into the local one (anti-entropy). The operation
// is commutative and idempotent, so peers exchanging views in any order settle on
// the same state.
// Per remote node:
//  1. a strictly higher incarnation is adopted;
//  2. on an incarnation tie the deader state wins;
//  3. a claim that self is not alive is refuted by out-incarnating it;
//     and other news about self is ignored.
func (m *Members) Merge(remote []Node, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, r := range remote {
		local := m.nodes[r.ID]

		if local == nil { // unknown → insert
			m.nodes[r.ID] = &entry{node: r, lastSeen: now}
			m.logger.Info("node joined", "node", r.ID, "addr", r.Addr, "state", r.State)
			continue
		}

		if r.ID == m.self {
			if r.State != Alive && r.Incarnation >= local.node.Incarnation {
				local.node.Incarnation = r.Incarnation + 1
				local.node.State = Alive
				local.lastSeen = now
				m.logger.Warn("refuted a false claim about self",
					"claimed", r.State, "incarnation", local.node.Incarnation)
			}
			continue
		}

		switch {
		case r.Incarnation > local.node.Incarnation:
			was := local.node.State
			local.node = r
			local.lastSeen = now
			m.logStateChange(r.ID, was, r.State)
		case r.Incarnation == local.node.Incarnation && r.State > local.node.State:
			was := local.node.State
			local.node.State = r.State
			local.lastSeen = now
			m.logStateChange(r.ID, was, r.State)
		}
	}
}

// DetectFailures marks every alive non-self node whose last update is older than
// timeout as Dead; a revived node later refutes with a higher incarnation.
func (m *Members) DetectFailures(now time.Time, timeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, l := range m.nodes {
		if l.node.ID == m.self {
			continue
		}

		if l.node.State == Alive && now.Sub(l.lastSeen) > timeout {
			l.node.State = Dead
			m.logger.Warn("node marked dead", "node", l.node.ID, "silent_for", now.Sub(l.lastSeen))
		}
	}
}

// logStateChange reports a member's liveness transition. Called with the lock held.
func (m *Members) logStateChange(id string, was, now NodeState) {
	if was == now {
		return
	}

	if now == Alive {
		m.logger.Info("node recovered", "node", id, "was", was)
		return
	}

	m.logger.Warn("node state changed", "node", id, "was", was, "now", now)
}

// Snapshot returns a copy of every member's Node: the payload the gossip loop
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

// Alive returns a copy of the members currently in state Alive.
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

// Self returns the self node.
func (m *Members) Self() Node {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.nodes[m.self].node
}
