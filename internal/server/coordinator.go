package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/telemetry"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// replicator is the slice of Peer the Coordinator needs. Depending on an interface
// (rather than *Peer) lets the quorum logic be unit-tested against a fake with
// deterministic acks and failures, no real sockets.
type replicator interface {
	Replicate(addr, verb string, args [][]byte) error
	Get(addr, key string) ([]byte, bool, error)
}

// Coordinator drives Dynamo-style N/R/W quorum replication with vector-clock
// versioning. The primary (the node the client reached via -MOVED) applies the op to
// its local store — minting/advancing the clock inside store.Update — then fans the
// versioned write out to the replica set and waits for a quorum: W acks for a write,
// R responses for a read.
type Coordinator struct {
	self    string          // this node's ID/addr, excluded from the peer fan-out
	router  *cluster.Router // resolves a key to its N-node replica set
	store   store.Store     // this node's local store (holds versioned blobs)
	peer    replicator      // sends internal ops to the other replicas
	hints   *hintStore      // parks a write when a replica's fan-out fails; drained by the HintReplayer
	n, r, w int             // replication factor, read quorum, write quorum
	// telemetry records replication events (hints parked, read-repairs, admission rejects). May be
	// nil — the Record calls are no-ops on a nil receiver — which is how tests skip instrumentation.
	telemetry *telemetry.Telemetry
}

// CoordinatorOptions carries the quorum tunables and injected collaborators, bundled into one struct
// so NewCoordinator's signature stays stable as more are added. Unlike ServerOptions the zero value
// is NOT usable: N/R/W have no sensible defaults, and a zero quorum would make every op fail.
type CoordinatorOptions struct {
	N, R, W   int                  // replication factor, read quorum, write quorum
	Telemetry *telemetry.Telemetry // may be nil → recording is a no-op
}

// NewCoordinator returns a Coordinator over the local store, configured by opts. self is this node's
// client address, excluded from fan-out. hints is shared with the HintReplayer: write parks a hint
// here for any replica it can't reach.
func NewCoordinator(self string, router *cluster.Router, store store.Store, peer replicator, hints *hintStore, opts CoordinatorOptions) *Coordinator {
	return &Coordinator{
		self:      self,
		router:    router,
		store:     store,
		peer:      peer,
		hints:     hints,
		n:         opts.N,
		r:         opts.R,
		w:         opts.W,
		telemetry: opts.Telemetry,
	}
}

// coordinate applies a clustered client op end to end: the versioned local apply plus
// the N/R/W quorum across the replica set. The coordinator owns the local write now
// (no more apply-then-coordinate split in Handle).
func (c *Coordinator) coordinate(cmd protocol.Command) protocol.Value {
	key := string(cmd.Args[0])
	switch strings.ToUpper(cmd.Name) {
	case set:
		return c.write(key, cmd.Args[1])
	case del:
		return c.delete(key)
	case get:
		return c.read(key)
	default:
		return protocol.Error("ERR command not coordinated")
	}
}

// write mints a new version for value, stores it locally, then fans the SAME version
// out to the replica set, returning +OK once W acks land (else a RESP error).
func (c *Coordinator) write(key string, value []byte) protocol.Value {
	// Mint the new clock inside Update so the read-modify-write is atomic: decode the
	// current siblings, increment self on their frontier, reconcile the new version in.
	var newClock vclock.Clock
	admitted, err := c.store.Update(key, func(old []byte) []byte {
		existing, _ := version.Decode(old)
		newClock = version.Frontier(existing).Increment(c.self)
		return version.Encode(version.Reconcile(existing,
			version.Version{Value: value, Clock: newClock}))
	})

	if err != nil {
		return protocol.Error("ERR " + err.Error())
	}

	if !admitted {
		c.telemetry.RecordEvent(evAdmissionRejects)
	}

	blob := version.Encode([]version.Version{{Value: value, Clock: newClock}})
	replicas := c.router.Owners(key, c.n)
	need := min(c.w, len(replicas))

	// Fan the write out, parking a hint for any replica we can't reach so a write that still met
	// quorum isn't lost to a down node — the HintReplayer redelivers it on recovery. Every failed
	// peer gets a hint (all gather goroutines run to completion), and parking happens inside the
	// goroutine, so a hint can land just after the client's +OK. delete() parks hints the same way
	// (its tombstone is just another rset), so a delete is equally durable across a down replica.
	acks := c.gather(peersExcept(replicas, c.self), need, func(addr string) bool {
		err := c.peer.Replicate(addr, rset, [][]byte{[]byte(key), blob})
		if err != nil {
			c.hints.add(addr, key, blob)
			c.telemetry.RecordEvent(evHintsParked)
			return false
		}
		return true
	})

	if acks >= need {
		return protocol.SimpleString("OK")
	}

	return quorumError("write", acks, need)
}

// delete writes a tombstone for key — a "gone" version stamped with a superseding clock — and fans
// it out to the replica set like write() does, returning :1 if a live value existed before (else
// :0) once W acks land. Tombstoning rather than removing the key is what stops a replica that missed
// the delete from resurrecting the value: the tombstone rides read-repair / anti-entropy and
// re-buries it. A hint is parked for any replica it can't reach, so a quorum-met delete still lands
// on a node that recovers later.
func (c *Coordinator) delete(key string) protocol.Value {
	var newClock vclock.Clock
	var opState int64
	deletedAt := time.Now().Unix()
	admitted, err := c.store.Update(key, func(old []byte) []byte {
		existing, _ := version.Decode(old)
		newClock = version.Frontier(existing).Increment(c.self)
		if len(version.Live(existing)) > 0 {
			opState = 1
		}

		return version.Encode(version.Reconcile(existing,
			version.Tombstone(newClock, deletedAt)))
	})

	if err != nil {
		return protocol.Error("ERR " + err.Error())
	}

	if !admitted {
		c.telemetry.RecordEvent(evAdmissionRejects)
	}

	blob := version.Encode([]version.Version{version.Tombstone(newClock, deletedAt)})
	replicas := c.router.Owners(key, c.n)
	need := min(c.w, len(replicas))

	acks := c.gather(peersExcept(replicas, c.self), need, func(addr string) bool {
		err := c.peer.Replicate(addr, rset, [][]byte{[]byte(key), blob})
		if err != nil {
			c.hints.add(addr, key, blob)
			c.telemetry.RecordEvent(evHintsParked)
			return false
		}
		return true
	})

	if acks >= need {

		return protocol.Integer(opState)
	}

	return quorumError("delete", acks, need)
}

// peerResult carries one replica's decoded sibling set back to the read loop. addr
// keys it into the responders map so read-repair can tell who returned what.
type peerResult struct {
	versions []version.Version
	ok       bool
	addr     string
}

// read gathers R sibling sets for key (the local one counts as one), reconciles them,
// and returns the resulting value.
func (c *Coordinator) read(key string) protocol.Value {
	replicas := c.router.Owners(key, c.n)
	need := min(c.r, len(replicas))

	// The local sibling set is response #1.
	blob, _ := c.store.Get(key)
	merged, _ := version.Decode(blob)
	responses := 1

	// responders records each replica's set exactly as it was read, so read-repair can
	// push merged back to the ones that lagged. Seeding self with merged captures the
	// local decode: Reconcile reassigns merged to fresh slices below, so this entry
	// keeps pointing at self's original set. Only replicas that actually respond are
	// recorded — a failed peer's state is unknown, so it's never repaired blindly.
	responders := map[string][]version.Version{c.self: merged}

	if responses < need {
		peers := peersExcept(replicas, c.self)
		results := make(chan peerResult, len(peers))

		for _, addr := range peers {
			go func(addr string) {
				d, _, err := c.peer.Get(addr, key)
				if err != nil {
					results <- peerResult{ok: false}
					return
				}
				v, err := version.Decode(d)
				results <- peerResult{versions: v, ok: err == nil, addr: addr}
			}(addr)
		}

		for range peers {
			if responses >= need {
				break
			}

			res := <-results

			if !res.ok {
				continue
			}

			responses++
			// Record the responder's raw set before folding it in — here in the main
			// loop, so building responders never races the peer goroutines.
			responders[res.addr] = res.versions

			for _, v := range res.versions {
				merged = version.Reconcile(merged, v)
			}
		}
	}

	if responses < need {
		return quorumError("read", responses, need)
	}

	// Quorum met and merged is final — heal the laggards off the read path so it never
	// delays the reply. The responses > 1 guard skips the single-responder fast path,
	// where self already IS merged and there's nothing to repair.
	if responses > 1 {
		go c.readRepair(key, merged, responders)
	}

	// Collapse the reconciled set to what the client sees: a winning tombstone (or an empty set)
	// reads as a MISS, never as its empty value. Read-repair above ran on the full merged set, so
	// the tombstone still propagates to laggards — Live only filters the reply.
	live := version.Live(merged)
	if len(live) == 0 {
		return protocol.BulkString(nil)
	}
	return protocol.BulkString(live[0].Value)
}

// readRepair heals replicas that returned a set behind the reconciled result. For each
// responder whose set differs from merged, it pushes merged back — the local node
// updates its own store, a peer gets an RSET per version. It's meant to run off the
// read path (in a goroutine): best-effort, and must never delay the client reply.
func (c *Coordinator) readRepair(key string, merged []version.Version, responders map[string][]version.Version) {
	for addr, set := range responders {
		if version.Equal(set, merged) {
			continue
		}

		c.telemetry.RecordEvent(evReadRepairs)
		if addr == c.self {
			c.store.Update(key, func(old []byte) []byte {
				existing, _ := version.Decode(old)
				for _, v := range merged {
					existing = version.Reconcile(existing, v)
				}
				return version.Encode(existing)
			})
			continue
		}

		for _, v := range merged {
			// Best effort operation, this ensures that at least an attempt to repair was made,
			// not a confirmation that the peer repair was successful.
			c.peer.Replicate(addr, rset, [][]byte{[]byte(key), version.Encode([]version.Version{v})})
		}
	}
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
