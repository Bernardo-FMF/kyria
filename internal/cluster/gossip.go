package cluster

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/binenc"
)

type GossiperOption func(*Gossiper)

func WithSeeds(s []string) GossiperOption {
	return func(g *Gossiper) { g.seeds = s }
}

func WithGossipInterval(d time.Duration) GossiperOption {
	return func(g *Gossiper) { g.gossipInterval = d }
}

func WithFailTimeout(d time.Duration) GossiperOption {
	return func(g *Gossiper) { g.failTimeout = d }
}

func WithFanout(i int) GossiperOption {
	return func(g *Gossiper) { g.fanout = i }
}

// Gossiper drives a Members roster over UDP: each round it heartbeats (Bump),
// reaps stale peers (DetectFailures), and sends its Snapshot to a few random
// peers; incoming packets are decoded and Merged in. Two goroutines run it — a
// receive loop and a periodic gossip loop — both ended by Stop (close the conn to
// unblock the reader, close stop to end the ticker loop), same shape as the janitor.
type Gossiper struct {
	members        *Members
	conn           net.PacketConn
	seeds          []string      // bootstrap peer UDP addresses ("host:port")
	gossipInterval time.Duration // how often to heartbeat, gossip, and detect failures
	failTimeout    time.Duration // mark a peer Dead after this long with no fresh news
	fanout         int           // how many random peers to gossip to each round

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

// Max size of a udp packet.
// Serves as the worst case scenario when reading a packet - most packets will be much smaller than this
const udpPacketSize = 65536

// A gossip message is a compact big-endian encoding of a []Node, sized to fit in a
// single UDP packet:
//
//	[uint16 count]  then, per node:  [uint8 State][uint64 Incarnation]
//	                                 [uint16 idLen][id][uint16 addrLen][addr]
//
// Strings are length-prefixed and the count is bounded, so a message can never
// overflow a uint16.

// marshal encodes nodes into the wire format above. It errors if there are more
// than a uint16 of nodes, or if an ID/Addr is too long for its length prefix.
func marshal(nodes []Node) ([]byte, error) {
	if len(nodes) > math.MaxUint16 {
		return nil, fmt.Errorf("cluster: too many nodes to encode: %d", len(nodes))
	}

	buf := new(bytes.Buffer)
	binenc.PutUint16(buf, uint16(len(nodes)))

	for _, n := range nodes {
		buf.WriteByte(byte(n.State))
		binenc.PutUint64(buf, n.Incarnation)
		if err := binenc.PutString(buf, n.ID); err != nil {
			return nil, err
		}
		if err := binenc.PutString(buf, n.Addr); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// unmarshal decodes a gossip message produced by marshal back into a []Node. It
// walks data with a bounds-checked cursor: because this is untrusted network input,
// every read is guarded against len(data) and returns errMalformed rather than
// indexing past the slice, so no packet can make it panic.
func unmarshal(data []byte) ([]Node, error) {
	cursor := 0
	count, cursor, err := binenc.Uint16(data, cursor)
	if err != nil {
		return nil, err
	}

	nodes := make([]Node, 0, count)

	// Decode one node per iteration, in marshal's field order.
	var incarnation uint64
	var id, addr string
	for range count {
		if len(data)-cursor < 1 {
			return nil, binenc.ErrMalformed
		}
		state := NodeState(data[cursor])
		cursor++
		incarnation, cursor, err = binenc.Uint64(data, cursor)
		if err != nil {
			return nil, err
		}

		id, cursor, err = binenc.String(data, cursor)
		if err != nil {
			return nil, err
		}

		addr, cursor, err = binenc.String(data, cursor)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, Node{ID: id, Addr: addr, State: state, Incarnation: incarnation})
	}

	return nodes, nil
}

// pickPeers returns up to k addresses chosen at random from addrs. It works on a
// copy, so the caller's slice is left untouched; fewer than k come back when addrs
// holds fewer than k.
func pickPeers(addrs []string, k int) []string {
	shuffled := slices.Clone(addrs)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	if k > len(shuffled) {
		k = len(shuffled)
	}
	return shuffled[:k]
}

// NewGossiper wraps a Members roster and a bound UDP connection into a gossip
// engine. Call Start to run it and Stop to shut it down.
func NewGossiper(members *Members, conn net.PacketConn, opts ...GossiperOption) *Gossiper {
	g := &Gossiper{
		members:        members,
		conn:           conn,
		gossipInterval: 1 * time.Second, // defaults live here now
		failTimeout:    5 * time.Second,
		fanout:         3,
		stop:           make(chan struct{}),
	}

	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Start launches the two goroutines that run the engine: the receive loop and the
// periodic gossip loop.
func (g *Gossiper) Start() {
	g.wg.Add(2)

	go g.receiveLoop()
	go g.gossipLoop()
}

// Stop shuts the engine down and is safe to call more than once. Closing stop ends
// the gossip loop and closing the connection unblocks the receive loop's ReadFrom;
// Wait then drains both goroutines.
func (g *Gossiper) Stop() {
	g.stopOnce.Do(func() {
		close(g.stop)
		g.conn.Close()
	})
	g.wg.Wait()
}

// receiveLoop reads gossip packets until the connection is closed, merging each
// well-formed one into the roster. A malformed packet is skipped, not fatal, and a
// read error (the connection closed by Stop) ends the loop.
func (g *Gossiper) receiveLoop() {
	defer g.wg.Done()

	buf := make([]byte, udpPacketSize)
	for {
		n, _, err := g.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		nodes, err := unmarshal(buf[:n])
		if err != nil {
			continue
		}
		g.members.Merge(nodes, time.Now())
	}
}

// gossipLoop runs one gossip round on every tick until Stop closes the stop channel.
func (g *Gossiper) gossipLoop() {
	defer g.wg.Done()

	ticker := time.NewTicker(g.gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-g.stop:
			return
		case <-ticker.C:
			g.round()
		}
	}
}

// round is one gossip cycle: heartbeat, reap stale peers, then send the current
// membership snapshot to a random fanout of known peers (plus the configured
// seeds), skipping self. UDP is best-effort, so send errors are ignored — a dropped
// packet is made up for on the next round.
func (g *Gossiper) round() {
	now := time.Now()

	g.members.Bump(now)
	g.members.DetectFailures(now, g.failTimeout)

	// Gossip targets: every alive peer's address plus the seeds, deduped (a seed we
	// already know as an alive peer would otherwise be listed twice).
	self := g.members.Self()
	seen := make(map[string]bool)
	var candidates []string
	addCandidate := func(addr string) {
		if !seen[addr] {
			seen[addr] = true
			candidates = append(candidates, addr)
		}
	}
	for _, node := range g.members.Alive() {
		if node.ID != self.ID {
			addCandidate(node.Addr)
		}
	}
	for _, seed := range g.seeds {
		addCandidate(seed)
	}
	payload, err := marshal(g.members.Snapshot())
	if err != nil {
		return
	}

	for _, addr := range pickPeers(candidates, g.fanout) {
		dst, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			continue
		}
		g.conn.WriteTo(payload, dst)
	}
}
