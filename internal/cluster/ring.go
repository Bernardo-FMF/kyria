package cluster

import (
	"cmp"
	"slices"
	"sort"
	"strconv"
)

// Ring is a consistent-hash ring: it maps each key to an owning node so that when
// nodes join or leave, only a small fraction of keys move, unlike `hash % N`
// (modulo hashing), which remaps almost everything.
// Each node is placed at several points throughout the ring (virtual nodes),
// and a key is owned by the first node clockwise from the key's own hash position.
// Virtual nodes (`replicas` per physical node) even out the load, since without them
// each node would own one big arc and the split would be uneven.
//
// The hash MUST be deterministic across processes (see hashStr) — every node in the
// cluster has to compute the SAME key to node mapping, or they'd disagree on ownership.
// (This is the opposite of the store's shardFor, which uses a random per-process
// maphash seed precisely because that mapping is node-local.)
type Ring struct {
	replicas int
	points   []point // sorted ascending by hash
}

// point is one virtual node: a position on the ring and the physical node there.
type point struct {
	hash uint64
	node string
}

// NewRing returns an empty ring that places replicas virtual points per node.
func NewRing(replicas int) *Ring {
	return &Ring{
		replicas: replicas,
	}
}

// Add places a node on the end of the ring at replicas virtual points.
func (r *Ring) Add(node string) {
	for i := range r.replicas {
		hash := hashStr(node + "#" + strconv.Itoa(i))
		r.points = append(r.points, point{hash: hash, node: node})
	}
}

// Sort orders the nodes point list by hash.
func (r *Ring) Sort() {
	slices.SortFunc(r.points, func(a, b point) int {
		return cmp.Compare(a.hash, b.hash)
	})
}

// Remove drops all of node's virtual points, keeping the remaining points sorted.
func (r *Ring) Remove(node string) {
	r.points = slices.DeleteFunc(r.points, func(p point) bool {
		return p.node == node
	})
}

// Get returns the node that owns key: the first virtual point clockwise from the
// key's hash, wrapping around the ring.
func (r *Ring) Get(key string) (string, bool) {
	if len(r.points) == 0 {
		return "", false
	}

	hash := hashStr(key)

	idx := findIndex(r, hash)
	return r.points[idx].node, true
}

// GetN returns distinct replica set for key: the primary (the node Get returns) plus the
// next n-1 DISTINCT physical nodes clockwise, in that order. It returns fewer than n
// only when the cluster has fewer than n nodes.
func (r *Ring) GetN(key string, n int) []string {
	if len(r.points) == 0 || n <= 0 {
		return nil
	}

	hash := hashStr(key)
	idx := findIndex(r, hash)
	// Walk clockwise from the key's position, taking each physical node the first
	// time we meet it. Virtual nodes repeat the same node around the ring, so the
	// slices.Contains skip dedups the replica set. The (idx+i)%count wraps the
	// walk around the end of the ring.
	count := len(r.points)
	var nodes []string
	for i := range count {
		p := r.points[(idx+i)%count]

		if !slices.Contains(nodes, p.node) {
			nodes = append(nodes, p.node)
		}
		if len(nodes) == n {
			break
		}
	}

	return nodes
}

// findIndex performs a binary search to find the index (clockwise) that the hash belongs
// to. It's a circular structure, so if the index reaches the last point we wrap to the start.
func findIndex(r *Ring, hash uint64) int {
	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= hash
	})

	if idx == len(r.points) { // hashed past the last point; wrap to the first (circular)
		idx = 0
	}
	return idx
}

const (
	fnv1aOffset = 14695981039346656037 // FNV-1a 64-bit offset basis
	fnv1aPrime  = 1099511628211        // FNV-1a 64-bit prime
)

// hashStr returns a deterministic FNV-1a 64-bit hash of s. It allocates nothing and
// uses no random seed, so every node in the cluster hashes a given string identically.
func hashStr(s string) uint64 {
	h := uint64(fnv1aOffset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i]) // XOR in the byte…
		h *= fnv1aPrime   // multiply by the prime
	}
	return h
}
