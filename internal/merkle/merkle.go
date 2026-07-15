// Package merkle builds Merkle trees for anti-entropy — the periodic background repair
// that keeps replicas converged even for the divergence read-repair and hinted handoff
// miss: keys that are never read (so read-repair never fires) and writes lost when a
// coordinator crashed with hints still in memory.
//
// Two replicas that own the same keys each build a tree over their (key, blob) pairs and
// compare. Equal roots ⇒ identical data, proven by exchanging ONE hash. Differing roots
// are walked top-down, descending only into subtrees whose hashes differ, so the data
// exchanged is proportional to the number of DIFFERENCES, not the dataset size. The
// differing leaves name the buckets of keys the caller must then reconcile.
package merkle

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
)

// Tree is a Merkle tree over a set of (key, blob) pairs, laid out as a perfect binary
// tree in one flat array (heap layout: root at index 1, node i's children at 2i and 2i+1,
// the leaves at [leafCount, 2*leafCount), index 0 unused).
// Keys are bucketed into a fixed, power-of-two number of leaves, so many keys share a leaf.
type Tree struct {
	leafCount int
	nodes     [][]byte
}

// New returns an empty tree, rounding the leaf count up to a power of two so any size
// yields a perfect binary tree - where every level is completely full, and every internal
// node has 2 children.
func New(leaves int) *Tree {
	roundedLeafCount := nextPowerOfTwo(leaves)
	return &Tree{
		leafCount: roundedLeafCount,
		nodes:     make([][]byte, roundedLeafCount*2),
	}
}

// Leaves reports the rounded leaf count, so a caller can size a matching tree and range
// over bucket indices.
func (t *Tree) Leaves() int {
	return t.leafCount
}

// Bucket returns the leaf index a key maps to, in [0, leafCount).
// It uses sha256 to turn the key into a hash.
// The first 8 bytes of the key's sha256 are read as a deterministic integer derived from
// the key. To reduce this value into a bucket, we take x % L, with x the value and L the
// leaf count.
// The modulo is equivalent to a bitwise mask, x & (L-1), because L is a power of two, so
// L-1 has all of its lower log2(L) bits set to 1.
// If L = 8, then L - 1 = 7 => 0111;
// ANDing with this mask keeps only the lower bits, and for a power-of-two modulus those
// bits are exactly the remainder.
func (t *Tree) Bucket(key string) int {
	sum := sha256.Sum256([]byte(key))
	return int(binary.BigEndian.Uint64(sum[:8]) & uint64(t.leafCount-1))
}

// Add folds (key, blob) into its leaf. The entry hash is sha256 over the key's own sha256
// (a fixed 32 bytes) followed by the blob — hashing the key at a fixed width delimits it
// from the blob, so distinct entries can't alias by concatenation ("ab"+"c" vs "a"+"bc").
// That digest is XOR-accumulated into the leaf; XOR is commutative and associative, so a
// bucket's hash depends only on the SET of entries, not the order Add saw them — essential,
// since keys come from a map and two replicas add a bucket's keys in different orders yet
// must reach the same leaf hash. The leaf sits at leafCount+Bucket(key); Root rebuilds the
// internal nodes from these leaves.
func (t *Tree) Add(key string, blob []byte) {
	hasher := sha256.New()
	keyHash := sha256.Sum256([]byte(key))

	hasher.Write(keyHash[:])
	hasher.Write(blob)
	fullHash := hasher.Sum(nil)

	leafIndex := t.leafCount + t.Bucket(key)

	leaf := t.nodes[leafIndex]
	if leaf == nil {
		leaf = make([]byte, 32)
		t.nodes[leafIndex] = leaf
	}
	for i := range 32 {
		leaf[i] ^= fullHash[i]
	}
}

// Root builds the internal nodes from the leaves.
// The node i is equal to sha256(child 2i concatenated with child 2i+1).
// Iterating through the internal nodes starting from the end, guarantees that both children
// are processed before their parent node.
// Add only writes leaves, so Root must calculate all internal nodes.
// Index 1 corresponds to the root of the tree.
func (t *Tree) Root() []byte {
	for i := t.leafCount - 1; i >= 1; i-- {
		hasher := sha256.New()
		hasher.Write(t.nodes[2*i])
		hasher.Write(t.nodes[(2*i)+1])
		t.nodes[i] = hasher.Sum(nil)
	}
	return t.nodes[1]
}

// Diff returns an array of bucket numbers that differ in the trees.
// If the number of leaves differ we already know the trees are not comparable.
//
// Afterwards we need to call Root to build the tree internal nodes,
// as the leaf nodes were already created by calls to Add
func (t *Tree) Diff(other *Tree) []int {
	if t.leafCount != other.leafCount {
		return nil
	}

	t.Root()
	other.Root()

	var diffs []int
	t.collect(other, 1, &diffs)

	return diffs
}

// The algorithm to collect the differences is recursive:
//   - The start index is 1 because 0 is always unused in merkle trees
//   - 1. If the hash of the nodes being evaluated is equal, we return because
//     it means all children nodes will be identical in both trees
//   - 2. Check if the index surpasses the leafCount (meaning the node at that index is a leaf)
//   - 2.1 If the index is >= than the leafCount -> node is a leaf:
//     -- At this point the hash differs and we've reached the finest granularity,
//     since a leaf has no children nodes to descend into, so this leaf is a terminal
//     answer and we can collect it into our difference collection.
//     -- What we collect is the index - leafCount, because we record the bucket number and not
//     the array index. This is because Add writes to nodes[leafCount + Bucket(key)] so to get
//     the bucket we invert the operation.
//   - 2.2 If the index is < than the leafCount -> node is internal:
//     -- The hash still differs, but the difference is deeper in the tree, so recursively
//     traverse into the children.
func (t *Tree) collect(other *Tree, start int, diffs *[]int) {
	if bytes.Equal(t.nodes[start], other.nodes[start]) {
		return
	}

	if start >= t.leafCount {
		*diffs = append(*diffs, start-t.leafCount)
		return
	}

	t.collect(other, 2*start, diffs)
	t.collect(other, (2*start)+1, diffs)
}

// nextPowerOfTwo returns the smallest power of two >= n (and 1 for n <= 1).
func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
