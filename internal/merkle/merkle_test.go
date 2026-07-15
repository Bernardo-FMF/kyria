package merkle

import (
	"bytes"
	"testing"
)

// build makes a tree and Adds every entry (a [key, value] pair).
func build(leaves int, entries [][2]string) *Tree {
	tr := New(leaves)
	for _, kv := range entries {
		tr.Add(kv[0], []byte(kv[1]))
	}
	return tr
}

// TestMerkle_OrderIndependent: the same entries added in opposite orders produce the
// same root and no diff — leaf hashing must not depend on insertion order (keys come
// from a map, whose iteration order is random).
func TestMerkle_OrderIndependent(t *testing.T) {
	entries := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}}
	forward := build(64, entries)

	reversed := New(64)
	for i := len(entries) - 1; i >= 0; i-- {
		reversed.Add(entries[i][0], []byte(entries[i][1]))
	}

	if !bytes.Equal(forward.Root(), reversed.Root()) {
		t.Error("same entries in a different order gave different roots")
	}
	if d := forward.Diff(reversed); len(d) != 0 {
		t.Errorf("identical trees diff = %v, want none", d)
	}
}

// TestMerkle_EmptyMatch: two empty trees are identical.
func TestMerkle_EmptyMatch(t *testing.T) {
	a, b := New(16), New(16)
	if !bytes.Equal(a.Root(), b.Root()) {
		t.Error("two empty trees have different roots")
	}
	if d := a.Diff(b); len(d) != 0 {
		t.Errorf("empty trees diff = %v, want none", d)
	}
}

// TestMerkle_ValueDifferenceIsolated: trees identical except one key's blob → the root
// changes and Diff names exactly that key's bucket (proving the walk prunes the matching
// subtrees instead of returning every leaf).
func TestMerkle_ValueDifferenceIsolated(t *testing.T) {
	entries := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}}
	a := build(64, entries)

	changed := make([][2]string, len(entries))
	copy(changed, entries)
	changed[2][1] = "CHANGED" // key "c"
	b := build(64, changed)

	if bytes.Equal(a.Root(), b.Root()) {
		t.Fatal("changing a value did not change the root")
	}
	d := a.Diff(b)
	if len(d) != 1 || d[0] != a.Bucket("c") {
		t.Errorf("diff = %v, want exactly [bucket(c)=%d]", d, a.Bucket("c"))
	}
}

// TestMerkle_MissingKey: a key present on one side only surfaces as its bucket in Diff.
func TestMerkle_MissingKey(t *testing.T) {
	full := build(64, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	missing := build(64, [][2]string{{"a", "1"}, {"b", "2"}}) // no "c"

	d := full.Diff(missing)
	if len(d) != 1 || d[0] != full.Bucket("c") {
		t.Errorf("diff = %v, want [bucket(c)=%d]", d, full.Bucket("c"))
	}
}

// TestMerkle_MultipleDiffs: two independent differences both surface — as the SET of
// their buckets, collapsing to one entry if the two keys happen to share a bucket.
func TestMerkle_MultipleDiffs(t *testing.T) {
	base := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}}
	a := build(64, base)

	changed := make([][2]string, len(base))
	copy(changed, base)
	changed[0][1] = "X" // key "a"
	changed[4][1] = "Y" // key "e"
	b := build(64, changed)

	want := map[int]bool{a.Bucket("a"): true, a.Bucket("e"): true}
	got := a.Diff(b)
	if len(got) != len(want) {
		t.Fatalf("diff = %v, want the buckets of a and e (%v)", got, want)
	}
	for _, bucket := range got {
		if !want[bucket] {
			t.Errorf("diff has unexpected bucket %d, want one of %v", bucket, want)
		}
	}
}

// TestMerkle_LeafAccumulatesMultipleKeys: with a single leaf, every key lands in the
// same bucket, so the leaf must XOR-ACCUMULATE all of them rather than overwrite with
// the last one added. Adding the same keys in a different order must still match, and
// dropping one must surface as a diff at bucket 0.
func TestMerkle_LeafAccumulatesMultipleKeys(t *testing.T) {
	entries := [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}}

	forward := build(1, entries) // New(1) → one leaf → all keys collide in bucket 0

	reversed := New(1)
	for i := len(entries) - 1; i >= 0; i-- {
		reversed.Add(entries[i][0], []byte(entries[i][1]))
	}

	// Overwrite would leave each tree holding only its last-added key → different roots.
	if !bytes.Equal(forward.Root(), reversed.Root()) {
		t.Error("same keys in one bucket, different add order, gave different roots — leaf overwrites instead of accumulating")
	}
	if d := forward.Diff(reversed); len(d) != 0 {
		t.Errorf("diff of identical bucket contents = %v, want none", d)
	}

	// Dropping a key changes the bucket's XOR → a diff at bucket 0.
	fewer := build(1, entries[:2]) // only a, b
	if d := forward.Diff(fewer); len(d) != 1 || d[0] != 0 {
		t.Errorf("diff after dropping a key = %v, want [0]", d)
	}
}

// TestMerkle_KeyBlobBoundary: (key="ab", blob="c") and (key="abc", blob="") are different
// entries, but their key+blob byte streams are both "abc". Without a delimiter between key
// and blob they hash identically, so the two trees would look equal and Diff would miss a
// real difference. New(1) forces both into the same bucket to isolate the aliasing.
// (Currently RED — passes once Add hashes the key at a fixed width before the blob.)
func TestMerkle_KeyBlobBoundary(t *testing.T) {
	a := New(1)
	a.Add("ab", []byte("c"))

	b := New(1)
	b.Add("abc", []byte(""))

	if d := a.Diff(b); len(d) != 1 || d[0] != 0 {
		t.Errorf("diff = %v, want [0] — (ab,c) and (abc,\"\") are different data", d)
	}
}

// TestMerkle_BucketStableAndInRange: Bucket is deterministic and within [0, Leaves()),
// and New rounds the leaf count up to a power of two.
func TestMerkle_BucketStableAndInRange(t *testing.T) {
	tr := New(1000)
	if tr.Leaves() != 1024 {
		t.Errorf("Leaves() = %d, want 1024 (1000 rounded up to a power of two)", tr.Leaves())
	}
	for _, k := range []string{"a", "hello", "key-42", ""} {
		if b1, b2 := tr.Bucket(k), tr.Bucket(k); b1 != b2 {
			t.Errorf("Bucket(%q) not stable: %d then %d", k, b1, b2)
		}
		if b := tr.Bucket(k); b < 0 || b >= tr.Leaves() {
			t.Errorf("Bucket(%q) = %d, out of range [0,%d)", k, b, tr.Leaves())
		}
	}
}
