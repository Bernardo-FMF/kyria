package server

import (
	"log/slog"
	"testing"

	"github.com/Bernardo-FMF/kyria/internal/merkle"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/vclock"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// fakeTreeSyncer is a deterministic stand-in for *Peer's tree/bucket verbs: it returns a canned
// tree and entry set, and counts BucketEntries calls so a test can prove the fetch was skipped.
type fakeTreeSyncer struct {
	tree        *merkle.Tree
	entries     map[string][]byte
	bucketCalls int
}

func (f *fakeTreeSyncer) Tree(addr string, leaves int) (*merkle.Tree, error) {
	return f.tree, nil
}

func (f *fakeTreeSyncer) BucketEntries(addr string, leaves int, buckets []int) (map[string][]byte, error) {
	f.bucketCalls++
	return f.entries, nil
}

// TestAntiEntropy_SyncWithReconciles: local holds a stale version; the peer's tree differs and
// its bucket entry is newer, so syncWith folds it in and the local key catches up.
func TestAntiEntropy_SyncWithReconciles(t *testing.T) {
	const leaves = 64
	s := store.New()
	s.Set("k", verBlob("old", vclock.Clock{"a": 1}))

	// The peer's tree and entry reflect a newer version of k.
	peerTree := merkle.New(leaves)
	peerTree.Add("k", verBlob("new", vclock.Clock{"a": 2}))
	fake := &fakeTreeSyncer{
		tree:    peerTree,
		entries: map[string][]byte{"k": verBlob("new", vclock.Clock{"a": 2})},
	}

	ae := &AntiEntropy{self: "a", store: s, peer: fake, leaves: leaves, logger: slog.Default()}
	ae.syncWith("b")

	blob, ok := s.Get("k")
	if !ok {
		t.Fatal("k missing after sync")
	}
	if vs, _ := version.Decode(blob); len(vs) != 1 || string(vs[0].Value) != "new" {
		t.Errorf("after sync k = %v, want the reconciled [new]", vs)
	}
}

// TestAntiEntropy_SyncWithNoDiffSkipsFetch: when the trees match, syncWith must not fetch any
// bucket entries — proving the Merkle short-circuit (equal roots ⇒ one exchange, then done).
func TestAntiEntropy_SyncWithNoDiffSkipsFetch(t *testing.T) {
	const leaves = 64
	s := store.New()
	s.Set("k", verBlob("v", vclock.Clock{"a": 1}))

	// The peer's tree is identical to what the local store builds.
	peerTree := merkle.New(leaves)
	peerTree.Add("k", verBlob("v", vclock.Clock{"a": 1}))
	fake := &fakeTreeSyncer{tree: peerTree}

	ae := &AntiEntropy{self: "a", store: s, peer: fake, leaves: leaves, logger: slog.Default()}
	ae.syncWith("b")

	if fake.bucketCalls != 0 {
		t.Errorf("matching trees fetched buckets %d times, want 0", fake.bucketCalls)
	}
}
