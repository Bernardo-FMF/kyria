package server

import (
	"strconv"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/merkle"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// TestHandle_RTree: RTREE builds a Merkle tree over the local store and returns it encoded;
// the served tree must match one built independently over the same entries.
func TestHandle_RTree(t *testing.T) {
	s := store.New()
	s.Set("a", versionBlob("1"))
	s.Set("b", versionBlob("2"))
	h := NewHandler(s, nil, nil, nil)

	const leaves = 64
	reply := h.Handle(protocol.Command{Name: "RTREE", Args: [][]byte{[]byte(strconv.Itoa(leaves))}})

	blob, ok := reply.AsBulk()
	if !ok {
		t.Fatalf("RTREE reply is not a bulk string, got %q", encodeReply(t, reply))
	}
	got, err := merkle.Decode(blob)
	if err != nil {
		t.Fatalf("Decode RTREE tree: %v", err)
	}

	// A tree built independently over the same entries must be identical.
	want := merkle.New(leaves)
	want.Add("a", versionBlob("1"))
	want.Add("b", versionBlob("2"))
	if d := want.Diff(got); len(d) != 0 {
		t.Errorf("served tree differs from an independently-built one at buckets %v", d)
	}
}

// TestPeer_Tree: the client fetches a peer's tree over a socket — seed via RSET, then RTREE,
// and the decoded tree matches the seeded entry.
func TestPeer_Tree(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	if err := peer.rsetTest(addr, "k", "v"); err != nil {
		t.Fatalf("seed via RSET: %v", err)
	}

	const leaves = 64
	got, err := peer.Tree(addr, leaves)
	if err != nil {
		t.Fatalf("Tree: %v", err)
	}

	want := merkle.New(leaves)
	want.Add("k", versionBlob("v"))
	if d := want.Diff(got); len(d) != 0 {
		t.Errorf("fetched tree differs from the seeded entry at buckets %v", d)
	}
}
