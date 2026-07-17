package server

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/merkle"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// TestHandle_RBucket: RBUCKET returns the store's entries whose bucket is in the requested
// set, and nothing from other buckets. Requesting only "a"'s bucket must yield "a" (with its
// stored value) and no key that maps elsewhere.
func TestHandle_RBucket(t *testing.T) {
	const leaves = 64
	s := store.New()
	s.Set("a", versionBlob("1"))
	s.Set("b", versionBlob("2"))
	s.Set("c", versionBlob("3"))
	h := NewHandler(s, nil, nil, nil, nil)

	bucketer := merkle.New(leaves)
	want := bucketer.Bucket("a")

	req := [][]byte{[]byte(strconv.Itoa(leaves)), encodeBuckets([]int{want})}
	reply := h.Handle(protocol.Command{Name: "RBUCKET", Args: req})

	blob, ok := reply.AsBulk()
	if !ok {
		t.Fatalf("RBUCKET reply is not a bulk string, got %q", encodeReply(t, reply))
	}
	entries, err := decodeEntries(blob)
	if err != nil {
		t.Fatalf("decodeEntries: %v", err)
	}

	// "a" must be present with its stored value.
	if got, present := entries["a"]; !present || !bytes.Equal(got, versionBlob("1")) {
		t.Errorf("entries[a] = %v (present %v), want the stored blob", got, present)
	}
	// No key outside the requested bucket may leak in.
	for key := range entries {
		if bucketer.Bucket(key) != want {
			t.Errorf("entry %q is in bucket %d, not the requested %d", key, bucketer.Bucket(key), want)
		}
	}
}

// TestPeer_BucketEntries: the client fetches a bucket's entries over a socket — seed via RSET,
// then RBUCKET the seeded key's bucket, and the decoded entry matches.
func TestPeer_BucketEntries(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	if err := peer.rsetTest(addr, "k", "v"); err != nil {
		t.Fatalf("seed via RSET: %v", err)
	}

	const leaves = 64
	bucket := merkle.New(leaves).Bucket("k")
	entries, err := peer.BucketEntries(addr, leaves, []int{bucket})
	if err != nil {
		t.Fatalf("BucketEntries: %v", err)
	}

	blob, ok := entries["k"]
	if !ok {
		t.Fatalf("k not returned, got %v", entries)
	}
	if vs, err := version.Decode(blob); err != nil || len(vs) != 1 || string(vs[0].Value) != "v" {
		t.Errorf("decoded entry = %v (err %v), want [v]", vs, err)
	}
}
