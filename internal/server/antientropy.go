package server

import (
	"bytes"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/binenc"
	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/merkle"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/version"
)

// Anti-entropy exchange: a node diffs its Merkle tree against a peer's, then fetches the
// (key, blob) entries in the differing buckets to reconcile. This file holds the RBUCKET wire
// codecs for that exchange (request = a set of bucket numbers, reply = the entries in them; the
// decoders parse defensively, binenc.ErrMalformed) and the AntiEntropy loop that drives it.

// encodeBuckets serializes a set of bucket numbers (a Merkle Diff's output) for an RBUCKET
// request: a uint32 count followed by one uint32 per bucket.
func encodeBuckets(buckets []int) []byte {
	buf := new(bytes.Buffer)
	binenc.PutUint32(buf, uint32(len(buckets)))
	for _, b := range buckets {
		binenc.PutUint32(buf, uint32(b))
	}
	return buf.Bytes()
}

// decodeBuckets parses encodeBuckets' bytes back into bucket numbers, defensively — the blob
// arrives over the network, so a short or corrupt one yields binenc.ErrMalformed.
func decodeBuckets(b []byte) ([]int, error) {
	bucketSize, cursor, err := binenc.Uint32(b, 0)
	if err != nil {
		return nil, err
	}

	buckets := make([]int, 0, bucketSize)
	var bucket uint32
	for range bucketSize {
		bucket, cursor, err = binenc.Uint32(b, cursor)
		if err != nil {
			return nil, err
		}

		buckets = append(buckets, int(bucket))
	}

	return buckets, nil
}

// encodeEntries serializes the (key, blob) pairs an RBUCKET reply carries: a uint32 count,
// then per entry a length-prefixed key, a uint32 blob length, and the blob bytes.
func encodeEntries(entries map[string][]byte) []byte {
	buf := new(bytes.Buffer)
	binenc.PutUint32(buf, uint32(len(entries)))
	for key, blob := range entries {
		binenc.PutString(buf, key)
		binenc.PutUint32(buf, uint32(len(blob)))
		buf.Write(blob)
	}

	return buf.Bytes()
}

// decodeEntries parses encodeEntries' bytes back into a map, defensively. An empty input (no
// entries) is a valid empty map; a short or corrupt blob yields binenc.ErrMalformed.
func decodeEntries(b []byte) (map[string][]byte, error) {
	entriesSize, cursor, err := binenc.Uint32(b, 0)
	if err != nil {
		return nil, err
	}

	entries := make(map[string][]byte, entriesSize)

	var key string
	var blobLen uint32
	var blob []byte
	for range entriesSize {
		key, cursor, err = binenc.String(b, cursor)
		if err != nil {
			return nil, err
		}
		blobLen, cursor, err = binenc.Uint32(b, cursor)
		if err != nil {
			return nil, err
		}
		blob, cursor, err = binenc.Bytes(b, cursor, int(blobLen))
		if err != nil {
			return nil, err
		}

		entries[key] = blob
	}

	return entries, nil
}

// ── The anti-entropy loop ────────────────────────────────────────────────────

// treeSyncer is the peer surface the loop needs: fetch a peer's Merkle tree, then its entries in a
// set of buckets. *Peer satisfies it; the interface keeps the loop testable against a fake.
type treeSyncer interface {
	Tree(addr string, leaves int) (*merkle.Tree, error)
	BucketEntries(addr string, leaves int, buckets []int) (map[string][]byte, error)
}

// AntiEntropy is a background goroutine that periodically reconciles this node's store with a
// random live peer: it Merkle-diffs the two stores and pulls the entries in the buckets that
// differ, folding them in with the same reconcile read-repair uses. It owns a goroutine, so
// NewAntiEntropy starts it and Stop shuts it down; failing to Stop leaks it.
//
// The tree covers the WHOLE store — exact when every node replicates every key (N = node count),
// coarse otherwise (Dynamo scopes a tree per key range and syncs it with that range's co-replicas),
// and there is no ownership filter on what gets reconciled in. Both are deliberate simplifications.
type AntiEntropy struct {
	self     string
	store    store.Store
	peer     treeSyncer
	members  *cluster.Members
	leaves   int
	interval time.Duration
	stop     chan struct{} // closed by Stop to tell run to exit
	done     chan struct{} // closed by run once it has exited
	stopOnce sync.Once     // guards close(stop) so Stop is idempotent
}

// NewAntiEntropy starts the reconcile loop against peers drawn from members, using leaves as the
// (cluster-fixed) Merkle leaf count and syncing every interval; self is excluded from peer
// selection. The caller must Stop it or the goroutine leaks.
func NewAntiEntropy(self string, store store.Store, peer treeSyncer, members *cluster.Members, leaves int, interval time.Duration) *AntiEntropy {
	a := &AntiEntropy{
		self:     self,
		store:    store,
		peer:     peer,
		members:  members,
		leaves:   leaves,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	go a.run()

	return a
}

// run is the sync loop: on each tick, pick a live peer (skipping the tick when there's no other
// node) and sync with it, until stop is closed. It closes done on exit and stops the ticker.
func (a *AntiEntropy) run() {
	defer close(a.done)

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p, ok := a.pickPeer()
			if ok {
				a.syncWith(p)
			}
		case <-a.stop:
			return
		}
	}
}

// pickPeer returns a uniformly random live node other than self, or ok=false when this node is the
// only live one. Reservoir sampling — one pass over members.Alive(), no filtered slice allocated.
func (a *AntiEntropy) pickPeer() (string, bool) {
	chosen := ""
	count := 0
	for _, n := range a.members.Alive() {
		if n.ID == a.self {
			continue
		}
		count++
		if rand.IntN(count) == 0 {
			chosen = n.ID
		}
	}
	return chosen, count > 0
}

// syncWith reconciles this node's store with the peer at addr: it builds the local Merkle tree,
// fetches the peer's, and Diffs them. Equal trees mean nothing to do — one exchange and done. For
// the buckets that differ it fetches the peer's entries and folds each version into the local store
// (proactive read-repair: a stale local key catches up, a locally-newer one is untouched, and
// concurrent writes become siblings). Best-effort — a peer error just ends this round.
func (a *AntiEntropy) syncWith(addr string) {
	t := merkle.New(a.leaves)
	a.store.Range(func(k string, v []byte) { t.Add(k, v) })
	pt, err := a.peer.Tree(addr, a.leaves)
	if err != nil {
		return
	}

	diffs := t.Diff(pt)
	if len(diffs) == 0 {
		return
	}

	entries, err := a.peer.BucketEntries(addr, a.leaves, diffs)

	for key, blob := range entries {
		a.store.Update(key, func(old []byte) []byte {
			existing, _ := version.Decode(old)
			incoming, _ := version.Decode(blob)

			for _, v := range incoming {
				existing = version.Reconcile(existing, v)
			}
			return version.Encode(existing)
		})
	}
}

// Stop shuts the loop down and blocks until its goroutine has exited, so no sync runs after Stop
// returns. Safe to call any number of times.
func (a *AntiEntropy) Stop() {
	a.stopOnce.Do(func() { close(a.stop) })
	<-a.done
}
