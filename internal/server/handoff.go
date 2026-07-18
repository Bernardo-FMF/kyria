package server

import (
	"bytes"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Hinted handoff keeps a write durable when one of its N target replicas is
// temporarily unreachable. When the coordinator fans a write out and a replica
// doesn't ack, that replica's copy would otherwise be lost: the write can still meet
// W (so the client sees +OK), but the cluster then holds fewer than N copies, and a
// later read of the key could miss it until a repair happens.
//
// The fix is to PARK the missed write as a "hint" — a note that node X still owes this
// write. A background replayer periodically retries delivering parked hints to their
// intended nodes; once a node is reachable again the hint lands and is dropped, so the
// write reaches N copies eventually without ever blocking the client on the down node.

// hintStore holds writes that couldn't be delivered, keyed target -> key -> latest
// blob. Keying by key (rather than a flat list) collapses duplicates: if a key is
// written several times while a replica is down, only the latest versioned blob
// matters, since a newer write supersedes the parked one and rset reconciles on
// arrival — which also bounds how much a single down node can pile up. Every method
// takes the lock, and none does network I/O; that's the replayer's job.
type hintStore struct {
	mu    sync.Mutex
	hints map[string]map[string][]byte
}

// NewHintStore returns an empty hint store. It is exported so main can build one and
// hand the same instance to both the coordinator (which parks hints) and the replayer
// (which drains them).
func NewHintStore() *hintStore {
	return &hintStore{
		hints: make(map[string]map[string][]byte),
	}
}

// add parks blob for key under target, creating the inner map on the first hint for a
// target. An existing entry is overwritten — the latest write wins.
func (h *hintStore) add(target, key string, blob []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	outer := h.hints[target]
	if outer == nil {
		outer = make(map[string][]byte)
		h.hints[target] = outer
	}
	outer[key] = blob
}

// snapshot returns a copy of the parked hints — fresh outer and inner maps, sharing the
// (immutable) blob slices. The replayer iterates the copy and calls Replicate on each
// entry, so it must not hold the store's lock across a network round-trip: a slow or
// dead peer would otherwise freeze every writer trying to park a hint.
func (h *hintStore) snapshot() map[string]map[string][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()

	hints := make(map[string]map[string][]byte, len(h.hints))

	for oKey, oValue := range h.hints {
		keys := make(map[string][]byte, len(oValue))

		for iKey, iValue := range oValue {
			keys[iKey] = iValue
		}

		hints[oKey] = keys
	}

	return hints
}

// remove drops the hint for (target, key) only if the currently-parked blob still
// equals delivered. Between the replayer's snapshot and this call a newer add may have
// replaced the blob; removing unconditionally would silently drop that undelivered
// write, so a mismatch leaves the newer hint parked for the next tick. An emptied
// target map is deleted so a recovered node leaves nothing behind.
func (h *hintStore) remove(target, key string, delivered []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	keys := h.hints[target]
	if keys == nil {
		return
	}
	if !bytes.Equal(keys[key], delivered) {
		return // absent, or a newer add() superseded it — leave it parked
	}
	delete(keys, key)
	if len(keys) == 0 {
		delete(h.hints, target)
	}
}

// Size reports the total number of parked hints across all targets — the handoff backlog. A rising
// value means a replica has been unreachable for a while, which is why it is exported: main registers
// it as the hints_pending gauge.
func (h *hintStore) Size() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	totalHints := 0
	for _, inner := range h.hints {
		totalHints += len(inner)
	}

	return totalHints
}

// ── The replayer ─────────────────────────────────────────────────────────────

// HintReplayer is a background goroutine that periodically retries delivering parked
// hints to their targets, draining the hint store as nodes recover. It owns a
// goroutine, so its lifetime must be managed: NewHintReplayer starts it and Stop shuts
// it down. Failing to call Stop leaks the goroutine.
type HintReplayer struct {
	store      *hintStore
	replicator replicator
	interval   time.Duration
	logger     *slog.Logger
	stop       chan struct{} // closed by Stop to tell run to exit
	done       chan struct{} // closed by run once it has exited
	stopOnce   sync.Once     // guards close(stop) so Stop is idempotent
}

// NewHintReplayer starts a goroutine that replays parked hints from store over
// replicator every interval, and returns a handle. The caller must call Stop to release
// the goroutine. It is exported so main can hold the handle and Stop it on shutdown.
// A nil logger falls back to slog.Default().
func NewHintReplayer(store *hintStore, replicator replicator, interval time.Duration, logger *slog.Logger) *HintReplayer {
	if logger == nil {
		logger = slog.Default()
	}

	h := &HintReplayer{
		store:      store,
		replicator: replicator,
		interval:   interval,
		logger:     logger.With("component", "handoff"),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}

	go h.run()

	return h
}

// run is the replay loop: sweep on every tick until stop is closed. It closes done on
// exit so Stop can wait for it, and stops the ticker so it does not leak.
func (h *HintReplayer) run() {
	defer close(h.done)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.replayOnce()
		case <-h.stop:
			return
		}
	}
}

// replayOnce attempts to deliver every parked hint once, concurrently (a goroutine per
// key/blob), and returns how many landed. A successful Replicate removes the hint — a
// conditional remove, so a newer parked write isn't clobbered — and a failure leaves it
// for the next tick. It snapshots first so the fan-out never holds the store's lock, and
// waits for all deliveries before returning, so it stays synchronous for callers.
func (h *HintReplayer) replayOnce() int {
	snaphot := h.store.snapshot()

	var delivered atomic.Int64
	var failed atomic.Int64
	var wg sync.WaitGroup

	for target, hintMap := range snaphot {
		for key, blob := range hintMap {
			wg.Add(1)
			go func(target string, key string, blob []byte) {
				defer wg.Done()

				err := h.replicator.Replicate(target, rset, [][]byte{[]byte(key), blob})
				if err == nil {
					h.store.remove(target, key, blob)
					delivered.Add(1)
					return
				}
				failed.Add(1)
			}(target, key, blob)
		}
	}

	wg.Wait()

	d := int(delivered.Load())
	if f := failed.Load(); f > 0 {
		h.logger.Warn("hints undelivered", "failed", f, "delivered", d, "pending", h.store.Size())
	} else if d > 0 {
		h.logger.Info("hints delivered", "delivered", d, "pending", h.store.Size())
	}

	return d
}

// Stop shuts the replayer down and blocks until its goroutine has exited, so no sweep
// runs after Stop returns. It is safe to call any number of times: sync.Once guards the
// close (closing a closed channel panics), and the done receive is safe to repeat.
func (h *HintReplayer) Stop() {
	h.stopOnce.Do(func() { close(h.stop) })
	<-h.done
}
