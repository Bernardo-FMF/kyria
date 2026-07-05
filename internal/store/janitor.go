package store

import (
	"sync"
	"time"
)

// ── The synchronous sweep ────────────────────────────────────────────────────

// reaper is the behaviour a Janitor drives: remove entries that have expired as
// of now, returning how many were removed. It is unexported, but a *ShardedStore
// passed to NewJanitor from another package still satisfies it.
type reaper interface {
	reapExpired(now time.Time) int
}

// reapExpired deletes every entry that has expired as of now and returns the
// number removed. It does no locking of its own — MapStore is not safe for
// concurrent use, so a caller (a shard) must hold the lock.
func (m *MapStore) reapExpired(now time.Time) int {
	removed := 0

	for k, e := range m.data {
		if e.expired(now) {
			delete(m.data, k)
			removed++
		}
	}

	return removed
}

// reapExpired sweeps every shard, deleting expired entries and returning the
// total removed. Reaping is a write, so each shard is taken under its exclusive
// lock, one shard at a time.
func (s *ShardedStore) reapExpired(now time.Time) int {
	removed := 0

	for _, shard := range s.shards {
		shard.mu.Lock()

		removed += shard.store.reapExpired(now)

		shard.mu.Unlock()
	}

	return removed
}

// Compile-time assertions that both stores satisfy reaper.
var (
	_ reaper = (*MapStore)(nil)
	_ reaper = (*ShardedStore)(nil)
)

// ── The asynchronous machinery ───────────────────────────────────────────────

// Janitor periodically drives a reaper to reclaim expired entries.
//
// Lazy expiry (in Get) hides expired entries but never frees their memory, so a
// key written once with a TTL and never read again would linger forever. The
// Janitor is the active counterpart: a background goroutine that sweeps on a
// fixed interval and deletes whatever has expired.
//
// A Janitor owns a goroutine, so its lifetime must be managed: NewJanitor starts
// it and Stop shuts it down. Failing to call Stop leaks the goroutine.
type Janitor struct {
	reaper   reaper
	interval time.Duration
	stop     chan struct{} // closed by Stop to tell run to exit
	done     chan struct{} // closed by run once it has exited
	stopOnce sync.Once     // guards close(stop) so Stop is idempotent
}

// NewJanitor starts a background goroutine that reaps expired entries from r
// every interval, and returns a handle to it. The caller must call Stop to
// release the goroutine.
func NewJanitor(r reaper, interval time.Duration) *Janitor {
	j := &Janitor{
		reaper:   r,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	go j.run()

	return j
}

// run is the sweep loop: reap on every tick until stop is closed. It closes done
// on exit so Stop can wait for it, and stops the ticker so it does not leak.
func (j *Janitor) run() {
	defer close(j.done)

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			j.reaper.reapExpired(time.Now())
		case <-j.stop:
			return
		}
	}
}

// Stop shuts the janitor down and blocks until its goroutine has exited, so no
// further reap can run once Stop returns. It is safe to call any number of times.
//
// The stop/done channels form a shutdown handshake: closing stop signals run to
// exit (a close, not a send, so the case stays ready), and receiving from done
// waits for run to acknowledge by closing it. sync.Once guards the close, since
// closing an already-closed channel panics; the done receive is safe to repeat
// because a closed channel is always ready.
func (j *Janitor) Stop() {
	j.stopOnce.Do(func() { close(j.stop) })
	<-j.done
}
