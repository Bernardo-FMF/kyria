package cluster

import (
	"sync"
	"sync/atomic"
	"time"
)

// Router maps keys to the node that owns them by keeping a consistent-hash Ring in
// sync with the gossiped membership. It bridges the two halves of the cluster:
// Members says who is alive, the Ring says who owns what. A background goroutine
// rebuilds the ring from Members.Alive() every interval and swaps it in atomically,
// so the request-path reads (Owner/IsLocal) are lock-free and never see a
// half-built ring.
type Router struct {
	self     string        // this node's ID (from Members.Self)
	members  *Members      // the live membership view
	replicas int           // virtual nodes per physical node
	interval time.Duration // how often to rebuild the ring from Alive()

	// ring holds the current consistent-hash ring. rebuild() swaps in a brand-new
	// ring with a single atomic Store, so request-path reads (Owner) are lock-free
	// and never see a half-built ring — an immutable value published by pointer swap.
	ring atomic.Pointer[Ring]

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

// NewRouter returns a Router for members, with replicas virtual nodes per node and
// a rebuild every interval. It builds the initial ring immediately, so Owner works
// before (and without) Start — handy for a single-node setup.
func NewRouter(members *Members, replicas int, interval time.Duration) *Router {
	router := &Router{
		self:     members.self,
		members:  members,
		replicas: replicas,
		interval: interval,
		stop:     make(chan struct{}),
	}
	router.rebuild()
	return router
}

// rebuild constructs a fresh ring from the current alive membership and publishes it
// with a single atomic Store. Building a brand-new ring (rather than mutating the
// live one) is what lets readers stay lock-free — see the note at the bottom.
func (r *Router) rebuild() {
	ring := NewRing(r.replicas)
	for _, node := range r.members.Alive() {
		ring.Add(node.ID)
	}
	ring.Sort()

	r.ring.Store(ring)
}

// Start launches the background loop that rebuilds the ring every interval, until
// Stop. Same lifecycle shape as the janitor and gossiper.
func (r *Router) Start() {
	r.wg.Go(func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.rebuild()
			case <-r.stop:
				return
			}
		}
	})
}

// Stop ends the background rebuild loop and waits for it to exit. Safe to call more
// than once.
func (r *Router) Stop() {
	r.stopOnce.Do(func() { close(r.stop) })
	r.wg.Wait()
}

// Owner returns the ID of the node that owns key, and false if the cluster is empty.
// The ring read is lock-free — an atomic Load of the current ring.
func (r *Router) Owner(key string) (string, bool) {
	return r.ring.Load().Get(key)
}

// IsLocal reports whether this node is the owner of key.
func (r *Router) IsLocal(key string) bool {
	owner, ok := r.Owner(key)
	return ok && owner == r.self
}
