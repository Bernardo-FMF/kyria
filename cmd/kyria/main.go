// Command kyria runs the cache server: it builds a concurrency-safe store, wraps
// it in the RESP/TCP server, and serves until interrupted, shutting down cleanly.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/server"
	"github.com/Bernardo-FMF/kyria/internal/store"
	"github.com/Bernardo-FMF/kyria/internal/telemetry"
)

// antiEntropyLeaves is the Merkle tree leaf count used for anti-entropy. It's a constant, not a
// flag, because every node must use the same value or their trees can't be compared.
const antiEntropyLeaves = 1024

// main parses configuration, builds a concurrency-safe store, and serves RESP
// over TCP until a shutdown signal (SIGINT/SIGTERM) arrives — at which point it
// closes the server and drains in-flight connections before exiting. Serve runs
// in a goroutine so main can wait on either the signal or Serve failing on its
// own; the buffered serveErr channel lets that goroutine exit cleanly either way.
func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	st := store.NewSharded(cfg.Shards, cfg.storeOptions()...)

	var janitor *store.Janitor
	if cfg.ReapInterval > 0 {
		janitor = store.NewJanitor(st, cfg.ReapInterval) // starts the reap goroutine
	}

	// Clustering is opt-in: only with -gossip-addr do we join a cluster and build
	// the router. members and router stay nil in standalone mode, which turns off
	// gossip and request routing respectively.
	var members *cluster.Members
	var gossiper *cluster.Gossiper
	var router *cluster.Router
	// peer and coordinator make up the replication layer; both stay nil in standalone
	// mode, which leaves NewServer with a nil coordinator (replication off).
	var peer *server.Peer
	var coordinator *server.Coordinator
	// replayer, antiEntropy, and tombstoneGC are the background convergence loops; all stay nil in
	// standalone mode and are Stop()ed in the shutdown block. replayer and antiEntropy run over
	// `peer`; tombstoneGC runs over the local store, reaping tombstones once they age past grace.
	var replayer *server.HintReplayer
	var antiEntropy *server.AntiEntropy
	var tombstoneGC *server.TombstoneGC

	tel := telemetry.New(server.ClientCommands...)
	tel.RegisterGauge("store_keys", func() int64 { return int64(st.Size()) })
	tel.RegisterEvents(server.ReplicationEvents)

	if cfg.GossipAddr != "" {
		conn, err := net.ListenPacket("udp", cfg.GossipAddr)
		if err != nil {
			log.Fatalf("failed to bind gossip address %s: %v", cfg.GossipAddr, err)
		}
		addr := conn.LocalAddr().String()
		// The node ID is the CLIENT (TCP) address, not the gossip addr: the ring
		// keys on IDs, and an ID is what goes into a -MOVED reply, so a redirected
		// client can reconnect to the owner. Addr stays the gossip UDP addr peers
		// reach us on. NB: cfg.Addr must be routable and unique per node (e.g.
		// 127.0.0.1:7001) — ":6379" alone won't route a client anywhere.
		self := cluster.Node{ID: cfg.Addr, Addr: addr, State: cluster.Alive, Incarnation: 1}
		members = cluster.NewMembers(self)
		gossiper = cluster.NewGossiper(members, conn, cfg.gossiperOptions()...)
		gossiper.Start()

		router = cluster.NewRouter(members, cfg.Replicas, cfg.RebuildInterval)
		router.Start()

		// The replica set is talked to over the client port, so the coordinator's
		// "self" is this node's ID (its client address), matching what the ring returns.
		peer = server.NewPeer(cfg.ReplicaTimeout)
		// One hint store is shared by the coordinator (parks a hint when a fan-out write can't
		// reach a replica) and the replayer (delivers parked hints once the replica recovers).
		hints := server.NewHintStore()
		tel.RegisterGauge("hints_pending", func() int64 { return int64(hints.Size()) })
		coordinator = server.NewCoordinator(self.ID, router, st, peer, hints, server.CoordinatorOptions{
			N:         cfg.ReplicationFactor,
			R:         cfg.ReadQuorum,
			W:         cfg.WriteQuorum,
			Telemetry: tel,
		})
		replayer = server.NewHintReplayer(hints, peer, cfg.HintReplayerInterval)

		// Anti-entropy is opt-in — a zero interval disables it. When on, the background loop
		// periodically Merkle-diffs a peer and reconciles the keys that differ.
		if cfg.AntiEntropyInterval > 0 {
			antiEntropy = server.NewAntiEntropy(self.ID, st, peer, members, antiEntropyLeaves, cfg.AntiEntropyInterval)
		}

		if cfg.TombstoneGCInterval > 0 {
			tombstoneGC = server.NewTombstoneGC(st, cfg.TombstoneGrace, cfg.TombstoneGCInterval, tel)
		}

		log.Printf("gossip listening on %s", addr)
	}

	srvOpts := server.ServerOptions{
		MaxConns:    cfg.MaxConns,
		ConnTimeout: cfg.ConnTimeout,
		Telemetry:   tel,
	}
	srv := server.NewServer(st, members, router, coordinator, srvOpts)
	if err := srv.Listen(cfg.Addr); err != nil {
		log.Fatalf("failed to bind %s: %v", cfg.Addr, err)
	}
	log.Printf("kyria listening on %s", srv.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	select {
	case <-ctx.Done():
		log.Println("shutting down…")
		if janitor != nil {
			janitor.Stop()
		}
		if gossiper != nil {
			gossiper.Stop()
		}
		if router != nil {
			router.Stop() // end the background ring-rebuild loop
		}
		// Stop the replayer and anti-entropy loops before peer.Close(): both issue calls over
		// `peer`, so drain their goroutines before its pooled connections are released. tombstoneGC
		// is store-only, so its stop order vs peer.Close doesn't matter — it's grouped here for tidiness.
		if replayer != nil {
			replayer.Stop()
		}
		if antiEntropy != nil {
			antiEntropy.Stop()
		}
		if tombstoneGC != nil {
			tombstoneGC.Stop()
		}

		if peer != nil {
			peer.Close() // release pooled replica connections
		}

		if err := srv.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	case err := <-serveErr:
		log.Fatalf("serve: %v", err)
	}
}

// splitSeeds parses the comma-separated -seeds value into peer addresses,
// trimming whitespace and dropping empty entries.
func splitSeeds(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
