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
)

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

		log.Printf("gossip listening on %s", addr)
	}

	srv := server.NewServer(st, members, router)
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
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
