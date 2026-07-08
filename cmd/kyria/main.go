// Command kyria runs the cache server: it builds a concurrency-safe store, wraps
// it in the RESP/TCP server, and serves until interrupted, shutting down cleanly.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

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

	srv := server.NewServer(st)
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
		if err := srv.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	case err := <-serveErr:
		log.Fatalf("serve: %v", err)
	}
}
