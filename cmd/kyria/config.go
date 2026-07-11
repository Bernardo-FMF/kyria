package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// Config is the server's startup configuration, parsed from command-line flags.
// It is deliberately plain data: parseFlags fills it, storeOptions turns it into
// store.Options, and main reads Addr. Keeping it here leaves main.go to wiring.
type Config struct {
	Addr              string        // TCP listen address, e.g. ":6379"
	Shards            int           // number of lock-striped shards (concurrency)
	Eviction          string        // "none" | "lru" | "lfu" | "tinylfu"
	MaxEntries        int           // PER-SHARD entry cap; 0 = unbounded (no eviction)
	MaxValueSize      int           // max value bytes; 0 = store default
	MaxKeySize        int           // max key bytes; 0 = store default
	ReapInterval      time.Duration // active expiry sweep interval; 0 disables the janitor
	GossipAddr        string        // UDP gossip address; empty = standalone (no clustering)
	Seeds             string        // comma-separated seed peer addresses to bootstrap from
	GossipInterval    time.Duration // gossip round interval; 0 = engine default
	FailTimeout       time.Duration // mark a peer dead after this long silent; 0 = engine default
	Fanout            int           // peers to gossip per round; 0 = engine default
	Replicas          int           // virtual nodes per physical node on the hash ring
	RebuildInterval   time.Duration // how often the router rebuilds the ring from membership
	ReplicationFactor int           // N: how many nodes hold each key
	ReadQuorum        int           // R: responses a read waits for
	WriteQuorum       int           // W: acks a write waits for
	ReplicaTimeout    time.Duration // per-op dial+IO timeout to a replica
}

// parseFlags parses args (typically os.Args[1:]) into a Config using a local
// FlagSet — so it is callable repeatedly in tests and returns a flag error
// rather than exiting the process. It validates that -eviction names a known
// policy and that any policy is paired with a positive -max-entries: a policy
// without a cap never evicts, since eviction only fires once a shard is full.
func parseFlags(args []string) (Config, error) {
	fs := flag.NewFlagSet("kyria", flag.ContinueOnError)
	addr := fs.String("addr", ":6379", "TCP listen address")
	shards := fs.Int("shards", 32, "number of shards")
	eviction := fs.String("eviction", "none", "none|lru|lfu|tinylfu")
	maxEntries := fs.Int("max-entries", 0, "per-shard entry cap (0 = unbounded)")
	maxValueSize := fs.Int("max-value-size", 0, "max value bytes (0 = store default)")
	maxKeySize := fs.Int("max-key-size", 0, "max key bytes (0 = store default)")
	reapInterval := fs.Duration("reap-interval", time.Second, "active expiry sweep interval (0 disables)")
	gossipAddr := fs.String("gossip-addr", "", "UDP gossip address (empty = standalone)")
	seeds := fs.String("seeds", "", "comma-separated seed peer addresses")
	gossipInterval := fs.Duration("gossip-interval", 0, "gossip round interval (0 = default)")
	failTimeout := fs.Duration("fail-timeout", 0, "mark a peer dead after this long silent (0 = default)")
	fanout := fs.Int("fanout", 0, "peers to gossip per round (0 = default)")
	replicas := fs.Int("replicas", 100, "virtual nodes per physical node on the hash ring")
	rebuildInterval := fs.Duration("rebuild-interval", time.Second, "how often the router rebuilds the ring from membership")
	replicationFactor := fs.Int("replication-factor", 3, "replicas per key (N)")
	readQuorum := fs.Int("read-quorum", 2, "responses a read waits for (R)")
	writeQuorum := fs.Int("write-quorum", 2, "acks a write waits for (W)")
	replicaTimeout := fs.Duration("replica-timeout", 2*time.Second, "per-op timeout talking to a replica")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	cfg := Config{
		Addr:              *addr,
		Shards:            *shards,
		Eviction:          *eviction,
		MaxEntries:        *maxEntries,
		MaxValueSize:      *maxValueSize,
		MaxKeySize:        *maxKeySize,
		ReapInterval:      *reapInterval,
		GossipAddr:        *gossipAddr,
		Seeds:             *seeds,
		GossipInterval:    *gossipInterval,
		FailTimeout:       *failTimeout,
		Fanout:            *fanout,
		Replicas:          *replicas,
		RebuildInterval:   *rebuildInterval,
		ReplicationFactor: *replicationFactor,
		ReadQuorum:        *readQuorum,
		WriteQuorum:       *writeQuorum,
		ReplicaTimeout:    *replicaTimeout,
	}

	switch cfg.Eviction {
	case "none", "lru", "lfu", "tinylfu":
		// ok
	default:
		return Config{}, fmt.Errorf("unknown -eviction %q (want none, lru, lfu, or tinylfu)", cfg.Eviction)
	}

	if cfg.Eviction != "none" && cfg.MaxEntries <= 0 {
		return Config{}, fmt.Errorf("-eviction %s needs -max-entries > 0", cfg.Eviction)
	}

	// Quorum bounds: R and W must be reachable within the replica set. R+W>N gives
	// read-your-writes, but a weaker config is still valid, so we don't require it.
	if cfg.ReplicationFactor <= 0 {
		return Config{}, fmt.Errorf("-replication-factor must be >= 1, got %d", cfg.ReplicationFactor)
	}

	if cfg.ReadQuorum <= 0 || cfg.ReadQuorum > cfg.ReplicationFactor {
		return Config{}, fmt.Errorf("-read-quorum must be in [1, %d], got %d", cfg.ReplicationFactor, cfg.ReadQuorum)
	}

	if cfg.WriteQuorum <= 0 || cfg.WriteQuorum > cfg.ReplicationFactor {
		return Config{}, fmt.Errorf("-write-quorum must be in [1, %d], got %d", cfg.ReplicationFactor, cfg.WriteQuorum)
	}

	return cfg, nil
}

// storeOptions translates a validated Config into the store.Options handed to
// store.NewSharded: the size limits and entry cap when set, plus the eviction
// policy. The policy mapping is deliberately non-uniform — NewLRU and NewLFU are
// already func() Policy, so they pass by value, while NewTinyLFU is called with
// MaxEntries to size its sketch. MaxEntries thus both caps each shard and sizes
// TinyLFU; the cap is per shard, so the global cap ≈ MaxEntries × Shards.
func (c Config) storeOptions() []store.Option {
	var opts []store.Option

	if c.MaxEntries > 0 {
		opts = append(opts, store.WithMaxEntries(c.MaxEntries))
	}

	if c.MaxValueSize > 0 {
		opts = append(opts, store.WithMaxValueSize(c.MaxValueSize))
	}

	if c.MaxKeySize > 0 {
		opts = append(opts, store.WithMaxKeySize(c.MaxKeySize))
	}

	switch c.Eviction {
	case "lru":
		opts = append(opts, store.WithPolicy(store.NewLRU))
	case "lfu":
		opts = append(opts, store.WithPolicy(store.NewLFU))
	case "tinylfu":
		opts = append(opts, store.WithPolicy(store.NewTinyLFU(c.MaxEntries)))
	}

	return opts
}

// gossiperOptions translates a Config into the cluster.GossiperOptions passed to
// cluster.NewGossiper: the seeds, plus any timing knob a flag overrode. A knob left
// at zero is omitted, so NewGossiper's built-in default applies — the same "append
// only what's set" shape as storeOptions.
func (c Config) gossiperOptions() []cluster.GossiperOption {
	opts := []cluster.GossiperOption{
		cluster.WithSeeds(splitSeeds(c.Seeds)),
	}

	if c.GossipInterval > 0 {
		opts = append(opts, cluster.WithGossipInterval(c.GossipInterval))
	}
	if c.FailTimeout > 0 {
		opts = append(opts, cluster.WithFailTimeout(c.FailTimeout))
	}
	if c.Fanout > 0 {
		opts = append(opts, cluster.WithFanout(c.Fanout))
	}

	return opts
}
