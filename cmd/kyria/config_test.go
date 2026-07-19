package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/store"
)

// These tests cover cmd/kyria's config layer: parseFlags (flag parsing +
// validation) and Config.storeOptions (translation into store options).
//
// NOTE: they call parseFlags(args []string). Refactor parseFlags to take its
// arguments and use a local flag.NewFlagSet("kyria", flag.ContinueOnError)
// instead of the global flag package — otherwise it can't be re-parsed across
// table cases, and a bad flag would os.Exit the test process instead of
// returning an error. In main, call parseFlags(os.Args[1:]).

// TestParseFlags checks that valid flag sets parse into the expected Config,
// including the zero-flag defaults.
func TestParseFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want Config
	}{
		{
			name: "defaults",
			args: nil,
			want: Config{Addr: ":6379", Shards: 32, LogLevel: slog.LevelInfo, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "custom addr and shards",
			args: []string{"-addr", ":7000", "-shards", "8"},
			want: Config{Addr: ":7000", Shards: 8, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "max-conns",
			args: []string{"-max-conns", "100"},
			want: Config{Addr: ":6379", Shards: 32, MaxConns: 100, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "conn-timeout",
			args: []string{"-conn-timeout", "30s"},
			want: Config{Addr: ":6379", Shards: 32, ConnTimeout: 30 * time.Second, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "log-level",
			args: []string{"-log-level", "debug"},
			want: Config{Addr: ":6379", Shards: 32, LogLevel: slog.LevelDebug, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "log-level is case-insensitive",
			args: []string{"-log-level", "WARN"},
			want: Config{Addr: ":6379", Shards: 32, LogLevel: slog.LevelWarn, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "log-level accepts an offset form",
			args: []string{"-log-level", "warn+2"},
			want: Config{Addr: ":6379", Shards: 32, LogLevel: slog.LevelWarn + 2, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "lru with cap",
			args: []string{"-eviction", "lru", "-max-entries", "100"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "lru", MaxEntries: 100, ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "tinylfu with cap",
			args: []string{"-eviction", "tinylfu", "-max-entries", "50"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "tinylfu", MaxEntries: 50, ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "size limits",
			args: []string{"-max-value-size", "2048", "-max-key-size", "128"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", MaxValueSize: 2048, MaxKeySize: 128, ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "custom reap interval (0 disables the janitor)",
			args: []string{"-reap-interval", "500ms"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", ReapInterval: 500 * time.Millisecond, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			// -addr is spelled out because enabling gossip makes it an advertised value: the
			// default ":6379" names no host, so peers would have nothing to reach this node at.
			name: "gossip flags",
			args: []string{"-addr", "127.0.0.1:6379", "-gossip-addr", "127.0.0.1:7946", "-seeds", "10.0.0.1:7946,10.0.0.2:7946"},
			want: Config{Addr: "127.0.0.1:6379", Shards: 32, Eviction: "none", ReapInterval: time.Second, GossipAddr: "127.0.0.1:7946", Seeds: "10.0.0.1:7946,10.0.0.2:7946", Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "gossip timing flags",
			args: []string{"-gossip-interval", "2s", "-fail-timeout", "10s", "-fanout", "5"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", ReapInterval: time.Second, GossipInterval: 2 * time.Second, FailTimeout: 10 * time.Second, Fanout: 5, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "routing flags",
			args: []string{"-replicas", "256", "-rebuild-interval", "5s"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", ReapInterval: time.Second, Replicas: 256, RebuildInterval: 5 * time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "replication flags",
			args: []string{"-replication-factor", "5", "-read-quorum", "3", "-write-quorum", "4", "-replica-timeout", "1s"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 5, ReadQuorum: 3, WriteQuorum: 4, ReplicaTimeout: time.Second, HintReplayerInterval: time.Second},
		},
		{
			name: "tombstone gc flags",
			args: []string{"-tombstone-grace", "24h", "-tombstone-gc-interval", "10s"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", ReapInterval: time.Second, Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second, TombstoneGrace: 24 * time.Hour, TombstoneGCInterval: 10 * time.Second},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFlags(tc.args)
			if err != nil {
				t.Fatalf("parseFlags(%v) error: %v", tc.args, err)
			}
			if got != tc.want {
				t.Errorf("parseFlags(%v) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

// TestParseFlags_Errors checks that invalid flag combinations are rejected:
// an unrecognized policy, and any policy without a positive -max-entries (which
// would silently never evict).
func TestParseFlags_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"unknown eviction", []string{"-eviction", "bogus"}},
		{"policy without cap", []string{"-eviction", "lru"}},
		{"policy with zero cap", []string{"-eviction", "lfu", "-max-entries", "0"}},
		{"replication-factor below 1", []string{"-replication-factor", "0"}},
		{"read-quorum above replication-factor", []string{"-read-quorum", "5", "-replication-factor", "3"}},
		{"write-quorum below 1", []string{"-write-quorum", "0"}},
		{"tombstone gc without grace", []string{"-tombstone-gc-interval", "10s"}},
		{"negative max-conns", []string{"-max-conns", "-1"}},
		{"unknown log-level", []string{"-log-level", "bogus"}},
		{"empty log-level", []string{"-log-level", ""}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseFlags(tc.args); err == nil {
				t.Errorf("parseFlags(%v) = nil error, want an error", tc.args)
			}
		})
	}
}

// TestParseFlags_Environment covers the KYRIA_* fallback, which exists so a container can be
// configured with -e instead of a long argv. The contract is flag > env > default.
func TestParseFlags_Environment(t *testing.T) {
	t.Run("fills a flag that was not given", func(t *testing.T) {
		t.Setenv("KYRIA_SHARDS", "8")
		cfg, err := parseFlags(nil)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if cfg.Shards != 8 {
			t.Errorf("Shards = %d, want 8 from the environment", cfg.Shards)
		}
	})

	t.Run("an explicit flag beats the environment", func(t *testing.T) {
		t.Setenv("KYRIA_SHARDS", "8")
		cfg, err := parseFlags([]string{"-shards", "64"})
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if cfg.Shards != 64 {
			t.Errorf("Shards = %d, want 64 — an explicit flag must win over the environment", cfg.Shards)
		}
	})

	t.Run("dashes in a flag name become underscores", func(t *testing.T) {
		t.Setenv("KYRIA_LOG_LEVEL", "debug")
		cfg, err := parseFlags(nil)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if cfg.LogLevel != slog.LevelDebug {
			t.Errorf("LogLevel = %v, want debug from KYRIA_LOG_LEVEL", cfg.LogLevel)
		}
	})

	t.Run("typed values go through the flag's own parser", func(t *testing.T) {
		t.Setenv("KYRIA_CONN_TIMEOUT", "30s")
		cfg, err := parseFlags(nil)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if cfg.ConnTimeout != 30*time.Second {
			t.Errorf("ConnTimeout = %v, want 30s — the value must be parsed as a Duration", cfg.ConnTimeout)
		}
	})

	// The failure mode this guards against: a container silently running the default because
	// someone typo'd a value, with nothing in the logs to say so.
	t.Run("an unparseable value is an error, not a silent default", func(t *testing.T) {
		t.Setenv("KYRIA_SHARDS", "not-a-number")
		_, err := parseFlags(nil)
		if err == nil {
			t.Fatal("parseFlags = nil error, want a rejection of the bad environment value")
		}
		if !strings.Contains(err.Error(), "KYRIA_SHARDS") {
			t.Errorf("error %q does not name the offending variable", err)
		}
	})

	// Pins os.LookupEnv over os.Getenv: Getenv cannot tell an unset variable from an empty
	// one, so an explicitly empty value would silently keep the flag's default instead.
	t.Run("an empty value is honored, not treated as unset", func(t *testing.T) {
		t.Setenv("KYRIA_ADDR", "")
		cfg, err := parseFlags(nil)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if cfg.Addr != "" {
			t.Errorf("Addr = %q, want it overridden to empty", cfg.Addr)
		}
	})

	t.Run("an unset variable leaves the default", func(t *testing.T) {
		cfg, err := parseFlags(nil)
		if err != nil {
			t.Fatalf("parseFlags: %v", err)
		}
		if cfg.Shards != 32 {
			t.Errorf("Shards = %d, want the default 32", cfg.Shards)
		}
	})
}

// TestParseFlags_AdvertisedAddresses: with clustering on, -addr and -gossip-addr are both
// published to the rest of the cluster — -addr becomes this node's ring identity, the target
// of a -MOVED reply, and the address peers dial, while -gossip-addr is echoed back from
// conn.LocalAddr() as the UDP contact address. A wildcard or port-only value starts up fine
// and then fails silently, so it has to be rejected at parse time instead.
func TestParseFlags_AdvertisedAddresses(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		// Clustered: a host peers can actually resolve.
		{"hostname, the compose form", []string{"-addr", "kyria-1:6379", "-gossip-addr", "kyria-1:7946"}, false},
		{"loopback, the local-cluster form", []string{"-addr", "127.0.0.1:7001", "-gossip-addr", "127.0.0.1:8001"}, false},

		// Clustered: nothing a peer could reach.
		{"addr port-only", []string{"-addr", ":6379", "-gossip-addr", "kyria-1:7946"}, true},
		{"addr 0.0.0.0", []string{"-addr", "0.0.0.0:6379", "-gossip-addr", "kyria-1:7946"}, true},
		{"addr ::", []string{"-addr", "[::]:6379", "-gossip-addr", "kyria-1:7946"}, true},
		{"addr missing port", []string{"-addr", "kyria-1", "-gossip-addr", "kyria-1:7946"}, true},
		{"gossip-addr port-only", []string{"-addr", "kyria-1:6379", "-gossip-addr", ":7946"}, true},
		{"gossip-addr 0.0.0.0", []string{"-addr", "kyria-1:6379", "-gossip-addr", "0.0.0.0:7946"}, true},

		// Standalone publishes nothing, so the same values must stay valid — otherwise the
		// check breaks every single-node invocation, which is the common case.
		{"standalone keeps the default addr", nil, false},
		{"standalone accepts 0.0.0.0", []string{"-addr", "0.0.0.0:6379"}, false},
		{"standalone accepts ::", []string{"-addr", "[::]:6379"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFlags(tc.args)
			if tc.wantErr && err == nil {
				t.Errorf("parseFlags(%v) = nil error, want a rejection", tc.args)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("parseFlags(%v) error: %v, want it accepted", tc.args, err)
			}
		})
	}
}

// TestStoreOptions_EvictionWiring verifies that storeOptions actually threads the
// cap + policy through to the store: with a 1-shard, cap-3 config, inserting more
// than 3 keys must leave the store bounded. (store.Option is a func and isn't
// comparable, so we assert on behavior, not on the returned slice.)
func TestStoreOptions_EvictionWiring(t *testing.T) {
	cfg := Config{Shards: 1, Eviction: "lru", MaxEntries: 3}
	st := store.NewSharded(cfg.Shards, cfg.storeOptions()...)

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if _, err := st.Set(k, []byte("v")); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
	}
	if got := st.Size(); got > 3 {
		t.Errorf("Size = %d, want <= 3 (eviction should cap the shard)", got)
	}
}

// TestStoreOptions_NoEvictionUnbounded is the control: with no policy and no cap,
// storeOptions must add neither, so the store holds everything.
func TestStoreOptions_NoEvictionUnbounded(t *testing.T) {
	cfg := Config{Shards: 1, Eviction: "none"}
	st := store.NewSharded(cfg.Shards, cfg.storeOptions()...)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		if _, err := st.Set(k, []byte("v")); err != nil {
			t.Fatalf("Set(%q): %v", k, err)
		}
	}
	if got := st.Size(); got != len(keys) {
		t.Errorf("Size = %d, want %d (no cap configured)", got, len(keys))
	}
}

// TestStoreOptions_MaxValueSize verifies the size-limit options are wired: an
// oversized value must be rejected once MaxValueSize is set.
func TestStoreOptions_MaxValueSize(t *testing.T) {
	cfg := Config{Shards: 1, MaxValueSize: 4}
	st := store.NewSharded(cfg.Shards, cfg.storeOptions()...)

	if _, err := st.Set("k", []byte("toolong")); err == nil {
		t.Error("Set with a 7-byte value = nil error, want a size-limit error")
	}
}
