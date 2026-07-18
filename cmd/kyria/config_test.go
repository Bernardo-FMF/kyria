package main

import (
	"log/slog"
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
			name: "gossip flags",
			args: []string{"-gossip-addr", "127.0.0.1:7946", "-seeds", "10.0.0.1:7946,10.0.0.2:7946"},
			want: Config{Addr: ":6379", Shards: 32, Eviction: "none", ReapInterval: time.Second, GossipAddr: "127.0.0.1:7946", Seeds: "10.0.0.1:7946,10.0.0.2:7946", Replicas: 100, RebuildInterval: time.Second, ReplicationFactor: 3, ReadQuorum: 2, WriteQuorum: 2, ReplicaTimeout: 2 * time.Second, HintReplayerInterval: time.Second},
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
