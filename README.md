# kyria


kyria is a distributed, in-memory key-value cache written in Go, that was built to 
learn how distributed caches work by implementing one from scratch: a sharded storage 
engine, gossip membership, a consistent-hash ring, tunable-quorum replication, 
vector-clock versioning, and the repair mechanisms that keeps replicas
convergent through failure.

## Features

- **RESP2 wire protocol**: implements the Redis protocol, so standard Redis clients can plugged in to it
- **Sharded storage engine**: stores are lock-striped for concurrent access, with lazy + active TTL expiry
- **Pluggable eviction**: LRU, LFU, and TinyLFU with a count-min-sketch admission filter
- **Gossip membership**: no coordinator or leader; nodes converge over UDP gossip with incarnation-based failure detection
- **Consistent-hash ring**: virtual nodes for even load, with `-MOVED` client redirects (Redis Cluster style)
- **Dynamo-style replication**: tunable **N / R / W** quorums
- **Vector-clock versioning**: concurrent writes are *detected and kept as siblings*, never silently lost
- **Convergence**: read repair, hinted handoff, and Merkle-tree anti-entropy
- **Delete tombstones**: with grace-period garbage collection to prevent resurrection

## How it works

The design is documented in depth under [`docs/`](docs/). Start with
[ARCHITECTURE.md](docs/ARCHITECTURE.md) for the whole picture, then dive into a subsystem:

| Document | Covers |
|---|---|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | basic overview of the system |
| [STORE.md](docs/STORE.md) | the single-node engine: lock striping, TTL expiry, eviction, the count-min sketch |
| [CLUSTERING.md](docs/CLUSTERING.md) | gossip membership, the merge rule, failure detection, the hash ring, `-MOVED` routing |
| [REPLICATION.md](docs/REPLICATION.md) | N/R/W quorums, vector clocks, read repair / hinted handoff / anti-entropy, tombstones |

## Quick start

Requires **Go 1.26+**. The `make` targets below just wrap the Go and Docker toolchains —
if you don't have `make`, run the plain `go` / `docker` command shown with each one instead.

### Build

```sh
make build-bin        # ./bin/kyria
# without make:
go build -o bin/kyria ./cmd/kyria
```

### Run a single node

```sh
./bin/kyria
```

It listens on `:6379` and speaks RESP2, so any Redis client works:

```sh
redis-cli -p 6379 PING
redis-cli -p 6379 SET greeting hi
redis-cli -p 6379 GET greeting
redis-cli -p 6379 SET session tok EX 60
```

> kyria implements the command subset below, not all of Redis. A client that probes for
> capabilities on connect (newer `redis-cli` sends `COMMAND`/`HELLO`) will log a benign
> *unknown command* for those probes; the implemented commands work regardless.

### Run a 3-node cluster (local)

Clustering turns on with `-gossip-addr`. Both the client address (`-addr`) and the gossip
address are **advertised to peers**, so each must be a routable, unique host — the default
`:6379` won't do, and kyria rejects a wildcard/port-only address at startup when clustering
is enabled.

```sh
# node 1 — the seed
./bin/kyria -addr 127.0.0.1:7001 -gossip-addr 127.0.0.1:8001 \
    -replication-factor 3 -read-quorum 2 -write-quorum 2

# node 2
./bin/kyria -addr 127.0.0.1:7002 -gossip-addr 127.0.0.1:8002 -seeds 127.0.0.1:8001 \
    -replication-factor 3 -read-quorum 2 -write-quorum 2

# node 3
./bin/kyria -addr 127.0.0.1:7003 -gossip-addr 127.0.0.1:8003 -seeds 127.0.0.1:8001 \
    -replication-factor 3 -read-quorum 2 -write-quorum 2
```

A request may reach any node; if that node doesn't own the key it replies `-MOVED
<owner>` and the client reconnects there. `redis-cli -p 7001 NODES` lists the live
membership.

### Docker

```sh
make image
make docker-run
# without make:
docker build -t kyria:dev .
docker run --rm -p 6379:6379 -e KYRIA_LOG_LEVEL=debug kyria:dev
```

## Commands

| Command | Arity | Reply |
|---|---|---|
| `PING` | `PING` | `+PONG` |
| `GET` | `GET key` | the value (bulk string), or nil if absent |
| `SET` | `SET key value [EX seconds \| PX millis]` | `+OK` |
| `DEL` | `DEL key` | `:1` if the key existed, else `:0` |
| `NODES` | `NODES` | live cluster membership (one line per node) |
| `STATS` | `STATS` | this node's telemetry (per-command counters, latencies, and gauges) as a text block |

Internal node-to-node verbs (`RGET`, `RSET`, `RTREE`, `RBUCKET`) exist for replication and
anti-entropy; they aren't part of the client API.

## Configuration

Every flag can also be set by environment variable: `KYRIA_` + the flag name uppercased
with dashes as underscores (`-log-flevel debug` ≡ `KYRIA_LOG_LEVEL=debug`). Precedence is
**flag > environment > default**.

**Server**

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:6379` | TCP listen address (must be a routable host when clustering) |
| `-shards` | `32` | lock-striped shards (concurrency) |
| `-max-conns` | `0` | max concurrent client connections (0 = unlimited) |
| `-conn-timeout` | `0` | per-connection idle timeout (0 = none) |
| `-log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |

**Store & eviction**

| Flag | Default | Meaning |
|---|---|---|
| `-eviction` | `none` | `none` \| `lru` \| `lfu` \| `tinylfu` |
| `-max-entries` | `0` | **per-shard** entry cap (0 = unbounded); global ≈ `max-entries × shards` |
| `-max-value-size` | `0` | max value bytes (0 = store default) |
| `-max-key-size` | `0` | max key bytes (0 = store default) |
| `-reap-interval` | `1s` | active TTL sweep interval (0 disables the janitor) |

**Clustering** (all inactive unless `-gossip-addr` is set)

| Flag | Default | Meaning |
|---|---|---|
| `-gossip-addr` | *(empty)* | UDP gossip address; empty = standalone, no clustering |
| `-seeds` | *(empty)* | comma-separated seed peers to bootstrap from |
| `-gossip-interval` | `1s` | gossip round interval |
| `-fail-timeout` | `5s` | mark a silent peer dead after this long |
| `-fanout` | `3` | peers gossiped per round |
| `-replicas` | `100` | virtual nodes per physical node on the ring |
| `-rebuild-interval` | `1s` | how often the ring rebuilds from membership |

**Replication**

| Flag | Default | Meaning |
|---|---|---|
| `-replication-factor` | `3` | **N** — nodes holding each key |
| `-read-quorum` | `2` | **R** — responses a read waits for |
| `-write-quorum` | `2` | **W** — acks a write waits for |
| `-replica-timeout` | `2s` | per-op timeout talking to a replica |
| `-hint-replayer-interval` | `1s` | how often parked hints are retried |
| `-anti-entropy-interval` | `0` | Merkle sweep interval (0 disables) |
| `-tombstone-grace` | `0` | how long a tombstone ages before it may be reaped |
| `-tombstone-gc-interval` | `0` | tombstone GC sweep interval (0 disables) |

`-tombstone-grace` must exceed the worst-case node downtime plus the anti-entropy interval,
or a long-down node can resurrect a deleted key when it returns - see
[REPLICATION.md](docs/REPLICATION.md#deletes-tombstones).

## Testing

```sh
make test         # without make: go test ./...
make test-race    # without make: go test -race ./...
make bench        # without make: go test -run '^$' -bench=. -benchmem ./...
```

## Limitations & non-goals

Stated up front, because knowing what a system *doesn't* do is half of understanding it:

- **No persistence.** A node restart loses that node's data (the cluster re-replicates 
  it from peers, but there is no on-disk snapshot or log).
- **No TLS or authentication.** Traffic is plaintext and unauthenticated.
- **Simplified failure detection.** A plain alive/dead timeout, not full
  [SWIM](https://en.wikipedia.org/wiki/SWIM_Protocol).
- **Server-side sibling resolution.** Concurrent writes are kept as siblings and stored,
  but a read returns one of them rather than handing the client all of them with a context
  token to resolve.
- **Whole-store anti-entropy.** The Merkle tree spans the entire store rather than being
  scoped per key-range, which is exact only when every node replicates every key.