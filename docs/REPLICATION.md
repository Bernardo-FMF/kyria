# Replication

[Clustering](CLUSTERING.md) decides *which* nodes hold a key, and this document goes into detail
as to how a key becomes **N durable, consistent copies** and how those copies reconcile after
the failures that can occur in distributed systems.

kyria follows these principles: 
1. no primary owns the truth;
2. writes and reads each satisfy a tunable [quorum](#quorums-n-r-w) - how many acknowledgements 
   the write and read operations wait for;
3. versions carry vector clocks so concurrent writes are detected rather than silently lost;
4. multiple mechanisms that merge divergent replicas back together: read-repair, hinted handoff,
   anti-entropy.

## Quorums: N, R, W

Three numbers tune the whole system:

- **N** - the replication factor: how many nodes hold each key.
- **W** - how many acknowledgements a write waits for before replying success.
- **R** - how many responses a read waits for before reconciling and replying.

splitting read and write quorums is what makes consistency tunable. The rule that matters
is **R + W > N**: when the read quorum and the write quorum must overlap in at least one
node, every read is guaranteed to see the latest acknowledged write - read-your-writes.

### Write operation

![Write path and the W quorum](img/repl-write-quorum.svg)

The coordinator applies the write to its **own** store first - the local apply counts as
a **W** ack, then fans the *same* versioned value out to the other replicas concurrently 
and waits until **N** is met. With W=2 and N=3, the local ack plus one replica ack
is enough to reply `+OK`; the third replica does not block the fan-out.
- **The local apply uses a store path that bypasses the eviction admission filter.** An
  ordinary write can be *refused* by a full cache (TinyLFU may decide a new key isn't
  worth admitting). A replica does not get to decide if a key is worth caching, so 
  replicated writes route around admission. Without this, a write could reply `+OK` 
  having landed on *zero* nodes, because every replica silently dropped it.
- **An unreachable replica does not fail the write, it gets a hint.** The coordinator
  parks the missed write locally and returns to the client; a background replayer
  delivers it once the replica recovers. This is [hinted handoff](#convergence-and-repair),
  and it is what lets a quorum-met write still reach all N copies eventually without ever
  blocking on a down node.

The quorum count is capped at the replica set: `need = min(W, len(replicas))`, so a
cluster smaller than W still makes progress instead of deadlocking on acks that can never
arrive.

The fan-out is launched to every replica regardless of W and it is where hints are parked.
Stopping early once W acks arrive only ends the *waiting*; the writes to the remaining
replicas still happen.

### Read operation

The coordinator reads its local value (the first response), queries the other replicas 
concurrently, and stops once **R** responses are in. It reconciles those responses into 
a single result and replies. Replicas that turn out to be behind are healed *after* the 
reply, off the read path, so repair never adds latency to the read itself.

## Versioning: vector clocks

If two clients write the same key at nearly the same time, we need to decide which version
we'll keep. For this, we use **vector clocks**, which don't pick a winner - they detect that there
*was* a conflict, so it can be kept rather than lost ([vclock.go](../internal/vclock/vclock.go), 
[version.go](../internal/version/version.go)).

A vector clock is a small map of node ID to counter. Comparing two clocks yields one of
the verdicts: 
1. one **descends** the other (a clean successor - safe to replace);
2. they are **equal**;
3. they are **concurrent** - each has a counter the other lacks, meaning the histories 
   diverged and neither is authoritative.

Every write mints its clock by taking the **frontier** of the versions already stored (the
pointwise maximum across all current siblings) and incrementing the coordinator's own
entry. That new clock descends everything it reconciles against - unless another writer
did the same thing concurrently from the same starting point.

![Vector clocks: siblings and convergence](img/repl-vclock.svg)

When reconciliation meets a version that is *concurrent* with what's stored, it cannot
drop either — so it keeps **both, as siblings**. A key can therefore hold several
concurrent values at once. They collapse back to one the moment a write arrives whose
clock descends all of them - it replaces the conflict with the new value. 
Reconciliation is the single rule behind all of this - drop a version that is
descended, keep one that is concurrent, and add the incoming one unless something already
covers it.

## Convergence and repair

Quorums make a write *durable* the instant it is acknowledged, but they do not make every
replica *identical*. A node went down, a packet was dropped, or a write that only met W leaves
replicas out of sync. Three mechanisms converge them, acting in different moments in time to
cover all scenarios:

| Mechanism | Triggered by | Covers | Timescale |
|---|---|---|---|
| **Read repair** | a read that reconciles R responses | keys that are actually read | immediate, off the read path |
| **Hinted handoff** | a write that can't reach a replica | the specific writes that a node that went down missed | replayed on a loop |
| **Anti-entropy** | a periodic background sweep | *everything*, including keys never read | whole-store comparison |

- **Read repair** is opportunistic. Having gathered R responses and reconciled them, the
coordinator notices which replicas returned a set behind the reconciled result and pushes
the merged version back to them. This happens in the background, so the client already 
has its reply. It costs nothing extra (the read already fetched every response) but only 
heals keys someone reads.

**Hinted handoff** closes the write-time gap. When a fan-out write can't reach a replica,
the coordinator parks a *hint*(target node, key, the versioned blob), and a background
replayer retries delivery on a loop until the node is back and the hint lands. It is what
makes a quorum-met write reach all N copies even though one replica was down when it was
written ([handoff.go](../internal/server/handoff.go)).

**Anti-entropy** handles keys that are written and never read again, or a hint lost due to
a coordinator crash. Periodically, a node builds a [Merkle tree](https://en.wikipedia.org/wiki/Merkle_tree) 
over its whole store, fetches a random peer's tree, and diffs them. A Merkle tree summarizes 
ranges of keys as hashes, so two identical stores compare in a single root-hash check, 
and a divergence narrows to just the buckets that differ. This means that only those entries 
that have differences are exchanged and reconciled, not the whole dataset 
([antientropy.go](../internal/server/antientropy.go)). The leaf count is a cluster-wide constant 
so every node's tree is comparable.

## Deletes: tombstones

Deleting a key cannot simply remove it. In a scenario where N=3 with one replica down: 
a plain delete lands on two nodes, and then anti-entropy compares the two empty stores 
against the third, and sees that one node still has the value and **restores** the value everywhere. 
This means that the delete would be undone.

So we have to treat a delete as a **write of a "gone" version**, a *tombstone*: an
empty value stamped with a superseding clock ([version.go](../internal/version/version.go)).
It reconciles, replicates, and repairs exactly like any other version, and because its
clock descends the value it replaces, it re-buries that value wherever a lagging replica
tries to resurrect it. The delete returns `:1` if a live value existed before it (else
`:0`), and reads filter the reconciled set through a *liveness* check so a winning
tombstone reads as a **miss**, never as an empty value.

To avoid tombstones that last forever there is a grace-period garbage collector that reaps
a key only when its versions are *all* tombstones and *all* older than a configured
grace period.

> The grace period **must exceed the worst-case node downtime plus the anti-entropy
> interval.** If we reap a tombstone too soon and a replica that was down when the delete
> happened comes back still holding the old value, we won't have a tombstone to bury it.
> This would result in the anti-entropy to spread the *resurrected* value back across the cluster.
> If the grace period is too long, the dead keys will occupy memory.

The reap itself is a compare-and-delete under the shard lock: it re-checks reapability at
the moment of removal, so a key that was written again between the sweep's scan and its
delete is spared. This keeps garbage collection from racing a concurrent write.
