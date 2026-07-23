# Architecture

This document describes the architecture of the whole system - how the pieces fit together 
and how a request flows through them.

A core concept here is that **every node runs the same code and is fully self-sufficient**.
There is no coordinator node, no leader, no special role. A cluster is just several
identical nodes gossiping. So the diagram below explodes *one* node into its parts; the
other two are drawn collapsed, but they contain exactly the same components.

## The whole system

![kyria system architecture](img/architecture.svg)

A single node has three groups of moving parts:

- **The request handler** (left) - the path every client command takes. `Server` frames
  RESP2 off the socket, `Handler` dispatches it and decides whether this node owns the
  key, `Coordinator` drives the replication quorum, and `Store` holds the data.
- **The cluster mechanism** (centre) — how the node participates in the cluster. `Router`
  keeps the consistent-hash ring, `Members` holds the gossiped roster of who's alive,
  `Gossiper` is the UDP engine that spreads that roster, and `Peer` is the pooled TCP
  client the node uses to talk to other nodes.
- **The background goroutines** (bottom) - loops that run continuously, on their own
  timers, independent of any client request. They are what keep the cluster converging.

Between nodes there are exactly two kinds of traffic: **gossip over UDP** (membership,
best-effort since gossip rounds are very frequent) and **replication over TCP** (the actual 
data, it needs to be reliable). A client's own connection is plain RESP2 over TCP to 
whichever node it reached.

## A request, end to end

Trace a `SET foo bar` to see how the request flows and how the cluster mechanism interacts:

1. **Server** accepts the connection and frames one RESP2 command off the socket, handing
   it to the Handler (Each connection runs in its own goroutine).
2. **Handler** looks the command up, and, if in a cluster, asks the **Router** whether this
   node owns `foo`. If not, it replies `-MOVED <owner>` and the client reconnects there;
   nothing else happens on this node. If it *is* the owner, the Handler passes the command
   to the Coordinator.
3. **Coordinator** applies the write to the local **Store** first (that counts as the first
   quorum ack), then fans the same versioned value out to the key's other replicas through
   **Peer**, and waits for W acknowledgements.
4. Once W acks are in, the client gets `+OK`. A replica that couldn't be reached doesn't
   block the reply - it gets a hint parked for later delivery.

A `GET` is the mirror image: the Coordinator reads locally, queries peers until R
responses are in, reconciles them, and replies. A standalone node (no clustering) skips
peer querying entirely and the Handler serves the Store directly.

The convergence mechanisms(the quorum math, vector-clock reconciliation, the repair paths) 
are in [REPLICATION.md](REPLICATION.md); the ownership and redirect logic is in
[CLUSTERING.md](CLUSTERING.md).

## The background goroutines

Each routine runs on its own timer and touches one part of the node. 
Together they're what makes the cluster *self-healing*:

| Loop | Cadence (configurable) | What it does |
|---|---|---|
| **janitor** | 1s | reaps expired-TTL keys from the store (active expiry) |
| **ring rebuild** | 1s | rebuilds the hash ring from the current live membership |
| **gossip round** | 1s | heartbeats self, detects failures, pushes the roster to random peers |
| **hint replayer** | 1s | retries delivering parked hints to replicas that were unreachable |
| **anti-entropy** | optional | merkle-diffs a random peer and reconciles the entries that differ |
| **tombstone GC** | optional | reaps tombstones once they've aged past the grace period |

The first three loops keep the node itself current (memory reclaimed, ring fresh, membership
converging); the last three keep *replicas* converging (missed writes delivered, drifted
data reconciled, deletes eventually cleaned up).