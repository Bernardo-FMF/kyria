# Clustering

kyria scales out by turning a set of identical processes into a cluster. There is
**no coordinator node, no leader, and no config server** - every decision each node 
makes is local: who is alive, who owns each key, when to give up on a peer. 
Gossip is the only thing that reconciles those independent views.
With nothing central to elect or depend on, the cluster gains or loses a member with 
no coordination step. The next gossip round carries the new reality and the ring 
rebuilds to match.

This document covers how nodes find each other and agree on who owns what:
[membership](#membership-gossip), the [merge rule](#the-merge-rule) that keeps their
views consistent, [failure detection](#failure-detection), the
[hash ring](#placement-the-ring) that decides ownership, and
[routing](#routing-redirect-not-proxy).

A **node** is one kyria process, identified by its client TCP address
(e.g. `kyria-2:6379`). That single string is the node's identity everywhere: its key
on the ring, the address peers dial, and what a redirected client is sent to. Keep
that in mind — it is why a node's address has to be one that others can actually
reach.

## Membership: gossip

Each node keeps its **own** roster of every member it has heard of, there is no
shared source of truth. Each member record is small: an ID, a gossip address, a state
(`alive` or `dead`), and an incarnation number.

Every **tick (configurable by the user at startup)**, a node runs a gossip round
([gossip.go](../internal/cluster/gossip.go)):

1. **Heartbeat itself**: bump its own incarnation and refresh its own timestamp.
2. **Detect failures**: mark any non-self member it has not heard from in over five
   seconds as `dead`, locally.
3. **Spread the word**: pick **N** targets **(fanout is configurable at startup)** at 
   random from its known-alive peers plus the configured seeds, and send each a 
   full snapshot of its roster over **UDP**.

![Cluster formation by gossip](img/cluster-gossip.svg)

The figure traces a node joining. At boot (round 0) the new node knows only itself and
its seed, and it gossips to the seed. the seed's next round carries a roster that 
includes the newcomer, which spreads to three more peers, and so on. Within
a round or two the joiner knows the whole cluster (round 1), and once every node has
gossiped the new member onward, all rosters match (round 2). Knowledge propagates the
way a rumour does, each telling reaches a few more nodes.

Gossip is **push-only and fire-and-forget**. There is no acknowledgement and no pull -
a node broadcasts its view and moves on; a dropped UDP packet is simply corrected on
the next round. This is what makes the protocol cheap and robust.

A new node **bootstraps from seeds**: given one or more seed addresses, it gossips to
a seed, the seed's reply carries the rest of the cluster's roster, and from there
knowledge spreads.

## The merge rule

When a node receives a peer's roster, it folds each record into its own. This merge is
the convergence rule of the protocol — nodes exchanging views in any order, 
any number of times, settle on the same roster.
Per incoming record ([membership.go](../internal/cluster/membership.go)):

- **Unknown node**: insert it.
- **A claim about *ourselves* that says we are not alive**: refute it. Raise our own
  incarnation above the claim's and re-assert `alive`. This is how a node that was
  wrongly declared dead brings itself back.
- **A strictly higher incarnation than we hold**: adopt the incoming record wholesale.
  A higher incarnation is fresher information, and only a node may raise its own.
- **An equal incarnation**: take the *deader* state. `dead` beats `alive`, so the worse
  state at the same version wins.

![The gossip merge rule](img/cluster-merge.svg)

The **incarnation number** is a per-node version counter that only its owner may increment 
— each heartbeat, and once more to refute a false claim.

## Failure detection

Detection is timeout-based and local: in each round, a node marks any alive peer it has
not heard fresh gossip from within the fail timeout as `dead` in its own roster. 
That verdict then spreads like any other record.

It is not a global agreement, each node decides independently and gossip reconciles
the verdicts. A node that was marked dead but is actually fine refutes the claim the
moment its next heartbeat arrives, via the incarnation rule above, so a false positive
self-corrects rather than partitioning the cluster.

A node that **restarted** boots fresh at incarnation 1, which is below the incarnation
value the cluster is holding, so its first `alive` gossip is *ignored* as stale. 
What revives it is the self-refutation rule: the cluster tells it "you are dead at inc X", and it responds by
raising its own incarnation to X + 1 and re-asserting `alive`.

## Consistent-hash ring

A consistent-hash ring answers what shard owns each key. A background loop rebuilds the ring 
from the currently-alive membership and swaps it in atomically, so the request path reads it 
lock-free and never sees a half-built ring ([router.go](../internal/cluster/router.go),
[ring.go](../internal/cluster/ring.go)).

![Consistent-hash ring placement](img/cluster-ring.svg)

Each node contributes **100 virtual nodes** (vnodes) to the ring by default. A vnode is
just a point on a 64-bit ring, its position derived by hashing `nodeID#i` with FNV-1a.
Because a node's 100 points are scattered around the ring rather than sitting together,
load spreads evenly, and when a node leaves its share is redistributed across many
neighbours instead of dumping entirely onto the next one.

To place a key: hash it, binary-search for the first vnode at or past that position
(wrapping around the end of the ring), then **walk clockwise, taking each distinct
physical node the first time it appears** until N are collected. The first node found
is the **primary**; the collected set is the replica set. The replica set are deduped
to avoid having a set where the replicas turn out to be vnodes of the same machine.

## Routing: redirect, not proxy

A client may connect to **any** node. If that node is not the primary for the requested
key, it does not fetch the value on the client's behalf, it replies with a redirect:

```
-MOVED kyria-2:6379
```

and the client reconnects to the owner. Requests are never proxies nor forwards a client
request.

![Routing by redirect, not proxy](img/cluster-routing.svg)

The four steps are the whole protocol: 
1. The client sends `GET foo` to whichever node it happened to reach. 
2. that node isn't the owner so it answers `-MOVED kyria-2:6379`. 
3. The client reconnects to the named owner.
4. The owner returns the value.