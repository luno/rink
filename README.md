# Rink

Rink is a "distributed sticky ranked ring" using etcd. A rink provides role scheduling across distributed processes, 
with each role only assigned to a single instance. Stickiness allows smooth transferring of roles during controlled
rolling deploys which minimises role rebalancing.

- A rink is a distributed self-managing group of processes; the members of the rink.
- Members can join or leave the rink. Inactive members are automatically removed.
- A rank is assigned to each member; mth of n members.
- Roles (`leader`, `3dbad20c`, or `BTC/ETH`) are implicitly assigned to a member
  by consistent hashing.
- When the size of rink (n) grows or shrinks, ranks are rebalanced (ensuring unique ranks less than n).
- Rebalancing results in all roles being reassigned.
- Stickyness is provided by a configurable rebalance delay. A rebalance 
  due to a joined member is only triggered after the rebalance delay. If another member leaves
  during this time, its rank will immediately be transferred without affecting other members.
- Kubernetes rolling deploy with `MaxUnavailable=0, MaxSurge>0` will smoothly transfer roles from
  old members to new members.
  
## Usage

Example Usage: Given multiple replicas of a service, `rink` will ensure that at most one replica 
does the work in `doFooStuff` at a time.
```
cli, err := etcd.NewClient()
if err != nil {...}
r := rinkv2.New(cli, "my_rink")
defer r.Shutdown()

for {
    ctx := r.AwaitRole("foo")
    doFooStuff(ctx)
}
```

## Concepts and building blocks

`rink` leverages the etcd `concurrency` package as underlying building blocks. It also introduces some concepts
of its own.

*`concurrency.Session`*: An etcd session identifies a member and acts as liveliness proxy.
A member joins the rink with an active session. A member leaves the rink by closing the session.
A session is implemented in etcd with an opaque key and lease (default TTL of 60s). The session 
is kept alive by an internal goroutine that periodically extends the lease. If the session is
closed, the key is deleted. If the lease times out, the key is automatically deleted and the session is
assumed cancelled.

*`concurrency.Election`*: An etcd election elects a leader that may proclaim values. These proclamations
 appear as an append-only log of values to all members of the election. All members of `rink` join an election. 
 One member will therefore be the leader. The leader maintains the rink; detecting member joins and leaves, 
 promoting and rebalancing ranks, and "proclaiming" subsequent version of the rink state. The election
 proclamations are therefore an immutable log of rink states.
 
*`member keys`*: When joining the rink, each member creates a key `{rink}/members/{member}` attached to
  its session lease. The value of the key is its timestamp on joining. 
  Since the member keys are linked to the session lease, they are automatically deleted when the session
  is closed (or times out).

*`rink ranks`*: One of the members is always the leader (of the etcd election). The leader determines the ranks
  of all the members; a map of member names to their integer rank `map[string]int`. The ranks are assigned from 0 up to 
  the size of the rink. i.e. if there are two members "alice" and "bob", the rink ranks might be `{"alice":0, "bob":1}`.
  
*`leader`*: The leader does the following to maintain the rink ranks:
 - It watches all member keys and reacts to members joining and leaving.
 - It maintains a timer of a future rebalance due to waiting members.
 - If a member leaves, it promotes any waiting member to its rank.
 - It rebalances ranks when new members have waited long enough (and not been promoted due to other members leaving).
