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
- Rebalancing results in all roles beign reassigned.
- Stickyness is provided by a configurable rebalance delay. A rebalance 
  due to a joined member is only triggered after the rebalacne delay. If another member leaves
  during this time, its rank will immediately be transferred without affecting other members.
- Kubernetes rolling deploy with `MaxUnavailable=0, MaxSurge>0` will smoothly transfer roles from
  old members to new members.
  
## Usage

Example 1: Given multiple replicas of a service, `rink` will ensure that at most one replica 
is always the "leader" even as other replicas start, stop or fail.

Note error handling is omitted for brevity.

```
cl, err := etcd_clientv3.New()
s, err := concurrency.NewSession(cl) // Session identifies the member
defer s.Close()                      // Leave the rink when done.
r, err := rink.New(s, "my_rink")

for {
  // Block until this replica assumes the "leader" role  
  ctx := r.AwaitRole("leader")

  // ctx is closed when the role is unassigned.

  // leadByExample runs until first error is encountered (err != nil).
  err := leadByExample(ctx)
  if ctx.Err() != nil {
    log.Printf("leader role unassigned: %v", err)
  } else {
    log.Printf("lead error: %v", err)
  }
  
  // In both cases, just try again.
}
```
Example 2: Given multiple `processor` services that runs `jobs` (note `jobs` can be both short or long lived). 
`rink` will ensure that `jobs` are evenly distributed among `processors`.

```
cl, err := etcd_clientv3.New()
s, err := concurrency.NewSession(cl) // Session identifies the member
defer s.Close()                      // Leave the rink when done.
r, err := rink.New(s, "my_rink")

for {
  jobs, _ := jobs.ListPendingJobs()
  
  for _, job := range jobs {
    ctx, ok := r.GetRole(job.ID)  
    if !ok {
      // Another processor has the role for this job now.
      continue
    }
  
    // Only this processor will process this job now.
    // ctx is closed when the role is unassigned.
    err := processJob(ctx, job)
    if err != nil {
      // Job completed. ListPendingJobs should not return this anymore.
    } else if ctx.Err() != nil {
      log.Printf("job role unassigned: %v", err)
    } else {
      log.Printf("job process error: %v", err)
    }
  }

  if len(jobs) == 0 {
    time.Sleep(time.Minute) // Backoff if no jobs
  }
}
```

## Concepts and building blocks

`rink` leverages etcd's `concurrency` package as underlying building blocks. It also introduces some concepts
of its own.

*`concurrency.Session`*: An etcd session identifies a member and acts as liveliness proxy.
A member joins the rink with an active session. A member leaves the rink by closing the session.
A session is implemented in etcd with an opaque key and lease (default TTL of 60s). The session 
is kept alive by an internal goroutine that periodically extends the lease. If the session is
closed, the key is deleted. If the lease times out, the key is automatically deleted and the session is
assumed cancelled. `rink` uses the `Session.LeaseID` is link member keys to their session.

*`concurrency.Election`*: An etcd election elects a leader that may proclaim values. These proclamations
 appear as an append-only log of values to all members of the election. All members of `rink` join an election. 
 One member will therefore be the leader. The leader maintains the rink; detecting member joins and leaves, 
 promoting and rebalacing ranks, and "proclaiming" subsequent version of the rink state. The election
 proclamations are therefore a immutable log of rink states.
 
*`member keys`*: When joining the rink, each member creates a key `{rink_keyprefix}/members/{name}` attached to
  its session lease. The value of the key is its `RebalanceAt` timestamp; implying the rebalance delay it is 
  willing to wait. Since the member keys are linked to the session lease, they are automatically deleted when the session
  is closed (or times out).

*`immutable rink state log`*: One of the members is always the leader (of the etcd election). The leader maintains
the rink state; a map of ranks by member names `map[string]int`. Subsequent versions of the rink state is proclaimed
on the etcd election. Members observe these proclamations (similar to stream consumers), updating their own state accordingly. 
Note this does mean that members are eventually consistent, with possibiilty of two members temporarily assuming the same role.
  
*`leader`*: The leader does the following to maintain the rink state:
 - It watches all member keys and reacts to members joining and leaving.
 - It maintains a timer of a future rebalance due to waiting members.
 - If a member leaves, it promotes any waiting member to its rank.
 - If required it rebalances ranks. 
 
## Comparison to guv and scheduler

1. Rink and guv have sticky roles while scheduler redistributes roles very often: 
 - Scheduler redistributes roles on each join and each leave. 
 - Given a rolling deploy of 5 instances, scheduler redistributes roles 10 times.
 - It is very difficult to determine what is happening during the 10 consecutive redistributions.
 - Delays or bugs resulting from this would be difficult to identify and fix.
 - Depending on application's role duration requirements, it might not get any work done during the deploy. 
 - Error logging will probably be high during this time.
 - Not rink doesn't have sticky roles on leaves.
 
2. Rink and guv supports stable and immediate role succession:
 - Given Kubernetes rolling deploys with `maxUnavailable=0` and `maxSurge>0`
 - Roles are immediately assumed by new waiting pods as old pods are stopped.
 - Each role is only moved once.
 - This provides very stable and strong role HA guarantees.  

2. Rink and guv and scheduler2 supports arbitrary roles:
 - They all don't manages roles, it only manages member rank. 

4. Rink is simpler to understand, debug and implement (imho): 
 - It has much less features than Guv.
 - It leverages etcd concurrency package.
 - Rink has a single leader maintaining state, compared to Guv and Schedeler
   where all members update state requiring more complex atomic guarded operations. 

5. Rink has much less features than Guv.
 - Guv also has sticky leaves, while rink only has sticky joins.
 - Guv therefore supports staggered rebalances when cluster shrinks.
 - Guv therefore supports sticky roles if a pod restarts.
 - Rink and guv support staggered rebalance when cluster grows.
 - Rink only supports smooth roles transfers on rolling deploy described above.
 - Guv has explicit state generations/versions, members sync to each version.
 - When rebalacing, guv can therefore first unassigns all ranks to ensure no roles overlap. 
 - Rink members are eventualy consistent, they have no mechanism to sync to specific version. 
 - Rink therefore has a probability of role overlap; multiple members assuming the same role.
 - This is considered "good enough", since it should only occur during failures, it should only 
   be temporary (few seconds max) and reflex consumers should be able to handle it (idempotent). 
