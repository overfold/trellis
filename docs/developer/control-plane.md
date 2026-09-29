# Control plane, reconciliation, and lifecycle

## Registration and heartbeats

Nodes register UUID, agent address, capacity, OS/architecture, labels, volume inventory, and optional WireGuard identity. Periodic heartbeats refresh node status and report allocation generation, task, phase, health, each task's observed namespace-network address when present, ports, capabilities, and version. The control plane retains endpoint observations per task rather than collapsing a multi-task allocation onto whichever task was reported first. A successful heartbeat is acknowledged without returning desired state. After three missed heartbeat intervals a healthy node is marked unhealthy. The leader keeps each node's latest observed allocation generations and their aggregated phase for reconciliation.

## Control-plane membership

Every node runs the same daemon and is a Raft member, but only a bounded set of members vote. The target voter count is the largest odd number not above the member count or five: one member votes in a one- or two-member cluster, three in a three- or four-member cluster, and five from five members up. Five voters tolerate two failures; more would only add write fan-out and quorum size. Even counts are avoided because they tolerate no more failures than one voter fewer.

`POST /v1/raft/join` always adds the node as a non-voter (`AddNonvoter`); a rejoining voter keeps its vote. The leader moves the voter set toward its desired size, one configuration change at a time, every 10 seconds once it has led for the 30-second recovery grace, and immediately after an operator removal. Each change is a compare-and-set against the configuration index it was planned from, so concurrent membership changes cannot combine into an unplanned configuration. The planner (`planMembership`) is a pure function of the configuration and the leader's node observations and breaks ties by node ID:

- a non-voter is eligible when its node is `healthy` (not draining or unhealthy) and its latest heartbeat, at most 30 seconds old, reported a Raft applied index within 256 entries of the leader's applied index at receipt. Only eligible non-voters are promoted;
- a voter whose node has been silent for 5 minutes (measured from no earlier than the current leader's election) is gone;
- the desired voter count is the largest odd number not above the target or the number of members that could vote now (voters that are not gone plus eligible non-voters). Voters are promoted up to it and demoted down to it, so an even voter set is always brought back to odd. Demotion never chooses the leader and prefers gone voters, then unreachable ones, then ineligible ones;
- a gone voter with an eligible replacement is swapped: the non-voter is promoted first and the resulting surplus then demotes the gone voter, so the number of reachable voters never shrinks. The gone node stays a member and can vote again later.

Removal (`DELETE /v1/raft/members/{id}`) of a voter promotes an eligible non-voter before removing it. Before changing anything, it is refused with `ErrMembershipUnsafe` (`409`) when the reachable remaining voters (the leader, the replacement, and voters whose nodes heartbeated within 30 seconds) are not a majority of the remaining voters, because such a configuration would leave the leader unable to commit, including the removal itself. Raft leadership transfer only selects voters.

Heartbeat progress is a leader-local renewable observation; nothing about eligibility is persisted. Configuration changes are ordinary Raft configuration entries, so every member applies the same membership.

## Scheduling algorithm

For each task-group deficit, `Schedule`:

1. sorts nodes by UUID for deterministic decisions;
2. excludes non-healthy nodes and constraint/host-volume mismatches;
3. sums all colocated task CPU/memory requirements and existing usage;
4. excludes nodes whose declared capacity would be exceeded;
5. selects the highest post-placement normalized CPU/memory utilization (best fit), using the number of same-group replicas as an anti-affinity tie-breaker.

The result may contain fewer placements than requested. Reconciliation will try later as cluster conditions change. No preemption occurs.

## Reconciliation

The leader serializes reconciliation runs. It normalizes allocations, expires unhealthy nodes, ignores terminal records, respects retry timestamps, stops allocations whose job disappeared, and detects outdated job revisions. It also compares heartbeat observations with desired allocations and stops orphaned or stale generations through the same reconciler action path after the leader-recovery grace period. It then scales each group down/up and executes agent actions outside the state scan. Actions remain ordered per node but run concurrently across nodes; finite agent calls have whole-response deadlines, so an unreachable agent or a response body that stops making progress cannot indefinitely block reconciliation for the rest of the cluster. Streaming log calls remain governed by their caller's context after the response headers arrive.

Drain and resume intent is saved before an agent sees it. Every reconciliation pass redelivers the drain of a draining allocation. Undrain saves the resumed allocations (clearing `draining` and advancing `drain_sequence`) and the node's healthy status, then delivers each resume. If a save fails, no resume is sent; the node stays draining and the next pass re-drains at a higher sequence. If delivery fails, the leader keeps an in-memory, per-term record of which resume sequence each agent acknowledged and redelivers unacknowledged resumes to running allocations on later passes; a new leadership term starts with an empty record and redelivers each saved resume once. Start requests carry the drain state themselves, so starting allocations need no separate redelivery.

`recreate` immediately stops outdated allocations. `rolling` marks them draining, explicitly tells the agent reconciler to suppress automatic restarts, creates at most `max_parallel` non-healthy replacements at a time, and sends the normal stop operation only as healthy new capacity makes them surplus. A zero/omitted strategy becomes recreate; omitted/nonpositive rolling parallelism is effectively one.

Agent failures receive deterministic exponential backoff with jitter, capped by the reconciliation attempt rules. Old leadership epochs, generations, and mismatched execution hashes produce protocol-level conflict codes rather than silently changing a newer allocation.

## Lifecycle and diagnostics

Lifecycle phases are pending, placed, starting, running, stopping, stopped, failed, and lost. Health is unknown, healthy, or unhealthy. Allocation records also preserve reason/message, attempt count, creation/transition times, next retry, and a bounded event history. Health probes support HTTP, TCP, and script checks. Restart policy is enforced by the agent per allocation within its configured window. Exhausting the budget is terminal for that allocation generation: the agent stops restarting it, persists the exhaustion so it survives agent restarts, and reports phase `failed` with health `unhealthy` and reason `restart_budget_exhausted`, which the server records on the allocation. Restart attempts and their window belong to the allocation generation: a start retry for the same generation, including one after recovery hands back a stopped task, keeps them, and only a new generation starts with a fresh budget. A failed task fails its whole group in the server's aggregation. The server treats the allocation as inactive, stops the observed container, and places a replacement to satisfy `count` after the task group's replacement backoff. Heartbeats never move a failed, lost, or stopped allocation to another phase, so a task a node still reports for it cannot make it count toward the group again or displace its replacement, and a task failure observed during a server-initiated stop does not override that stop. A start retry for an exhausted generation is rejected by the agent with the `restart_budget_exhausted` operation code before any task is touched, and the server records the allocation as failed with that reason.

### Replacement backoff

Count reconciliation delays replacements for a task group whose allocations keep failing. The leader keeps one replacement-backoff record per job task group (`trellis/<cluster>/replacement-backoffs/<namespace>/<job>/<group>`). Each reconciliation pass derives the next record from the previous one and the group's allocations without mutating either:

- a `failed` allocation counts as one failure the first pass that sees it, if it belongs to the job's current revision and is not draining; `stopped` and `lost` allocations never count. The record remembers the IDs of the retained failed allocations it has already seen, so counting does not depend on the clocks of whichever leaders recorded the failures, and the list shrinks as records are pruned;
- after `n` consecutive failures, replacements of the group's failed allocations wait until `min(10s × 2^(n-1), 5m)` after the pass that recorded the failure (10s, 20s, 40s, 80s, 160s, then 5m);
- the count resets to zero when an allocation created after the latest counted failure has been `running`, and not `unhealthy`, for 10 minutes, and whenever the job revision changes. The seen list survives resets, so retained failed records are never counted twice.

The backoff delays only placements that replace counted failed allocations. The record carries `delayed_replacements`, the number of failures counted while the backoff is active. For a group deficit `d` (desired count minus active allocations, after rolling-update limits), the pass withholds `min(d, delayed_replacements)` placements and places the rest immediately, so a deficit from an allocation lost with its node, or from a higher `count`, does not wait. The pass also lowers `delayed_replacements` to the group's missing capacity, so a failure whose capacity was scaled away is not held against a later count increase. The first reconciliation pass at or after `next_replacement_at` places the whole deficit and `delayed_replacements` returns to zero; failures counted in that pass start a new delayed set. The failure count and its reset rules are unchanged. While the backoff is active the pass still trims excess pending allocations. A record whose group is no longer desired has its failures cleared and is deleted together with the group's last allocation record.

An operator can clear a group's backoff with `POST /v1/jobs/{name}/groups/{group}/replacement-backoff/reset` (`trellisctl jobs reset-backoff`, or **Reset backoff** in the dashboard). The leader writes the cleared record (zero failures, no `next_replacement_at`, nothing delayed, the seen list kept so retained failures are never counted again) through the state store, publishes `job.replacement_backoff_reset`, and runs a reconciliation pass that places the withheld replacements.

These are server defaults (`DefaultReplacementPolicy`), not manifest fields.

### Terminal record retention

Allocation records are otherwise never deleted, so each pass also prunes terminal records. Per `(namespace, job, group)` the five newest `stopped`, `failed`, or `lost` records by transition time (allocation ID breaks ties) are always retained. An older record is deleted only when no node can still hold its container or resources: the allocation was never placed, its node has been removed from the cluster (it is no longer in the replicated node registry), or its node is healthy or draining and has sent a heartbeat after the allocation became terminal that lists no generation of it. A node that is down but still registered may return with the container, so its records are kept. The removed-node rule depends only on replicated state: a leader that kept the node pointer and one that reloaded the record after the removal, and so sees no node at all, decide alike. Records updated in the same pass are never pruned in that pass. Allocations of deleted jobs follow the same rule.

Allocation updates, new allocations, pruned records, and backoff changes from one pass are committed as a single atomic batch. Every value, including timestamps, is chosen by the leader and carried in the Raft entry; the FSM only applies the puts and deletes, so log replay and snapshot restore produce identical state on every node. A new leader reloads the committed backoff records, so failover neither shortens nor resets a backoff.

Operators see the state as `replacement_backoff` in job status (`trellisctl jobs status` prints a **Replacement backoff** table), a `job.replacement_delayed` event on `/v1/events` whenever a failure is counted, and the `trellis_replacement_backoff_failures` and `trellis_replacement_backoff_remaining_seconds` gauges labelled by namespace, job, and group.

### Lost allocations

A node is marked unhealthy after three missed heartbeat intervals (30s). Its non-terminal allocations become lost once the node's last heartbeat is at least the allocation loss timeout old (`DefaultAllocationLossTimeout`, 45s, configurable per server with `allocation_loss_timeout` / `--allocation-loss-timeout` between 30s and 24h) and the leader has held leadership for at least `leaderRecoveryGrace` (30s). The recovery grace applies to the orphan and stale-generation observation stops as well. It avoids duplicating work during transient leadership changes: a new leader first gives nodes a chance to heartbeat to it.

Lost is terminal. Heartbeats never move a lost allocation to another phase, and it never counts toward its group again. While its allocation record is retained, when its node returns and reports the lost generation's container `running`, reconciliation treats it as a retained original rather than stopping it immediately as an unowned observation:

- each group keeps up to `count` minus its running non-draining allocations of its retained originals, in allocation ID order. Originals are kept only while their job and group are desired, and only if they belong to the current revision or the group uses rolling updates. Once enough replacements are `running`, the remaining originals are stopped through the normal `stop_observed` action;
- placement treats retained originals as occupying their node's host ports, CPU, and memory, so a replacement is not placed where it could not start beside them;
- a retained original never blocks a replacement. If the originals are the only reason fewer replacements fit, such as a group pinned to the original's node by a host volume that needs the same host port, the originals on the nodes the unobstructed placement would use are released (same group first, then by ID) until the unobstructed count fits. A retained original that holds a host port an allocation already placed on its node needs is also released. Releasing trades a short gap for progress. Keeping the original would deadlock: the replacement could never start, so the original would never be stopped;
- the stops of released originals run before every other action of the pass, so a replacement starts only after the original holding its port is stopped.

These decisions are derived each pass from the allocation snapshot and the latest node observations. Nothing about them is persisted, and scheduler inputs are never mutated. Retained originals are added only to placement occupancy, never to the allocations counted for replica spreading. Any originals not kept by a group are stopped in the same pass.

Terminal retention keeps only the five newest allocation records per task group, including on registered unavailable nodes. If an older pruned allocation is later reported, it has no desired record and is stopped through the normal `stop_observed` orphan path.

## Catalog and discovery

Reconciliation refreshes the catalog from eligible allocation endpoints. Namespace-networked tasks advertise only their observed workload address from the agent; a missing namespace observation never falls back to the node address. Host-networked tasks use the node address. The allocation-level address remains available only when its routable task endpoints agree on one address; task-level endpoint data is retained separately, and ambiguous allocations are omitted from allocation-level catalog discovery. Catalog entries retain namespace, job/group, labels, address, ports, and status. Queries can be namespace scoped and label filtered (`key:value`). The node-authenticated internal discovery endpoint returns only namespaces with active allocations assigned to that node instead of distributing the cluster-wide catalog. The node resolver then binds each `*.trellis` query to the namespace network that owns the caller's source address, so a caller-selected DNS name cannot cross the namespace boundary. DNS maps authorized service-shaped names to IPv4 addresses with a short TTL. Proxy sync polls label-filtered allocations, keeps healthy endpoints, honors positive `trellis/weight`, atomically rewrites rendered output, and optionally reloads the proxy.

The node resolver handles UDP queries concurrently so a slow forwarded query
cannot block local discovery or unrelated forwarding. Admission is bounded at
256 in-flight UDP queries and 128 active TCP connections. UDP overload receives
`SERVFAIL`, TCP overload is closed, and cancellation closes listeners, accepted
connections, and upstream exchanges before the resolver waits for its workers.
