# Control plane, reconciliation, and lifecycle

## Registration and heartbeats

Nodes register UUID, agent address, capacity, OS/architecture, labels, volume inventory, and optional WireGuard identity. Periodic heartbeats refresh node status and report allocation generation, task, phase, health, each task's observed namespace-network address when present, ports, capabilities, and version. The control plane retains endpoint observations per task rather than collapsing a multi-task allocation onto whichever task was reported first. A successful heartbeat is acknowledged without returning desired state. After three missed heartbeat intervals a healthy node is marked unhealthy.

## Scheduling algorithm

For each task-group deficit, `Schedule`:

1. sorts nodes by UUID for deterministic decisions;
2. excludes non-healthy nodes and constraint/host-volume mismatches;
3. sums all colocated task CPU/memory requirements and existing usage;
4. excludes nodes whose declared capacity would be exceeded;
5. selects the highest post-placement normalized CPU/memory utilization (best fit), using the number of same-group replicas as an anti-affinity tie-breaker.

The result may contain fewer placements than requested. Reconciliation will try later as cluster conditions change. No preemption occurs.

## Reconciliation

The leader serializes reconciliation runs. It normalizes allocations, expires unhealthy nodes, ignores terminal records, respects retry timestamps, stops allocations whose job disappeared, and detects outdated job revisions. It also compares heartbeat observations with desired allocations and stops orphaned or stale generations through the same reconciler action path after the leader-recovery grace period. It then scales each group down/up and executes agent actions outside the state scan.

`recreate` immediately stops outdated allocations. `rolling` marks them draining, explicitly tells the agent reconciler to suppress automatic restarts, creates at most `max_parallel` non-healthy replacements at a time, and sends the normal stop operation only as healthy new capacity makes them surplus. A zero/omitted strategy becomes recreate; omitted/nonpositive rolling parallelism is effectively one.

Agent failures receive deterministic exponential backoff with jitter, capped by the reconciliation attempt rules. Old leadership epochs, generations, and mismatched execution hashes produce protocol-level conflict codes rather than silently changing a newer allocation.

## Lifecycle and diagnostics

Lifecycle phases are pending, placed, starting, running, stopping, stopped, failed, and lost. Health is unknown, healthy, or unhealthy. Allocation records also preserve reason/message, attempt count, creation/transition times, next retry, and a bounded event history. Health probes support HTTP, TCP, and script checks. Restart policy is enforced per allocation within its configured window.

After leader election there is a recovery grace period. A node that remains unavailable long enough causes its allocations to transition to lost, allowing replacement without prematurely duplicating work during transient leadership changes.

## Catalog and discovery

Reconciliation refreshes the catalog from eligible allocation endpoints. Namespace-networked tasks advertise only their observed workload address from the agent; a missing namespace observation never falls back to the node address. Host-networked tasks use the node address. The allocation-level address remains available only when its routable task endpoints agree on one address; task-level endpoint data is retained separately, and ambiguous allocations are omitted from allocation-level catalog discovery. Catalog entries retain namespace, job/group, labels, address, ports, and status. Queries can be namespace scoped and label filtered (`key:value`). DNS maps service-shaped names to IPv4 addresses with a short TTL. Proxy sync polls label-filtered allocations, keeps healthy endpoints, honors positive `trellis/weight`, atomically rewrites rendered output, and optionally reloads the proxy.
