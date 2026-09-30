# Core concepts

Start with the [Trellis user model](user-model.md) for the vocabulary shared by manifests, the CLI, and examples. This page explains how those user-facing concepts behave.

## Cluster and nodes

A **cluster** is one Trellis deployment operated as a unit. Each machine in the cluster is a **node** running `trellis`. Nodes report capacity, labels, registered local volumes, runtime state, and health. From an operator's perspective a node is healthy, unhealthy, or draining.

Trellis uses Raft internally to replicate desired state and elect a control-plane leader. Leadership is an implementation detail for normal workload workflows. See [Multi-node clusters](multi-node.md) for how nodes join, how many to run, and what happens when they fail, and the developer documentation when debugging the consensus layer itself.

## Namespaces and jobs

A **namespace** is the tenant, authorization, discovery, and workload-isolation boundary for jobs, allocations, secrets, volume identities, and namespace tokens. It scopes the resources Trellis owns, but does not act as admission policy for manifest capabilities such as host networking or absolute host paths. See [Multitenancy and trust boundaries](multitenancy.md) when accepting workloads from untrusted tenants.

A **job** is named desired state inside a namespace. Humans define a job with a YAML **job manifest**. Applying a manifest creates the job or advances its **revision** when desired state changes.

## Task groups, tasks, and allocations

A **task group** is the unit of placement, scaling, restart policy, and update strategy. `count` is the desired number of group replicas. A task group contains one or more **tasks**, each describing a container and its own network attachment.

Trellis creates runtime **allocations** to satisfy desired task-group capacity. Users normally inspect the job first and drill into allocations when they need placement, lifecycle, health, retry, port, event, or log details.

## Desired state versus runtime state

Keep these concepts separate when reading any Trellis interface:

- The job manifest and revision are **desired state**.
- Allocation **lifecycle** is execution state: `placed`, `starting`, `running`, `stopping`, `stopped`, `failed`, or `lost`.
- Allocation **health** is readiness/health state: `unknown`, `healthy`, or `unhealthy`.

An allocation can therefore be `running` and `unhealthy`. Lifecycle and health are independent parts of the canonical allocation state.

## Scheduling

The scheduler considers only healthy, non-draining nodes. It filters on `os`, `arch`, custom label constraints, registered volume locality, CPU millicores, and memory bytes. Among the remaining nodes it places each replica on the node with the fewest replicas of the same task group, counting replicas already placed there (draining replicas that are being replaced do not count). Among nodes with equally few replicas it prefers the one that would be most utilized after placement (best fit), and any remaining tie goes to the lowest node ID, so the same inputs always produce the same placements. Spreading is soft: when constraints, volumes, node ports, or capacity leave only some nodes eligible, replicas share those nodes. Resource values of zero on a node mean capacity is not enforced for that dimension.

## Reconciliation and failure handling

Trellis continuously compares desired jobs with runtime allocations and converges the cluster toward desired state. Start and stop operations are idempotent; failed control operations are retried with bounded exponential backoff and jitter. Allocation generations and leadership fencing prevent stale work from overwriting newer decisions.

Those generation/fencing details are useful diagnostics, but they are not separate workload resources users need to model in manifests.

## Networking and discovery

Each task selects its attachment through `networking.mode`. Omission or `namespace` joins the private Trellis network for the workload namespace, with service DNS and internet egress through the node; `none` gives the task loopback only; `host` joins the node network directly and bypasses Trellis tenant networking. Namespace networking is implemented with a separate WireGuard pathway per namespace, which every node provides: each active namespace has its own bridge, subnet, WireGuard interface, peer set, and UDP port on a node. The port remains stable while that namespace has desired or active networked allocations and is released after the last one is gone. `runsc` (gVisor) can be added for additional syscall-level sandboxing but is not required; nodes report whether they support it when they register and on heartbeats, and Trellis derives that placement requirement from `runtime: runsc`. A task's `ports` are published on the node in namespace mode (optionally on a different `host_port`) or reserved on the node in host mode; either way the scheduler keeps each node port to one allocation per node. Healthy allocation endpoints enter the service catalog. Namespace-networked workloads can resolve healthy services in their own namespace as `group.job.namespace.trellis`; cross-namespace names return no records. Trellis configures one workload DNS endpoint consistently for containers: authorized discovery names are answered locally and ordinary DNS names are forwarded to the node's upstream resolvers.

If desired replicas cannot be placed, Trellis keeps an allocation in `pending` for each unmet replica and records the current scheduler filter in its diagnostic. The reasons distinguish unavailable healthy nodes, constraint mismatches, unavailable volume owners, missing capabilities, host-port conflicts, and insufficient CPU or memory. `trellisctl jobs status` displays the reason and explanatory message. The pending record is reused while conditions remain unchanged and becomes the placed allocation when eligibility returns, avoiding duplicate records and repeated diagnostic churn.

## Persistence and secrets

A volume has a stable namespace-scoped `name`, an explicit node-side `host_path`, and a `container_path`. The first allocation using an unseen volume name establishes its node registration; future allocations using the same `(namespace, name)` are constrained to that node. `@/path` resolves below Trellis's per-namespace volume root, while an absolute host path is used verbatim and therefore does not receive filesystem-level namespace isolation. Volume registration provides locality, not replication or migration.

**Secrets** are namespace-scoped named values referenced by job manifests without embedding their plaintext in YAML. Trellis encrypts stored secret records and delivers values from verified tmpfs storage as environment variables or files below `/run/trellis-secrets/`. Managed environment values are absent from containerd's persisted OCI metadata, but necessarily become part of the live application process environment; prefer file delivery when supported. Updating a secret does not mutate already-running allocations.

## Updates

`recreate` stops outdated allocations before replacements. `rolling` starts bounded replacements and removes draining old allocations after replacements are healthy.

Blue/green and canary releases are deployment patterns composed from ordinary jobs, task groups, labels, health checks, and routing. They are not additional Trellis resource types. See the [Cookbook](cookbook.md) and [examples](../../examples/README.md).

[Documentation index](../README.md) · [Previous: User model](user-model.md) · [Next: Job manifest reference](job-specification.md)
