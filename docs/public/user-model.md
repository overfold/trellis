# Trellis user model

Trellis has several interfaces — YAML manifests, the `trellisctl` CLI, and the HTTP API — but they describe the same model. This page defines the user-facing vocabulary those interfaces should share.

## The hierarchy

A Trellis deployment is a **cluster**. A cluster contains one or more **nodes** and workloads organized into **namespaces**.

A namespace is the tenant and security boundary for workload-facing resources. It is not a separately lifecycle-managed object: users do not create or delete namespaces before using them. Applying a job names its namespace, and Trellis can discover namespace names that currently have desired jobs, secrets, or namespace-scoped credentials. Inside a namespace, users define **jobs**. A job is desired state: it says what should be running, not what happens to be running at this instant. Namespace scope does not filter valid manifest capabilities; operators accepting manifests from untrusted tenants must provide that admission layer in a frontend. See [Multitenancy and trust boundaries](multitenancy.md).

A job contains one or more **task groups**. A task group is the unit Trellis places, scales, restarts, and updates together. A task group contains one or more **tasks**, where each task describes a container and selects its own network attachment.

Trellis turns desired task-group replicas into **allocations**. Allocations are runtime instances managed by the scheduler. Users normally reason about jobs and task groups first and drill into allocations when observing or diagnosing execution.

In short:

```text
cluster
├── nodes
└── namespaces
    └── jobs
        └── task groups
            ├── tasks
            └── allocations (runtime instances)
```

## Canonical terms

| Term | User-facing meaning |
| --- | --- |
| **Cluster** | One Trellis deployment operated as a unit. |
| **Node** | A machine running `trellis` and participating in the cluster. |
| **Namespace** | Tenant, authorization, discovery, and workload-isolation boundary for Trellis-owned resources; named by jobs rather than managed through create/delete lifecycle. |
| **Job** | Named desired workload in a namespace. |
| **Job manifest** | The YAML document humans author and apply to create or update a job. |
| **Version** | The job specification produced by an accepted apply. Every change advances it, including scaling and label changes. |
| **Revision** | The execution content of a job. It advances only when a change replaces allocations, such as a new image or command. |
| **Task group** | Placement, scaling, restart, and update unit inside a job. |
| **Task** | One container definition, including its network attachment, inside a task group. |
| **Allocation** | Runtime instance created by Trellis to satisfy desired task-group capacity. |
| **Lifecycle** | Execution phase of an allocation: placed, starting, running, stopping, stopped, failed, or lost. |
| **Health** | Readiness/health of a running allocation: unknown, healthy, or unhealthy. |
| **Drain** | Prevent new work on a node and move existing allocations away when replacements can be scheduled. |
| **Secret** | Namespace-scoped named secret material referenced by jobs but not stored in manifests. |

## Manifest versus API representation

**YAML is the canonical human-authored representation of a job.** Documentation, examples, and the CLI should call it a **job manifest** and show YAML by default.

The HTTP API uses JSON because it is a transport API. JSON field names intentionally mirror the YAML schema, but JSON should be described as the **API representation**, not as a second job format users must learn.

This distinction keeps the workflow simple:

```text
write YAML manifest → apply → Trellis creates a version (and a revision when execution changes) → inspect job → inspect allocations when needed
```

## Desired state and runtime state

The interfaces should keep desired and runtime state visibly separate:

- A **job manifest**, its **version**, and its **revision** describe desired state.
- An **allocation lifecycle** describes whether Trellis has placed, started, stopped, failed, or lost runtime work.
- **Health** describes whether running work is ready/healthy.

For example, an allocation can be `running` and `unhealthy`. Interfaces should not collapse those into one ambiguous status.

### Lost allocations

An allocation becomes **lost** when its node stops sending heartbeats for longer than the allocation loss timeout (45 seconds by default; the operator changes the `allocation_loss_timeout` cluster setting with `trellisctl cluster set-reconciliation`), and the current leader has itself been leader for at least 30 seconds. The second condition gives nodes time to report to a newly elected leader before anything is declared lost. A newly elected leader counts a node's silence from the start of its own leadership, so a failover can delay, but never hasten, an allocation becoming lost. A lost allocation no longer counts toward its task group's `count`, so Trellis places a replacement.

Lost is terminal, like `stopped` and `failed`. If the node comes back and still runs the lost allocation's containers, Trellis does not adopt them again: the allocation stays `lost` and never counts toward the group. While its allocation record is retained, the containers are not stopped right away: Trellis keeps them running until the group has enough `running` replacements, then stops them. Older records beyond the per-task-group terminal retention limit are pruned; if that container is later reported, Trellis stops it as an observed orphan. The kept containers are not part of service discovery. They are stopped sooner if they stand in the way of a replacement: for example, when a replacement needs the same host port on the same node, or when the group can only be placed on that node (such as a volume bound to it) and would not otherwise fit.

## User-facing actions

Use the same verbs across interfaces:

- **Apply** a job manifest to create a job or advance its version.
- **Delete** a job to remove its desired state and retained version history, and stop its allocations.
- **Drain** / **undrain** a node for maintenance.
- **Inspect** a job for desired-versus-observed state.
- **Inspect an allocation** for placement, lifecycle, health, events, and task logs.
- **Set**, **describe**, and **delete** secrets.

Every apply that changes the job specification advances the job's **version** and records the new specification in its history. Changes to execution content (for example an image, command, environment, resources, or networking) also advance the **revision** and roll allocations according to the update policy; label, `count`, and update-policy-only changes keep the revision, so scaling does not restart running allocations but still appears in history. Applying an unchanged manifest creates neither. Trellis keeps the 10 newest versions of each live job for inspection and backup. Deleting a job removes its history, so applying the same name later starts again at version 1 and revision 1.

Applies are fenced by version. `trellisctl jobs apply` sends the version their plan was computed against, and Trellis rejects the apply with a conflict when the job was changed, created, or deleted in between, so two concurrent pipelines cannot silently overwrite each other. Plan again to review the current state and apply that. Because a recreated job starts again at version 1, a delete followed by a recreation that reaches the same version before the stale apply arrives is not detected.

Documentation and CLI output use these canonical terms; CLI aliases are convenience spellings rather than a second vocabulary.

## What is not part of the basic model

Some concepts are important to operating or debugging Trellis but are implementation details rather than the primary user model:

- Raft terms and leadership epochs
- raw HTTP endpoint layout
- agent/control-plane RPC boundaries
- reconciliation generations and fencing tokens
- internal registration and heartbeat operations

They should remain observable where useful, especially in advanced diagnostics and developer documentation, but ordinary workflows should not require users to understand them first.

## Interface contract

The first-party interfaces should follow these rules:

1. **Use the vocabulary on this page.** Do not invent a second term for an existing concept.
2. **Prefer jobs over allocations in top-level workflows.** Allocations are the diagnostic/runtime layer.
3. **Use YAML for human-authored job manifests.** JSON is the API representation.
4. **Show lifecycle and health separately.** Do not turn them back into one status field.
5. **Keep namespace scope visible.** A user should be able to tell which namespace a job, allocation, or secret belongs to.
6. **Hide implementation mechanics from the happy path.** Expose them in advanced operations and diagnostics instead.
7. **Keep destructive verbs consistent.** A job is applied or deleted; a node is drained, undrained, or removed from the cluster.

This is a UX contract, not a request to make the scheduler more opinionated. Trellis can keep its small primitive resource model while presenting those primitives consistently.

[Documentation index](../README.md) · [Previous: Learning path](learning-path.md) · [Next: Core concepts](core-concepts.md)
