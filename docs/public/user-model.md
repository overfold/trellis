# Trellis user model

Trellis has several interfaces — YAML manifests, the `trellisctl` CLI, the Trellis Console, and the HTTP API — but they describe the same model. This page defines the user-facing vocabulary those interfaces should share.

## The hierarchy

A Trellis deployment is a **cluster**. A cluster contains one or more **nodes** and workloads organized into **namespaces**.

A namespace is the tenant and security boundary for workload-facing resources. It is not a separately lifecycle-managed object: users do not create or delete namespaces before using them. Applying a job names its namespace, and Trellis can discover namespace names currently referenced by desired jobs. Inside a namespace, users define **jobs**. A job is desired state: it says what should be running, not what happens to be running at this instant.

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
| **Namespace** | Tenant, authorization, discovery, and workload-isolation boundary; named by jobs rather than managed through create/delete lifecycle. |
| **Job** | Named desired workload in a namespace. |
| **Job manifest** | The YAML document humans author and apply to create or update a job. |
| **Revision** | The version of a job produced by an apply. |
| **Task group** | Placement, scaling, restart, and update unit inside a job. |
| **Task** | One container definition, including its network attachment, inside a task group. |
| **Allocation** | Runtime instance created by Trellis to satisfy desired task-group capacity. |
| **Lifecycle** | Execution phase of an allocation: placed, starting, running, stopping, stopped, failed, or lost. |
| **Health** | Readiness/health of a running allocation: unknown, healthy, or unhealthy. |
| **Drain** | Prevent new work on a node and move existing allocations away when replacements can be scheduled. |
| **Secret** | Namespace-scoped named secret material referenced by jobs but not stored in manifests. |

## Manifest versus API representation

**YAML is the canonical human-authored representation of a job.** Documentation, examples, the CLI, and Console authoring should call it a **job manifest** and show YAML by default.

The HTTP API uses JSON because it is a transport API. JSON field names intentionally mirror the YAML schema, but JSON should be described as the **API representation**, not as a second job format users must learn.

This distinction keeps the workflow simple:

```text
write YAML manifest → apply → Trellis creates a revision → inspect job → inspect allocations when needed
```

## Desired state and runtime state

The interfaces should keep desired and runtime state visibly separate:

- A **job manifest** and **revision** describe desired state.
- An **allocation lifecycle** describes whether Trellis has placed, started, stopped, failed, or lost runtime work.
- **Health** describes whether running work is ready/healthy.

For example, an allocation can be `running` and `unhealthy`. Interfaces should not collapse those into one ambiguous status.

## User-facing actions

Use the same verbs across interfaces:

- **Apply** a job manifest to create a job or advance its revision.
- **Delete** a job to remove its desired state and stop its allocations.
- **Drain** / **undrain** a node for maintenance.
- **Inspect** a job for desired-versus-observed state.
- **Inspect an allocation** for placement, lifecycle, health, events, and task logs.
- **Set**, **describe**, and **delete** secrets.

Documentation and UI copy use these canonical terms; CLI aliases are convenience spellings rather than a second vocabulary.

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
