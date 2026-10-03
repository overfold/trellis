# Architecture and major concepts

## Process topology

`trellis` composes the control plane and worker agent. The control plane owns desired state, scheduling, service catalog, health-derived status, HTTP endpoints, Prometheus metrics, and reconciliation. The agent owns actual containers, ports, volumes, logs, secrets materialization, and local restart/health loops. `trellisctl` is the human CLI. Consumers such as reverse-proxy controllers are ordinary workloads outside this repository; [`trellis-proxy-sync`](https://github.com/overfold/trellis-proxy-sync), for example, turns labelled allocations into a proxy configuration.

The design is leader-driven. A Raft-backed state store persists jobs, secrets, allocation records, membership-related desired state, and a monotonically meaningful control epoch. Followers serve as cluster members, but only the elected leader reconciles. At most five members vote; the rest replicate as non-voters that the leader promotes to keep an odd voter set (see [control-plane membership](control-plane.md#control-plane-membership)). The server-to-agent protocol includes epoch, allocation generation, job revision, and execution hash to make repeat requests safe and reject stale control traffic.

## Desired and observed state

A `spec.JobSpec` is immutable input to a job revision. A task group's execution content is hashed independently of count, labels, and update policy so metadata/scale changes can be distinguished from container replacement. Server `Allocation` objects join desired identity (namespace/job/group/revision/generation) with placement and observed lifecycle/health. Agents reconstruct local allocation state from durable allocation records after restart and verify it against runtime labels.

Desired state is durable. Observations—heartbeats, runtime status, logs, much of the catalog—are renewable. Each live job retains its 10 newest versions (every accepted spec change is a version; execution changes also advance the revision); deletion removes that history with the job. Backups capture the replicated cluster settings, desired jobs, their retained version history, encrypted secrets, and placement metadata from one consistent view, and a restore installs them in one Raft entry; restoring reconstitutes desired state and lets reconciliation schedule clean allocations. Raft FSM snapshots stream the stable Bolt read transaction directly into deterministic key-ordered JSON and restore it inside one Bolt write transaction, avoiding whole-state maps while preserving the existing snapshot representation and atomic restore.

## Package map

- `internal/spec`: YAML decode, types, validation, execution hashing.
- `internal/server`: domain state, handlers, scheduler, reconciliation, metrics, secrets delivery, allocation queries.
- `internal/agent`: agent endpoints and local reconciliation, ports, volumes, restart integration.
- `internal/runtime`: the container runtime interface, containerd implementation, injected test runtime, log access.
- `internal/state`: abstract state store, Bolt implementation, Raft FSM/snapshot implementation.
- `internal/election`: single-node and Raft leadership events.
- `internal/network` and `internal/dns`: namespace network plans, WireGuard realization, service DNS.
- `internal/catalog`: healthy endpoint index.
- `internal/health` and `internal/lifecycle`: health probes and state-machine vocabulary/events.
- `internal/secrets` and `internal/auth`: envelope-style encrypted secret records and bearer token scopes.
- `api` / `client`: public operator-API wire types and Go client, used by `trellisctl` and external integrations such as [`trellis-proxy-sync`](https://github.com/overfold/trellis-proxy-sync).
- `internal/nodeapi` / `internal/client`: node-internal wire types and the agent and node clients; `internal/transport` is the HTTP transport shared with the public client.
