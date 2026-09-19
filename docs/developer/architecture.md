# Architecture and major concepts

## Process topology

`trellis` composes the control plane and worker agent. The control plane owns desired state, scheduling, service catalog, health-derived status, HTTP endpoints, Prometheus metrics, and reconciliation. The agent owns actual containers, ports, volumes, logs, secrets materialization, and local restart/health loops. `trellisctl` is the human CLI; `trellis-proxy-sync` turns catalog allocation labels into a proxy configuration.

The design is leader-driven. A Raft-backed state store persists jobs, secrets, allocation records, membership-related desired state, and a monotonically meaningful control epoch. Followers serve as cluster members, but only the elected leader reconciles. The server-to-agent protocol includes epoch, allocation generation, job revision, and execution hash to make repeat requests safe and reject stale control traffic.

## Desired and observed state

A `spec.JobSpec` is immutable input to a job revision. A task group's execution content is hashed independently of count, labels, and update policy so metadata/scale changes can be distinguished from container replacement. Server `Allocation` objects join desired identity (namespace/job/group/revision/generation) with placement and observed lifecycle/health. Agents reconstruct local allocation state from runtime labels after restart.

Desired state is durable. Observations—heartbeats, runtime status, logs, much of the catalog—are renewable. Backups capture desired jobs, their revision history, encrypted secrets, and placement metadata; restoring reconstitutes desired state and lets reconciliation schedule clean allocations.

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
- `internal/api` / `internal/client`: shared wire types and HTTP clients.
- `ui`: Next.js server proxy and React administration interface.
