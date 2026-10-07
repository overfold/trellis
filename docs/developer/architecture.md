# Architecture and major concepts

## Process topology

`trellis` composes the control plane and worker agent. Each responsibility can be enabled independently: worker-only nodes run no Raft/state/CA/secrets-key components, while control-plane-only nodes may opt out of workloads. The control plane owns desired state, scheduling, service catalog, HTTP endpoints, metrics, and reconciliation. The agent owns containers, ports, volumes, logs, secrets materialization, and local health loops.

The design is leader-driven. A Raft-backed state store persists jobs, secrets, allocation records, membership-related desired state, and a monotonically meaningful control epoch. Followers serve as cluster members, but only the elected leader reconciles. At most five members vote; the rest replicate as non-voters that the leader promotes to keep an odd voter set (see [control-plane membership](control-plane.md#control-plane-membership)). The server-to-agent protocol includes epoch, allocation generation, job revision, and execution hash to make repeat requests safe and reject stale control traffic.

Raft leadership notifications can coalesce, including consecutive acquire notifications with no intervening loss. Every notification deactivates the leader API and cancels and joins the previous term's background loops before any reload or epoch acquisition. New terms apply a Raft barrier before reloading durable state.

An unexpected committed FSM apply failure is process-fatal on leaders and followers. Raft can discard follower response errors and advance its applied index despite failure, so continuing would permit a divergent replica to serve, vote, or snapshot. The fatal diagnostic identifies the log index and term without dumping command contents. Ordinary mutation validation and restore freshness rejection happen before replication; restore preflight uses a barrier and shares submission ordering with writes, then installation rechecks freshness atomically. A failed replica requires storage repair or replacement from healthy replicated state, not treating `AppliedIndex` as proof of successful mutation.

## Desired and observed state

A `spec.JobSpec` is immutable input to a job revision. The server resolves authored image references at plan/apply and stores `ResolvedImages` with each job and historical version. Execution hashing and allocation construction use those pins, while the canonical spec retains authored tags. Registry resolution happens outside mutation locks and Raft application; planned pins are validated and reused at apply, and reconciliation never resolves tags. A task group's execution content is hashed independently of count, labels, and update policy so metadata/scale changes can be distinguished from container replacement. Server `Allocation` objects join desired identity (namespace/job/group/revision/generation) with placement and observed lifecycle/health. Agents reconstruct local allocation state from durable allocation records after restart and verify it against runtime labels.

Desired state is durable. Observations—heartbeats, runtime status, logs, much of the catalog—are renewable. Each live job retains its 10 newest versions (every accepted spec change is a version; execution changes also advance the revision); deletion removes that history with the job. Backups capture the replicated cluster settings, desired jobs, their retained version history, encrypted secrets, and placement metadata from one consistent view, and a restore installs them in one Raft entry; restoring reconstitutes desired state and lets reconciliation schedule clean allocations. Raft FSM snapshots stream the stable Bolt read transaction directly into deterministic key-ordered JSON and restore it inside one Bolt write transaction, avoiding whole-state maps while preserving the existing snapshot representation and atomic restore.

## Package map

- `internal/spec`: YAML decode, canonical types, defaulting, validation, and execution hashing.
- `internal/plan`: semantic job-change planning over canonical specifications and resolved images.
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

## Control-plane source organization

Within `internal/server`, `server.go` owns the shared coordinator, its locking
contract, construction, initialization, and leadership lifecycle. Related
operations are grouped by responsibility:

- `types.go`: cluster, node, job, and allocation records, including allocation
  lifecycle methods and persistence encoding.
- `jobs.go` / `nodes.go`: job admission, planning, queries, and mutations / node
  registration, heartbeats, queries, and drain operations.
- `backup.go` / `catalog.go`: desired-state backup and restore / service-discovery
  queries and catalog projection.
- `allocations.go` / `task_logs.go` / `exec.go`: allocation queries and operations,
  log access, and exec streams.
- `auth.go` / `node_trust.go` / `secrets.go`: operator authentication, node identity
  and trust, and secret operations.

These files remain one package, not independently synchronized services. Job,
node, and allocation mutations and reconciliation commits share the coordinator's
mutation ordering and leadership fencing. `StateController` in `state.go` owns
typed persistence; the existing liveness, observation-queue, and exec-relay
components own their local synchronization.
