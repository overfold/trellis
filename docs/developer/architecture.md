# Architecture and major concepts

## Process topology

`trellis` composes the control plane and worker agent. Each responsibility can be enabled independently: worker-only nodes run no Raft/state/CA/secrets-key components, while control-plane-only nodes may opt out of workloads. The control plane owns desired state, scheduling, service catalog, HTTP endpoints, metrics, and reconciliation. The agent owns containers, ports, volumes, logs, secrets materialization, and local health loops.

The design is leader-driven. A Raft-backed state store persists jobs, secrets, allocation records, membership-related desired state, and a monotonically meaningful control epoch. Followers serve as cluster members, but only the elected leader reconciles. At most five members vote; the rest replicate as non-voters that the leader promotes to keep an odd voter set (see [control-plane membership](control-plane.md#control-plane-membership)). The server-to-agent protocol includes epoch, allocation generation, job revision, and execution hash to make repeat requests safe and reject stale control traffic.

Raft leadership notifications can coalesce, including consecutive acquire notifications with no intervening loss. Every notification deactivates the leader API and cancels and joins the previous term's background loops, dispatched actions and admitted API work before any reload or epoch acquisition. Requests and actions retain their originating epoch; outcome callbacks cannot discard term cancellation or rewrite detached allocation objects. New terms apply a Raft barrier before reloading durable state.

An unexpected committed FSM apply failure is process-fatal on leaders and followers. Raft can discard follower response errors and advance its applied index despite failure, so continuing would permit a divergent replica to serve, vote, or snapshot. The fatal diagnostic identifies the log index and term without dumping command contents. Ordinary mutation validation and restore freshness rejection happen before replication; restore preflight uses a barrier and shares submission ordering with writes, then installation rechecks freshness atomically. A failed replica requires storage repair or replacement from healthy replicated state, not treating `AppliedIndex` as proof of successful mutation.

Each FSM command commits its changes and an internal applied-log-index checkpoint in one Bolt transaction. The checkpoint is part of full FSM snapshots, not desired-state backups. On restart, replay skips commands already covered by the persisted checkpoint; a restored snapshot rolls state and checkpoint back together. A committed desired-state restore is therefore not run again against its own populated state, and deleted jobs and node tombstones are not temporarily undone by older replayed commands. New restore requests still pass the barrier-backed freshness preflight and atomic installation check; failed commands never advance the checkpoint.

Operator restore also authenticates every encrypted secret through `internal/secrets.Store.ValidateRecord` before submitting desired state. It shares decryption with `Resolve`, authenticates both the wrapped DEK and secret ciphertext, and clears transient plaintext and DEK bytes. This is leader-local recovery validation, not FSM work: replicated snapshots contain only encrypted records, never encryption keys or plaintext. All potential leaders still need the original key bytes and explicit ID, if configured; no keyring or automatic rekeying is provided. Bolt's atomic freshness check and Raft's barrier-backed preflight return `state.ErrRestoreNotFresh`, preserved as HTTP `409`. Real barrier, storage, commit, and post-commit reload failures remain `503`, with potentially committed state requiring inspection before retry. See [operator recovery](../public/operations.md#backups).

## Desired and observed state

A `spec.JobSpec` is immutable input to a job revision. The server resolves authored image references at plan/apply and stores `ResolvedImages` with each job and historical version. Execution hashing and allocation construction use those pins, while the canonical spec retains authored tags. Registry resolution happens outside mutation locks and Raft application; planned pins are validated and reused at apply, and reconciliation never resolves tags. A task group's execution content is hashed independently of count, labels, and update policy so metadata/scale changes can be distinguished from container replacement. Server `Allocation` objects join desired identity (namespace/job/group/revision/generation) with placement and observed lifecycle/health. Agents reconstruct local allocation state from durable allocation records after restart and verify it against runtime labels.

Desired state is durable. Observations—heartbeats, runtime status, logs, much of the catalog—are renewable. Each live job retains its 10 newest versions (every accepted spec change is a version; execution changes also advance the revision); deletion removes that history with the job. Backups capture the replicated cluster settings, desired jobs, their retained version history, encrypted secrets, and placement metadata from one consistent view, and a restore installs them in one Raft entry; restoring reconstitutes desired state and lets reconciliation schedule clean allocations. Raft FSM snapshots stream a stable Bolt read transaction directly into deterministic key-ordered JSON and restore inside one Bolt write transaction, avoiding whole-state maps and partial installation.

### Application snapshot compatibility

`internal/state` owns the full FSM snapshot format, independently of Hashicorp
Raft's `SnapshotMeta.Version` (which versions Raft metadata/transport) and the
operator desired-state backup format. Current writers emit the JSON tuple
`[2,{"key":"base64-value"}]`: exactly an integer version followed by a key/value
object. Readers support v2 and explicitly recognize the pre-versioned bare
object as legacy v1, including snapshots from releases using that representation.
The tuple avoids reserving a key that could collide with legacy state. Empty
objects and empty/binary values are valid; record bytes are not migrated.

Restore rejects unsupported versions before starting a write transaction. Both
formats require nonempty, storage-sized, unique keys and base64 string values;
nulls, malformed JSON, extra tuple elements and trailing data are rejected.
Payload validation and bucket replacement share one Bolt transaction: even a
late decode or storage failure rolls back every write and preserves existing
state. Key order in received snapshots is immaterial; writers use Bolt key order.
Values remain opaque at this layer, not revalidated as desired-state backups or
interpreted using server record types. Domain validation on reload still applies.

This supports forward reading of legacy v1, **not old-binary reading of v2** or
arbitrary mixed-release operation. The introduction of v2 requires the
[coordinated control-plane upgrade](../public/operations.md#raft-snapshot-format-upgrades).
Future incompatible payload or persisted-record changes must explicitly revise
this version and document supported readers and upgrade boundaries; do not add
speculative migrations or reinterpret unknown versions as legacy. Snapshot
versioning alone does not negotiate log-command or private API compatibility.

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
