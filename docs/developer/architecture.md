# Architecture and major concepts

## Process topology

`trellis` composes the control plane and worker agent. Each responsibility can be enabled independently: worker-only nodes run no Raft/state/CA/secrets-key components, while control-plane-only nodes may opt out of workloads. The control plane owns desired state, scheduling, service catalog, HTTP endpoints, metrics, and reconciliation. The agent owns containers, ports, volumes, logs, secrets materialization, and local health loops.

The design is leader-planned and node-reconciled. A Raft-backed state store persists jobs, secrets, allocation records with frozen task/restart specifications, cleanup decisions/proofs, membership-related desired state, and a monotonically meaningful control epoch. Complete node execution documents are leader-local projections, not replicated records. Only the elected leader plans and publishes execution authority; nodes pull complete documents and converge locally. Epoch, allocation generation, job revision, execution hash, and durable attempt identity fence execution. At most five members vote; the rest replicate as non-voters that the leader promotes to keep an odd voter set (see [control-plane membership](control-plane.md#control-plane-membership)).

Raft leadership notifications can coalesce, including consecutive acquire notifications with no intervening loss. Every notification deactivates the leader API and cancels and joins the previous term's background loops and admitted API work before any reload or epoch acquisition. Requests and intent publication retain their originating term. New terms apply a Raft barrier before reloading durable state.

Leadership activation captures the Raft term before waiting for mutation ordering.
An exclusive submission hold and a barrier establish the authoritative FSM view
for epoch advancement; it does not use the follower's cached cluster record or
compact job history. Epoch exhaustion fails closed rather than wrapping.
Caches are reloaded after activation. Restore retains originating-term cancellation
even through detached request contexts and compares the exact target cluster
record before submission and atomically at installation, preserving target
authority and rejecting an intervening activation. Freshness conflicts remain
`409`; canceled or superseded authority and infrastructure failures remain `503`.

The local [Raft admission patch](../../third_party/raft/README.trellis.md) checks
originating terms and cancellation inside Raft's leader loop, before appending
commands or membership entries. This closes loss/reacquisition races that a
caller-side leadership check cannot fence. Bolt prefix replacement re-seeks after
each deletion, so different replica page layouts cannot leave different records.
The patch itself does not negotiate compatibility. Release admission and
activation are separate responsibilities, described below; no pre-v1
persisted-state migration or mixed-release guarantee is introduced.

An unexpected committed FSM apply failure is process-fatal on leaders and followers. Raft can discard follower response errors and advance its applied index despite failure, so continuing would permit a divergent replica to serve, vote, or snapshot. The fatal diagnostic identifies the log index and term without dumping command contents. Ordinary mutation validation and restore freshness rejection happen before replication; restore preflight uses a barrier and shares submission ordering with writes, then installation rechecks freshness atomically. A failed replica requires storage repair or replacement from healthy replicated state, not treating `AppliedIndex` as proof of successful mutation.

Each FSM command commits its changes and an internal applied-log-index checkpoint in one Bolt transaction. The checkpoint is part of full FSM snapshots, not desired-state backups. On restart, replay skips commands already covered by the persisted checkpoint; a restored snapshot rolls state and checkpoint back together. A committed desired-state restore is therefore not run again against its own populated state, and deleted jobs and node tombstones are not temporarily undone by older replayed commands. New restore requests still pass the barrier-backed freshness preflight and atomic installation check; failed commands never advance the checkpoint.

Operator restore also authenticates every encrypted secret through `internal/secrets.Store.ValidateRecord` before submitting desired state. It shares decryption with `Resolve`, authenticates both the wrapped DEK and secret ciphertext, and clears transient plaintext and DEK bytes. This is leader-local recovery validation, not FSM work: replicated snapshots contain only encrypted records, never encryption keys or plaintext. All potential leaders still need the original key bytes and explicit ID, if configured; no keyring or automatic rekeying is provided. Bolt's atomic freshness check and Raft's barrier-backed preflight return `state.ErrRestoreNotFresh`, preserved as HTTP `409`. Real barrier, storage, commit, and post-commit reload failures remain `503`, with potentially committed state requiring inspection before retry. See [operator recovery](../public/operations.md#backups).

## Desired and observed state

Execution authority is durable, not heartbeat observation; its delivery representation is derived. Each node document contains complete/version/epoch fencing, RUN/RETAIN/CLEANUP entries, sequenced drain state, and namespace network plans. Start/stop transitions and cleanup decisions commit atomically with the reconciliation plan. Projection reads the committed view under mutation ordering and caches documents without Raft writes; a new leader rebuilds them with its new durable epoch. Versions order content changes within an epoch, so agents compare `(epoch, version)`. During recovery grace `CleanupAbsent` is false but explicit cleanup remains effective. Exact generation cleanup receipts, not absent inventory or exited tasks, release reservations; cleanup tombstones survive allocation pruning. Nodes persist accepted documents and start admissions. Disconnection prevents new admission after pull failure but does not stop established work or its local restart policy. Secrets and workload tokens use a separate attempt-authorized credential endpoint and never enter cached intent. See [internal execution protocol](api.md#durable-node-execution-intent).

A `spec.JobSpec` is immutable input to a job revision. The server resolves authored image references at plan/apply and stores `ResolvedImages` with each job and historical version. Execution hashing and allocation construction use those pins, while the canonical spec retains authored tags. Registry resolution happens outside mutation locks and Raft application; planned pins are validated and reused at apply, and reconciliation never resolves tags. A task group's execution content is hashed independently of count, labels, and update policy so metadata/scale changes can be distinguished from container replacement. Server `Allocation` objects join desired identity (namespace/job/group/revision/generation) with placement and observed lifecycle/health. Agents reconstruct local allocation state from durable allocation records after restart and verify it against runtime labels.

Plans return the fully defaulted spec, pinned images, current job identity/version, and a fingerprint of replicated cluster settings. Apply checks the settings fingerprint before defaulting and again under the mutation lock, including before a no-op return. Planning snapshots settings before defaulting and rejects a change during registry resolution. The CLI submits the returned spec, not the authored input. Direct submissions without a settings precondition remain available.

Task-group content hashes are hexadecimal SHA-256 strings. Nil and empty constraints are equivalent, matching semantic diff's omitted-array representation. `StateController.ListJobs` always rebuilds these derived hashes from the validated canonical spec and stored image pins on restart, leadership reload, and after restore; it never trusts persisted digests. Rebuilding requires no registry I/O, Raft writes, version/revision changes, or workload replacement. Later job writes persist the rebuilt representation. Canonical specs and execution identity remain authoritative; this is derived-cache rebuilding, not a desired-state migration. Agent start-request execution hashes and allocation fencing are separate and unchanged.

Desired state is durable. Observations—heartbeats, runtime status, logs, much of the catalog—are renewable. Each live job retains its 10 newest versions (every accepted spec change is a version; execution changes also advance the revision); deletion removes that history with the job. Backups capture the replicated cluster settings, desired jobs, their retained version history, encrypted secrets, and placement metadata from one consistent view, and a restore installs them in one Raft entry; restoring reconstitutes desired state and lets reconciliation schedule clean allocations. Raft FSM snapshots stream a stable Bolt read transaction directly into deterministic key-ordered JSON and restore inside one Bolt write transaction, avoiding whole-state maps and partial installation.

### Application snapshot compatibility

`internal/state` owns the full FSM snapshot format, independently of Hashicorp
Raft's `SnapshotMeta.Version` (which versions Raft metadata/transport) and the
operator desired-state backup format. Current writers emit the JSON tuple
`[2,{"key":"base64-value"}]`: exactly an integer version followed by a key/value
object. Readers require this v2 tuple and reject bare key/value objects.
Empty objects and empty/binary values are valid; record bytes remain opaque.

Restore rejects unsupported versions before starting a write transaction. The
payload requires nonempty, storage-sized, unique keys and base64 string values;
nulls, malformed JSON, extra tuple elements and trailing data are rejected.
Payload validation and bucket replacement share one Bolt transaction: even a
late decode or storage failure rolls back every write and preserves existing
state. Key order in received snapshots is immaterial; writers use Bolt key order.
Values remain opaque at this layer, not revalidated as desired-state backups or
interpreted using server record types. Domain validation on reload still applies.

Only the current format is supported; there is no migration or mixed-release
guarantee for experimental releases. The FSM additionally validates the embedded
active compatibility level inside the snapshot replacement transaction.
Snapshot format versioning is separate from release admission and activation.

### Release compatibility and activation

`internal/compatibility` owns the explicit release sequence: experimental builds
use 0; v1 will establish level 1, and each subsequent supported stable release
increments it once. No pre-v1 version tag is parsed or recognized. A stable
binary implements its own and its predecessor's level, except v1, which does not
implement level 0. Sequence declarations are not proof of semantic compatibility:
future changes must retain predecessor behavior and cross-binary tests.

`internal/state` owns the replicated `\x00compatibility-release` record. Each
command carries envelope format 1 and the active release level, and commits that
boundary atomically with mutations and the applied-index checkpoint. A dedicated
activation command advances exactly one stable level. Submission ordering keeps
older-level writes from crossing activation. Startup rejects unmarked or
unsupported state before constructing Raft; snapshot installation validates the
boundary atomically. Transport uses an atomic mirror updated after commit, so
its frame checks do not depend on a storage read succeeding.

TLS Raft streams exchange a bounded compatibility preface before RPCs and
recheck compatibility at each inbound frame. A successor's admission first
checkpoints current state. Snapshots retain no trailing logs, so fresh replicas
recover current semantics rather than replaying unsupported historical commands.
On restart, a durable FSM checkpoint covering the latest snapshot avoids rolling
the database back to older semantics merely to replay already-applied commands;
Raft still restores membership/index metadata. Empty replicas restore normally.

`internal/server` owns live node probes and administrator activation, serialized
with membership changes and durable registration. Unknown, unreachable, or old
workers and Raft members block activation. The leader projects execution intent
at the replicated active level, not its binary level, and agents validate that
level before accepting cached or fetched authority. New semantics and persisted
formats must be gated at their owning package using this active boundary, not
the build tag. See [operator upgrade policy](../public/operations.md#upgrade-a-node)
and [private wire contracts](api.md).

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
- `internal/secrets` and `internal/auth`: envelope-style encrypted secret records and cluster-wide bearer token access levels.
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
- `execution.go`: durable node execution projection, versioned pulls, cleanup
  decisions/proofs, and separately authorized credential delivery.
- `auth.go` / `node_trust.go` / `secrets.go`: operator authentication, node identity
  and trust, and secret operations.

These files remain one package, not independently synchronized services. Job,
node, and allocation mutations and reconciliation commits share the coordinator's
mutation ordering and leadership fencing. `StateController` in `state.go` owns
typed persistence; the existing liveness, observation-queue, and exec-relay
components own their local synchronization.

## Agent source organization

Within `internal/agent`, `agent.go` owns the shared coordinator, construction,
configuration, initialization, and per-allocation operation locking. Related
workflows are grouped by responsibility:

- `state.go`: local allocation and retained-log records, epoch acceptance,
  persistence, queries, and health/restart observation callbacks.
- `recovery.go`: durable-state recovery, runtime ownership verification,
  re-observation, and interrupted-cleanup retries.
- `execution.go` / `start_admission.go`: intent validation, caching, pull and
  local convergence / durable admission and exact-generation cleanup receipts.
- `start.go` / `task_start.go`: background group-start admission and fencing /
  task preparation, resource acquisition, launch, commit, and rollback.
- `stop.go` / `drain.go`: generation-fenced stops and task cleanup / sequenced
  drain and resume intent, including starts still in progress.
- `heartbeat.go`: node registration and complete allocation-status reporting.
- `network_attachments.go`: network-plan application and attachment cleanup.
- `task_logs.go` / `exec.go`: log access, retention, usage, and limit enforcement /
  exec streams.

These files remain one package and one `Agent`, not independently synchronized
services. Starts, stops, drains, and recovery retain shared allocation state,
epoch and generation fencing, and the existing per-allocation operation locks.
The reconciler, health manager, ports, volumes, and secrets keep their existing
responsibilities and synchronization.
