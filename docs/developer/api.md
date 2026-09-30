# HTTP API

The control-plane API defaults to port 8128. Ordinary operator and workload callers send `Authorization: Bearer TOKEN`. Administrator requests use the signing protocol below. Use TLS outside a local sandbox.

Every namespaced resource names its namespace in the path: `/v1/namespaces/{namespace}/jobs`, `/allocations`, `/events`, and `/secrets`. There is no namespace header and no implicit default namespace. Listing across namespaces is a separate, explicit cluster-scoped request (`GET /v1/allocations`, `GET /v1/events`).

JSON request bodies must be sent with `Content-Type: application/json` (otherwise `415`) and are decoded strictly, with the same rules as YAML manifests and the published schemas: a body must contain exactly one JSON value, and unknown fields, trailing data, and an empty body are rejected with `400` and a `message` naming the problem (for example `invalid request body: json: unknown field "imgae"`). Bodies are size-limited per route; an oversized body returns `413`. Job submissions and plans are limited to 4 MiB, heartbeats to 32 MiB, backup restores to 64 MiB, secret writes to 96 KiB, exec input to 128 KiB, and other requests to 64 KiB or 1 MiB.

Trellis distinguishes three credential kinds:

- `administrator` — the root request context granted after verification of an operator-held Ed25519 key;
- `operator` — an explicitly minted API credential with `namespace` or `cluster` scope and `read` or `write` access;
- `workload` — a scoped credential injected through task-group `api_access`.

Credential prefixes (`trls_op_`, `trls_wl_`) are descriptive only. The server authenticates the complete bearer value and uses its authoritative stored principal metadata for generated credentials. The separate managed-enrollment credential conventionally uses `trls_enroll_`.

A task group requests workload access with an object such as `{"scope":"namespace","access":"read"}`. Namespace scope is restricted to the namespace containing the job. Cluster scope grants only the ordinary read/write API authority represented by the credential; it never turns into the administrator credential. Both scopes set `TRELLIS_NAMESPACE` to the job namespace, which clients use to build `/v1/namespaces/{namespace}/...` request paths.

Each workload credential belongs to one allocation generation and carries a subject naming its namespace, job, and task group, which `GET /v1/auth/whoami` reports as `subject`. The leader mints it on the generation's first start and, like every generated credential, authenticates it by hash. Replicated state keeps only that hash and a copy sealed with the secrets encryption key, so start retries and leadership changes re-deliver the same token and the allocation execution hash stays stable. A server without a secrets key cannot start API-enabled allocations. Starting a new generation of the allocation, or with a different grant, replaces its credential and revokes the previous one. The leader's reconciliation revokes a workload credential once its allocation record is pruned, its job or task group is deleted, the job is recreated under the same name, or the task group's current `api_access` is removed or narrowed below the credential's scope or access. Widening `api_access` does not revoke existing credentials. A start for a generation older than the credential's recorded generation is rejected rather than revoking the newer credential, and a storage error while recovering a credential fails the start instead of rotating the token.

The API uses the same resource vocabulary as the [Trellis user model](../public/user-model.md), but JSON is the transport representation. Humans author jobs as YAML manifests; job submission carries the equivalent JSON `JobSpec` inside the API request. Human-readable YAML memory sizes are normalized to byte counts in JSON.

## Public/operator endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics. |
| `POST` | `/v1/auth/administrator/challenge` | Issue a short-lived one-time administrator signing challenge. |
| `GET` | `/v1/auth/whoami` | Return the current credential kind, scope, access, namespace, and available provenance metadata. |
| `GET` | `/v1/nodes` | List node capacity, discovered capabilities, status, and control-plane membership; requires cluster scope. |
| `POST` / `DELETE` | `/v1/nodes/{id}/drain` | Drain or undrain; requires `cluster/write`. |
| `GET` | `/v1/namespaces` | Discover namespace names visible to the caller. |
| `GET` | `/v1/allocations?label=key:value` | List/filter allocations across all namespaces; requires cluster scope. |
| `GET` | `/v1/events` | Server-sent event stream across all namespaces; requires cluster scope. |
| `GET`, `POST` | `/v1/namespaces/{ns}/jobs` | List jobs or submit `{"spec": JobSpec, "expected_version": N}`; submitting requires write access. |
| `POST` | `/v1/namespaces/{ns}/jobs/plan` | Validate and calculate the authoritative semantic plan for a `JobSpec`. |
| `GET`, `DELETE` | `/v1/namespaces/{ns}/jobs/{name}` | Read job state/API representation or delete the job; deleting requires write access. |
| `POST` | `/v1/namespaces/{ns}/jobs/{name}/restart` | Restart the job's allocations; requires write access. |
| `GET` | `/v1/namespaces/{ns}/jobs/{name}/versions` | List the job's retained version history. |
| `POST` | `/v1/namespaces/{ns}/jobs/{name}/groups/{group}/replacement-backoff/reset` | Clear a task group's replacement backoff; requires write access. |
| `GET` | `/v1/namespaces/{ns}/allocations?label=key:value` | List/filter the namespace's allocations. |
| `DELETE` | `/v1/namespaces/{ns}/allocations/{id}` | Stop one allocation; requires write access. |
| `GET` | `/v1/namespaces/{ns}/allocations/{id}/events` | Lifecycle event array. |
| `GET` | `/v1/namespaces/{ns}/allocations/{id}/logs?task=NAME&tail=100&follow=true` | Plain-text logs for one task in an allocation. |
| `GET` | `/v1/namespaces/{ns}/allocations/{id}/exec?command=...` | Upgrade to a bidirectional exec stream (`Upgrade: trellis-exec.v1`) that runs one command; requires write access. See [Exec streams](#exec-streams). |
| `GET` | `/v1/namespaces/{ns}/allocations/{id}/metrics` | Current per-task CPU and memory usage. |
| `GET` | `/v1/namespaces/{ns}/events` | Server-sent event stream for one namespace. |
| `PUT` | `/v1/namespaces/{ns}/secrets/{name}` | Set a secret; requires write access. |
| `GET` | `/v1/namespaces/{ns}/secrets[/{name}]` | List/get secret metadata only. |
| `DELETE` | `/v1/namespaces/{ns}/secrets/{name}` | Delete a secret; requires write access. |

Authorization of `/v1/namespaces/{ns}/...` routes is by path: a namespace-scoped credential may address only its own namespace and receives `403` for any other, while cluster-scoped credentials and the administrator may address every namespace. Access (`read` or `write`) is checked separately, as noted per route. A `{ns}` that is not a valid identifier returns `400`.

`GET /v1/auth/whoami` is the capability/introspection primitive clients should use instead of probing protected endpoints. Typical generated-token response:

```json
{
  "kind": "operator",
  "scope": "namespace",
  "access": "write",
  "namespace": "payments",
  "created_at": "2026-09-02T20:00:00Z"
}
```

A workload credential additionally reports `"subject": {"namespace": "payments", "job": "router", "task_group": "sync"}`.

An administrator credential reports `kind: "administrator"`, `scope: "cluster"`, and `access: "write"`, but callers must still treat `administrator` as more privileged than ordinary `cluster/write`: root-only endpoint checks use the credential kind/context, not merely those two effective fields.

`GET /v1/namespaces` is discovery, not namespace lifecycle management. For cluster-scoped or administrator callers it returns the sorted union of namespace names that currently have a desired job, a stored secret, or a namespace-scoped credential. A namespace-scoped caller receives only its own namespace. There is no namespace-creation call: minting a credential for, storing a secret in, or applying a job to a previously unseen namespace makes it discoverable.

Node resource values use millicores for CPU and bytes for memory. In `GET /v1/nodes`, `cpu_capacity` and `memory_capacity` are whole-host capacity, while `cpu_allocatable` and `memory_allocatable` are the capacity available to the scheduler after the node's host reserve. The existing `cpu` and `memory` fields carry the same allocatable values. `last_heartbeat` is when the current leader last received a heartbeat from the node; heartbeat times are not replicated, so it is omitted until the node reports to a newly elected leader. `cpu_usage` is a whole-host ratio from 0 to 1; `memory_used`, `memory_available`, and `metrics_at` describe the same point-in-time host sample and are omitted until the agent can collect one. Scheduling uses allocatable capacity, never live utilization. `control_plane` is `voter` or `nonvoter` for a Raft member and omitted for a registered node that is no longer one.

`GET /v1/namespaces/{ns}/jobs` and `GET /v1/namespaces/{ns}/jobs/{name}` include `replacement_backoff` for task groups whose failed allocations are being replaced with a delay. Each entry has `group`, `job_revision`, `failures` (consecutive failed allocations counted since the last reset), `last_failure_at`, `last_allocation_id`, the failed allocation's `reason` and `message`, and `next_replacement_at`, the earliest time a new allocation may be placed for the group. The field is omitted when no group has counted failures. The backoff delays only replacements of failed allocations; allocations lost with their node and a higher `count` are placed immediately. `POST /v1/namespaces/{ns}/jobs/{name}/groups/{group}/replacement-backoff/reset` clears a group's backoff so its failed allocations are replaced at once; it returns `204` (also when the group has no counted failures), `404` when the job or task group does not exist in the namespace, and `403` without write access. The event streams emits `job.replacement_delayed` with `job`, `group`, `allocation_id`, `revision`, `failures`, and `next_replacement_at` whenever a failure is counted, and `job.replacement_backoff_reset` with `job`, `group`, and `revision` when a backoff is reset. `/metrics` exposes the same state as `trellis_replacement_backoff_failures` and `trellis_replacement_backoff_remaining_seconds`, labelled by `namespace`, `job`, and `group`. Only the five newest terminal allocations per task group are retained, so older `stopped`, `failed`, and `lost` allocations disappear from job status and allocation queries. If a node later reports a container for a pruned allocation, reconciliation treats it as an observed orphan and stops it.

When desired capacity cannot be placed, job status includes one `pending` allocation per unmet replica. Its existing allocation `reason` and `message` fields carry the current placement diagnostic: `no_healthy_nodes`, `constraint_mismatch`, `volume_owner_unavailable`, `missing_capability`, `host_port_conflict`, or `insufficient_capacity`. Reconciliation reuses these records rather than creating another pending allocation on every pass. It updates a record only when the blocking reason changes and places the same allocation, clearing the diagnostic, when a node becomes eligible. These are observations of scheduler filters, not new desired-state resources or scheduling policy.

A job has two counters, both reported by `GET /v1/namespaces/{ns}/jobs` and `GET /v1/namespaces/{ns}/jobs/{name}`. `version` advances on every accepted change to the specification, including label, `count`, and update-policy changes. `revision` identifies execution content: it advances only when a task group's execution hash changes, and allocations carry it as `job_revision` for fencing and rolling updates. Submitting a specification identical to the current one changes neither and writes nothing.

The job submitted to `POST /v1/namespaces/{ns}/jobs` or `.../jobs/plan` must name the path namespace in `spec.namespace`; a mismatch returns `400`. The plan returns `base_version` and `base_revision` for an existing job. Submission accepts an optional `expected_version`: `0` requires that the job does not exist, `N` requires that it is currently at version `N`, and omitting it applies unconditionally. The leader checks the precondition and commits the change under the same serialized job-mutation lock, so of several concurrent applies against the same version exactly one succeeds; the others receive `409 Conflict` with a `message` naming the expected and current versions. A successful submit returns `202` with `{"namespace", "name", "version", "revision"}` describing the committed job, so clients need not re-read it. `trellisctl jobs apply` always sends the `base_version` of the plan they showed (or `0` for a create). Versions restart at 1 when a job is deleted and recreated, so the precondition cannot distinguish a recreated job that has reached the planned version from the job that was planned against. `job.registered` events on the event streams fire for every apply that changes a job and carry its new `version` and `revision`.

`GET /v1/namespaces/{ns}/jobs/{name}/versions` returns the retained history in ascending version order; each entry has `version`, the `revision` that version ran, the full canonical `spec`, and `created_at`. Trellis retains at most the 10 newest versions for each live job, and every apply that changes the specification compacts that job's history. A newly elected leader also compacts all histories and removes records orphaned by older job deletions. Deleting a job atomically deletes all of its history, so recreating the same name starts again at version 1 and revision 1. Backups use format version 5, which records the producing `trellis_version`, carries the replicated `cluster_settings`, and holds canonical job and history records. `POST /v1/backup/restore` accepts only the current format version; a backup in any other format is refused with an error that names its format and the Trellis release that created it, so it can be restored with a release that uses the same format. Formats are never migrated.

For allocation logs, `task` selects the task name from the allocation's task group. It may be omitted when the allocation has exactly one task; a multi-task allocation returns `400` until the caller selects one. The allocation ID is the Trellis allocation identity, not an agent/container runtime ID.

### Exec streams

`GET /v1/namespaces/{ns}/allocations/{id}/exec` runs one command in an allocation task over a single long-lived, bidirectional stream. The request is an HTTP/1.1 upgrade: it carries `Connection: Upgrade` and `Upgrade: trellis-exec.v1` along with the usual credentials, and the server answers `101 Switching Protocols` once the command is admitted. HTTP/2 cannot upgrade connections, so clients must use HTTP/1.1 for this request. Followers proxy the upgrade to the leader like any other request.

The query string describes the process:

| Parameter | Meaning |
|---|---|
| `command` | Required and repeated: the argv, in order, such as `command=/bin/sh&command=-c&command=ls`. Trellis never adds a shell. |
| `task` | The task to run in. It may be omitted when the allocation has exactly one task. |
| `stdin=true` | Attach the stream's input frames to the process. Without it the process has no standard input. |
| `tty=true` | Allocate a terminal. Terminal output arrives as stdout frames; there is no separate stderr. |
| `term` | `TERM` for a TTY process: at most 64 characters of letters, digits, `.`, `_`, `+`, and `-`. |
| `cols`, `rows` | Initial TTY size, 1–1000 each; default 80×24. |

`term`, `cols`, and `rows` require `tty=true`. The command runs with the selected task container's OCI process context: environment variables, user, working directory, and security confinement are inherited from the task. `TERM` is the only override. Exec addresses only the current generation's running tasks, as do allocation metrics.

After the upgrade, both directions carry frames: a one-byte type, a four-byte big-endian payload length, and a payload of at most 32 KiB. Data frames carry raw bytes; control frames carry JSON.

| Type | Name | Direction | Payload |
|---|---|---|---|
| 1 | stdin | client → server | Input bytes. Requires `stdin=true`. |
| 2 | stdin-close | client → server | Empty. Ends the process's input, as end of file. |
| 3 | resize | client → server | `{"cols":120,"rows":32}`, 1–1000 each. Requires `tty=true`. |
| 4 | stdout | server → client | Output bytes; with a TTY, all terminal output. |
| 5 | stderr | server → client | Standard error bytes of a non-TTY process. |
| 6 | exit | server → client | `{"exit_code":0}`. The process exited; the server then closes the connection. |
| 7 | error | server → client | `{"message":"..."}`. The stream ended without an exit status; the server then closes the connection. |

Every stream ends with exactly one exit or error frame, unless the client disconnects first. Closing the connection kills the process, so a client that is interrupted never leaves a command running. A frame of an unknown or wrong-direction type, a malformed resize, or input without `stdin=true` ends the stream with an error frame. An error frame is sent when the process could not be started (for example, the executable does not exist), when its task stops, when the stream is idle or reaches its lifetime, when the node agent shuts down, when the node agent connection is lost, and when control-plane leadership changes.

The leader authorizes the request exactly as other namespaced writes, then relays the stream to the node agent that owns the allocation over the node-certificate mTLS agent API; clients never connect to agents. The relay carries the leader's control epoch. The agent rejects a stream from an older epoch and ends open streams as soon as a newer leader has fenced it, and a leader ends every stream it relays with an error frame when its term ends, so a failover never leaves a stream open to a deposed leader. Exec streams are node-local, ephemeral diagnostics state: they are not persisted in Raft, and processes left behind by an agent crash keep running in their containers until they exit or the container stops.

Buffering is bounded end to end. The leader holds one frame per direction of each stream, and the agent holds at most four input frames ahead of the process. Output is not buffered: a client that stops reading stalls the process's output instead of growing memory, and a single frame that the peer does not accept within 30 minutes ends the stream. A stream with no input, output, or resize for 30 minutes is closed, and a stream lasts at most eight hours even while active. Each node agent admits at most 64 streams in total and 8 for one allocation; a slot is held until the process has exited, including while a failed kill is being retried. Each leader relays at most 256 streams.

Exec and allocation-metrics requests that fail before the upgrade return a JSON `{"message":"..."}` body with these statuses:

| Status | Meaning |
|---|---|
| `400` | The request is invalid, is not an upgrade to `trellis-exec.v1`, or must name one task: the allocation has several tasks, or the named task is not in its task group. |
| `403` | The credential is scoped to another namespace, or lacks write access (exec only). |
| `404` | The allocation is not placed in the path namespace. |
| `409` | The allocation exists but its node has no running target for the request, such as a task that has not started or has exited. It also covers conflicting execution records for the task on the node, and an agent already fenced by a newer leader. Retry after the allocation is running again. |
| `429` | The node or allocation exec stream limit, or the leader's relay limit, has been reached. Close a stream or retry later. |
| `502` | The node agent failed while it was handling the request. Details are logged by the control plane, not returned. |
| `503` | The node agent is unreachable or shutting down, or the control-plane leader is not active. Retry later or target a replacement allocation. |


Secret write body: `{"value_base64":"...","expected_version":1}`; omit `expected_version` for unconditional update. Decoded values may contain at most 65,536 bytes; an oversized request returns `413` before base64 decoding. Lists are JSON arrays. Non-2xx responses are errors; clients must tolerate reconciliation-driven changes between reads.

Secrets follow the same namespace rule as jobs: a namespace-scoped credential with `write` access may set and delete secrets in its own namespace, and any credential that can address the namespace may list and describe secret metadata. Secrets are write-only for every caller, including cluster-scoped credentials and the administrator: no endpoint returns a stored value. Values reach a task only through leader-to-agent delivery for an allocation whose job references the secret, which a namespace writer can already arrange by applying a job, so a read-back API would add plaintext exposure without adding capability.

The control plane admits 256 simultaneous `/v1/events` subscribers per
process. Additional requests receive `503 Service Unavailable` and
`Retry-After: 1` without allocating a stream buffer. Subscriber admission does
not alter authorization: `GET /v1/namespaces/{ns}/events` receives only that
namespace's events, and only `GET /v1/events` spans namespaces. Clients should reconnect with
backoff after overload or a leader change.

## Cluster settings

Job limits, reconciliation settings, and namespace-network settings are cluster-wide semantics, so they live in the replicated cluster record rather than in each node's configuration; every leader applies the same values. The node that creates the cluster supplies the initial job limits and network settings from its configuration; reconciliation settings start at their defaults.

`GET /v1/cluster/settings` requires cluster-scoped read access (or the administrator credential) and returns:

```json
{
  "job_limits": {
    "max_replicas_per_task_group": 500,
    "max_task_groups_per_job": 64,
    "max_tasks_per_task_group": 32,
    "max_desired_allocations": 1000,
    "max_desired_allocations_per_namespace": 10000,
    "default_task_cpu": 100,
    "default_task_memory": 134217728,
    "max_task_cpu": 1000000,
    "max_task_memory": 1099511627776
  },
  "reconciliation": {
    "allocation_loss_timeout": 45000000000,
    "replacement_backoff_base": 10000000000,
    "replacement_backoff_max": 300000000000,
    "replacement_stable_after": 600000000000,
    "terminal_allocation_retention": 5
  },
  "network": {"wireguard_pool": "10.64.0.0/10", "wireguard_port_count": 256}
}
```

Memory values are byte counts. `PUT /v1/cluster/settings/job-limits` requires an administrator-signed request and replaces the complete `job_limits` object (unknown fields are rejected with `400`). It returns the updated settings, `422` when the limits are invalid (every value positive, defaults no larger than their maximums), and `409` when the new limits would stop admitting a job that is currently desired, naming the jobs; shrink or delete those jobs first. A job apply that races the change is checked against the limits current when it commits. Durations are nanoseconds. `PUT /v1/cluster/settings/reconciliation` requires an administrator-signed request and replaces the complete `reconciliation` object (unknown fields are rejected with `400`). It returns the updated settings or `422` when a value is out of bounds: `allocation_loss_timeout` between 30s and 24h, `replacement_backoff_base` between 1s and 24h, `replacement_backoff_max` between the base and 24h, `replacement_stable_after` between 10s and 24h, and `terminal_allocation_retention` between 0 and 100. Every later reconciliation pass, on any leader, applies the new values. The `network` settings are fixed when the cluster is created, because every namespace subnet and WireGuard port slot is assigned from them; the leader rejects node registrations whose WireGuard port count differs from `wireguard_port_count`.

## Administrator, enrollment, and cluster-internal endpoints

`POST /v1/credentials`, `PUT /v1/cluster/settings/job-limits`, `PUT /v1/cluster/settings/reconciliation`, `GET /v1/backup`, `POST /v1/backup/restore`, `DELETE /v1/raft/members/{id}`, and `POST /v1/raft/leadership-transfer` require an administrator-signed request. Removing a voter first promotes a healthy caught-up non-voter when one exists, and returns `409` without changing membership if the remaining voters could not form a quorum from reachable members. Removing an absent member succeeds. Leadership transfer only targets voters. The operator keeps the Ed25519 private key; replicated cluster state contains only its PKIX public key.

To sign a request, first `POST /v1/auth/administrator/challenge`. The leader returns a short-lived one-time `challenge`. Sign these newline-separated fields as UTF-8 bytes: `trellis-admin-request-v1`, challenge, uppercase HTTP method, exact path and query (`RequestURI`), and lowercase hexadecimal SHA-256 of the transmitted body. Send the challenge in `X-Trellis-Admin-Challenge` and the unpadded base64url Ed25519 signature in `X-Trellis-Admin-Signature`. The leader consumes a challenge on its first verification attempt. Challenges are leader-local and bound to the control epoch. A leader retains at most 4,096 outstanding challenges; issuing another invalidates the oldest rather than refusing issuance. Clients receiving `X-Trellis-Admin-Challenge-Status: invalid` must obtain a fresh challenge and retry. `trellisctl` does this automatically.

`POST /v1/nodes`, `POST /v1/nodes/{id}/heartbeat`, `GET /v1/internal/discovery`, `POST /v1/raft/join`, and agent port 8127 require a trusted node certificate whose URI SAN identifies the immutable node UUID. Followers redirect these control-plane requests rather than proxying them with the follower's certificate, so the leader authenticates the original node. Each UUID is durably bound to its first admitted certificate, preventing another CA-signed certificate for the same UUID from becoming that node or the current leader. Registration and heartbeat IDs must match it; Raft join derives the member ID from it, requires advertised hosts to match certificate SANs, and admits the node as a non-voter; see [Control-plane membership](control-plane.md#control-plane-membership). Ongoing outbound Raft streams verify the peer certificate against that joined Raft address rather than only the shared `trellis` DNS identity. Internal discovery returns catalog entries only for namespaces with active allocations assigned to the authenticated node; the node resolver applies the workload source-namespace check before answering. Managed-only `POST /v1/nodes/enroll` requires the separate enrollment credential over a connection authenticated with the pinned node CA. The server assigns the enrollment UUID and returns the node certificate and key without the CA key; a successful certificate-bound Raft join returns the managed CA key so the admitted member can later lead enrollment after it is promoted to voter and wins an election. Node registration reports physical and allocatable resources and includes the node WireGuard public key, externally reachable base endpoint, local port-range base, and port-range size for namespace networking, which every node runs. Heartbeats carry a complete current resource observation with discovered capabilities and the node's Raft applied index (`raft_applied_index`), and return no desired state; host usage fields may be absent when the platform counters are unavailable. A heartbeat body may contain at most 32 MiB and 320,000 allocation-task status reports; larger reports are rejected with `413`. Each reported allocation task may carry a `reason` only with phase `failed`; the agent currently reports `restart_budget_exhausted` when a task exhausted its restart budget, and the leader records that reason on the allocation when the observation moves it to `failed`. Heartbeats with any other reason, or a reason on another phase, are rejected. A task reported with phase `starting` may carry `start_failure` (`attempt`, `message` of at most 1024 bytes, and an optional `code` of `stale_generation`, `execution_conflict`, or `restart_budget_exhausted`) when the agent's background start of that generation failed; the field is rejected on any other phase. The leader counts a failure whose `attempt` equals the allocation's current attempt once, and fails the allocation with the code as its reason when a code is present. The control plane combines the namespace's durable port slot with each node's advertised bases when building WireGuard peer plans. Leader-to-agent requests verify both that the agent certificate identifies the scheduled target node and that the caller certificate identifies the current locally known Raft leader. Start, stop, drain, resume, and network-plan mutations require a positive control epoch. Allocation mutations also require a positive generation; starts additionally retain revision and execution-hash checks. The agent answers a start once it is fenced and pulls images and creates tasks in the background; the start request carries the leader's `attempt` count for the generation, which is excluded from the execution hash, and heartbeats report the outcome. A start for a generation whose restart budget the agent has exhausted is rejected with HTTP 409 and operation code `restart_budget_exhausted`; the leader records the allocation as failed rather than retrying it. Drain and resume requests carry a persisted allocation intent sequence so a delayed drain cannot override a later resume in the same epoch. The leader saves a drain or resume before delivering it. Undrain (`DELETE /v1/nodes/{id}/drain`) fails without contacting any agent if saving the resumed allocations or node fails; once saved it succeeds even when an agent is unreachable, and reconciliation redelivers the saved resume to running allocations until the agent acknowledges it. Start requests carry the allocation's current `draining` state and `drain_sequence` outside the execution hash; the agent keeps whichever of that state and its locally applied drain or resume has the higher sequence (the request on a tie) and applies the result to tasks already running before starting any task, so a retried start stays restart-suppressed while draining. These cluster-internal APIs are not a substitute for ordinary scoped operator access.

## Example

First-party clients treat a server address without an explicit scheme as HTTPS. In an API-enabled workload, use the injected namespace and CA rather than falling back to plaintext HTTP:

```sh
case "$TRELLIS_ADDR" in
  https://*) api_url=${TRELLIS_ADDR%/} ;;
  http://*) echo "refusing to send TRELLIS_TOKEN over plaintext HTTP" >&2; exit 1 ;;
  *) api_url="https://${TRELLIS_ADDR%/}" ;;
esac

printf '%s\n' "$TRELLIS_CA_CERT" > /tmp/trellis-ca.pem
curl -fsS --connect-timeout 5 --max-time 15 --cacert /tmp/trellis-ca.pem \
  -H "Authorization: Bearer $TRELLIS_TOKEN" \
  "$api_url/v1/namespaces/$TRELLIS_NAMESPACE/jobs"
```

Never send a workload bearer credential over plaintext HTTP. See [`examples/api-access/`](../../examples/api-access/) for an in-allocation namespace-scoped helper with the same TLS and timeout behavior.
