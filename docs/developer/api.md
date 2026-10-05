# Internal APIs and transport

This page describes node-to-node implementation contracts. Operator tools and external integrations use the [public HTTP API and Go client](../public/api.md), not these endpoints. Wire types live in `internal/nodeapi`, node and agent clients in `internal/client`, and shared HTTP transport in `internal/transport`. Public wire types and clients remain in `api` and `client` without server-side dependencies.

## Node identity and enrollment

`POST /v1/nodes`, `POST /v1/nodes/{id}/heartbeat`, `GET /v1/internal/discovery`, `POST /v1/raft/join`, and the agent API on port 8127 require a trusted node certificate whose URI SAN identifies the immutable node UUID. Followers redirect certificate-authenticated control-plane requests instead of proxying them with their own certificate. Registration and heartbeat IDs must match the caller's UUID.

Each UUID is durably bound to its admitted certificate. Removed UUIDs have replicated tombstones and are rejected on node-authenticated paths, enrollment, and Raft join regardless of their certificate. Raft join derives the member ID from the certificate, requires advertised hosts to match certificate SANs, and admits a non-voter. In managed mode the identity must already be bound by enrollment or bootstrap; external mode binds it on first join. The join response carries `members` (member IDs at admission) and, in managed mode, the CA private key. Outbound Raft streams verify the peer's joined address; inbound streams require a bound, non-removed member identity. See [membership and Raft transport authorization](control-plane.md#control-plane-membership).

Managed-only `POST /v1/nodes/enroll` uses a join-token bearer credential over TLS authenticated with the pinned node CA. Unknown, revoked, expired, or exhausted tokens return `401` without distinguishing the cause. The leader serializes enrollment and consumes a token use in the same replicated batch that binds the assigned UUID and certificate. Enrollment returns the certificate and private key without the CA key; only successful certificate-bound Raft admission delivers the CA key so that an admitted member can later lead enrollment.

## Registration, heartbeats, and discovery

Registration reports physical and allocatable resources, capabilities, and the node's WireGuard public key, advertised endpoint, local port base, and range size. Heartbeats carry a complete current observation, including capabilities and `raft_applied_index`; optional host usage fields may be absent when counters are unavailable. They return no desired state. A `204` means the leader validated the report and recorded liveness, not that allocation observations have been durably persisted: observations are queued asynchronously. An unregistered node's heartbeat fails, causing registration before the next heartbeat.

Heartbeat bodies are limited to 32 MiB and 320,000 allocation-task reports (`413` on excess). A task `reason` is allowed only for phase `failed`, and currently only `restart_budget_exhausted` is accepted. A task in phase `starting` may carry `start_failure`: `attempt`, a message of at most 1024 bytes, and optional code `stale_generation`, `execution_conflict`, or `restart_budget_exhausted`. Other phases reject that field. The leader counts a failure for the allocation's current attempt once; a non-retryable code immediately fails it.

Internal discovery exposes catalog entries only for namespaces with active allocations on the authenticated node. The node resolver additionally checks the workload's source namespace before answering. The leader combines durable namespace port slots with node-advertised bases to build WireGuard peer plans; see [networking](node-internals.md#networking).

## Leader-to-agent operations

The client verifies that the agent certificate identifies the scheduled node. The agent verifies that the caller identifies its locally known Raft leader. Start, stop, drain, resume, and network-plan mutations require a positive control epoch; allocation operations also require a positive generation, and starts verify revision and execution hash. Protocol conflicts reject stale or incompatible execution rather than changing newer work.

The agent acknowledges a fenced start before pulling images and creating tasks in the background. The request's `attempt` is excluded from the execution hash and is returned in background-start failure observations. Exhausted restart budgets reject same-generation starts with HTTP `409` and operation code `restart_budget_exhausted`.

Drain/resume requests carry a persisted intent sequence. The leader saves intent before delivery; a failed undrain save contacts no agent, while a delivery failure after a successful save is retried by reconciliation. Starts carry `draining` and `drain_sequence` outside the execution hash. The agent uses the higher sequence of its local intent and the request (the request wins ties), including for tasks already running, so delayed drains cannot override resumes and start retries cannot restart drained tasks. See [reconciliation](control-plane.md#reconciliation) and [agent convergence](node-internals.md#agent-convergence).

## Exec relay

The [public exec WebSocket](../public/api.md#exec-streams) is authorized at the leader and relayed over the node-certificate mTLS agent API. Clients never connect directly to agents. The relay carries the control epoch: agents reject older epochs and end open streams when fenced by a newer one, while a leader ends its relays when its term ends.

The leader buffers one frame per direction; the agent queues at most four input frames. Output is unbuffered, applying backpressure to the process. A blocked frame times out after 30 minutes. Limits are 256 relays per leader, 64 sessions per node, and 8 per allocation; admission stays held until the process exits, including while failed kills are retried. Streams are ephemeral, not Raft state. An agent crash can leave an exec process running until it exits or its container stops; graceful shutdown kills it. Runtime process confinement is described in [node internals](node-internals.md#runtime-abstraction).

[Documentation index](../README.md) · [Public HTTP API](../public/api.md)
