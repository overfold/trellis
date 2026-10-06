# Internal APIs and transport

This page describes node-to-node implementation contracts. Operator tools and external integrations use the [public HTTP API and Go client](../public/api.md), not these endpoints. Wire types live in `internal/nodeapi`, node and agent clients in `internal/client`, and shared HTTP transport in `internal/transport`. Public wire types and clients remain in `api` and `client` without server-side dependencies.

## Node identity and enrollment

`POST /v1/nodes`, `POST /v1/nodes/{id}/heartbeat`, `GET /v1/internal/discovery`, `POST /v1/raft/join`, and the agent API on port 8127 require a trusted node certificate whose URI SAN identifies the immutable node UUID. Every identity certificate also has the DNS SAN `<uuid>.node.trellis`; node-to-node clients verify that specific identity. Registration and heartbeat IDs must match the caller's UUID.

Each UUID is durably bound to its admitted certificate and role. Removed UUIDs have replicated tombstones and are rejected on node-authenticated paths, enrollment, and Raft join regardless of their certificate. Raft join is available only to control-plane identities, derives the member ID from the certificate, and admits a non-voter. In managed mode its response carries the member IDs and CA private key; workers never call this endpoint. Outbound Raft streams resolve the target address through membership and verify that exact UUID's certificate; inbound streams require a bound, non-removed control-plane member identity.

Managed-only `POST /v1/nodes/enroll` uses a role-bound join token over TLS authenticated with the pinned node CA. Enrollment takes `role` and a PEM `csr`, ignores requested names, and returns an identity certificate; the private key stays on the enrolling node. Only control-plane nodes join Raft and receive the CA key. `POST /v1/nodes/api-certificate` signs a CSR for the shared `trellis` API name only for a bound, non-removed current control-plane member. Its certificate is valid for 24 hours and renewed hourly; workers can never obtain that name. A worker's port 8128 relays TCP to a control-plane endpoint, preserving client-to-API TLS and client identity end to end. `GET /v1/internal/control-plane` and registration/heartbeat responses provide relay targets and the elected leader UUID; workers cache this public topology, not replicated state. `GET /v1/internal/node-role` reports the authenticated node's durable role for administrator-authorized promotion checks.

## Registration, heartbeats, and discovery

The authenticated control-plane topology response also includes `wireguard_port_count`. Workers fetch it before initializing local networking: an omitted local count adopts the cluster value, while an explicitly different count is rejected.

Registration reports physical and allocatable resources, workload eligibility (`runs_workloads`, default true), capabilities, and the node's WireGuard public key, advertised endpoint, local port base, and range size. Heartbeats carry a complete current observation, including capabilities and `raft_applied_index`; optional host usage and task log usage fields may be absent when counters are unavailable or the runtime cannot measure its log directory. They return no desired state. A `200` with topology JSON means the leader validated the report and recorded liveness, not that allocation observations have been durably persisted: observations are queued asynchronously. An unregistered node's heartbeat fails, causing registration before the next heartbeat.

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
