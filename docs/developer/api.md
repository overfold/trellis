# HTTP API

The control-plane API defaults to port 8128. Send `Authorization: Bearer TOKEN`; cluster-scoped callers may additionally select a namespaced view with `X-Trellis-Namespace: NAME`. JSON request bodies use `Content-Type: application/json`. Use TLS outside a local sandbox.

Trellis distinguishes three credential kinds:

- `bootstrap` — the root node/cluster credential used for node registration, Raft membership, backup/restore, and minting operator credentials;
- `operator` — an explicitly minted API credential with `namespace` or `cluster` scope and `read` or `write` access;
- `workload` — a scoped credential injected through task-group `api_access`.

Credential prefixes (`trls_boot_`, `trls_op_`, `trls_wl_`) are descriptive only. The server authenticates the complete bearer value and uses its authoritative stored principal metadata for generated credentials.

A task group requests workload access with an object such as `{"scope":"namespace","access":"read"}`. Namespace scope is restricted to the namespace containing the job. Cluster scope grants only the ordinary read/write API authority represented by the credential; it never turns into the bootstrap credential. Both scopes set `TRELLIS_NAMESPACE` to the job namespace as a default request scope.

The API uses the same resource vocabulary as the [Trellis user model](../public/user-model.md), but JSON is the transport representation. Humans author jobs as YAML manifests; job submission carries the equivalent JSON `JobSpec` inside the API request. Human-readable YAML memory sizes are normalized to byte counts in JSON.

## Public/operator endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics. |
| `GET` | `/v1/auth/whoami` | Return the current credential kind, scope, access, namespace, and available provenance metadata. |
| `GET` | `/v1/nodes` | List node capacity, discovered capabilities, and status; requires cluster scope. |
| `POST` / `DELETE` | `/v1/nodes/{id}/drain` | Drain or undrain; requires `cluster/write`. |
| `GET`, `POST` | `/v1/jobs` | List jobs or submit `{"spec": JobSpec}`. |
| `POST` | `/v1/jobs/plan` | Validate and calculate the authoritative semantic plan for a `JobSpec`. |
| `GET`, `DELETE` | `/v1/jobs/{name}` | Read job state/API representation or delete the job. |
| `GET` | `/v1/namespaces` | Discover namespace names visible to the caller. |
| `GET` | `/v1/allocations?label=key:value` | List/filter allocations. |
| `GET` | `/v1/allocations/{id}/events` | Lifecycle event array. |
| `GET` | `/v1/allocations/{id}/logs?task=NAME&tail=100&follow=true` | Plain-text logs for one task in an allocation. |
| `POST` | `/v1/allocations/{id}/exec` | Run one non-interactive command and capture stdout/stderr; requires write access. |
| `POST` | `/v1/allocations/{id}/exec/sessions` | Start an ephemeral interactive TTY session; requires write access. |
| `POST` / `GET` / `DELETE` | `/v1/allocations/{id}/exec/sessions/{session}/...` | Write input, read output, resize, or close an interactive TTY session; requires write access. |
| `GET` | `/v1/allocations/{id}/metrics` | Current per-task CPU and memory usage. |
| `PUT` | `/v1/namespaces/{ns}/secrets/{name}` | Set a secret; requires `cluster/write`. |
| `GET` | `/v1/namespaces/{ns}/secrets[/{name}]` | List/get secret metadata only; requires cluster scope. |
| `DELETE` | `/v1/namespaces/{ns}/secrets/{name}` | Delete a secret; requires `cluster/write`. |

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

A bootstrap credential reports `kind: "bootstrap"`, `scope: "cluster"`, and `access: "write"`, but callers must still treat `bootstrap` as more privileged than ordinary `cluster/write`: root-only endpoint checks use the credential kind/context, not merely those two effective fields.

`GET /v1/namespaces` is discovery, not namespace lifecycle management. For cluster-scoped or bootstrap callers it returns the sorted unique namespace names currently referenced by desired jobs. A namespace-scoped caller receives only its own namespace. Applying a valid job to a previously unseen namespace does not require a separate namespace-creation call; after that desired job exists, the name becomes discoverable.

For allocation logs, `task` selects the task name from the allocation's task group. It may be omitted when the allocation has exactly one task; a multi-task allocation returns `400` until the caller selects one. The allocation ID is the Trellis allocation identity, not an agent/container runtime ID.

Both non-interactive exec and interactive exec sessions create the command with the selected task container's OCI process context: environment variables, user, and working directory are inherited from the task. The command argv is supplied by the caller; Trellis does not invoke a shell implicitly. TTY sessions may additionally set or replace `TERM` from the session request.

Interactive exec sessions use the same task-selection rule. Create a session with `POST /v1/allocations/{id}/exec/sessions` and a body such as `{"task":"web","command":["/bin/sh"],"term":"xterm-256color","cols":120,"rows":32}`. `command` is required; Trellis does not choose a shell for the client. `term` is optional and, when present, is carried into the OCI process as `TERM`; Trellis does not assume a terminal type. The response is `{"id":"..."}`. Terminal bytes are transported as base64: send `{"data_base64":"..."}` to `.../{session}/input`, poll `.../{session}/output?offset=N` for `data_base64`, `next_offset`, `exited`, and optional `exit_code`, send `{"cols":120,"rows":32}` to `.../{session}/resize`, and `DELETE` the session when finished. Sessions are node-local, ephemeral diagnostics state: they are not persisted in Raft and end when the process, allocation, or explicit session closes.


Secret write body: `{"value_base64":"...","expected_version":1}`; omit `expected_version` for unconditional update. Lists are JSON arrays. Non-2xx responses are errors; clients must tolerate reconciliation-driven changes between reads.

A namespace credential is authorized only for its stored namespace regardless of the namespace header supplied by the caller. A cluster credential may deliberately select different namespaces but receives only the read/write authority encoded in its principal.

## Bootstrap and cluster-internal endpoints

`POST /v1/credentials`, `GET /v1/backup`, `POST /v1/backup/restore`, `DELETE /v1/raft/members/{id}`, and `POST /v1/raft/leadership-transfer` require an administrator credential; a node certificate does not grant these operations. `POST /v1/nodes` and `POST /v1/nodes/{id}/heartbeat` require mutual TLS and require the certificate's immutable node ID to match the requested node. Enrollment uses `POST /v1/raft/join`: the bootstrap credential is accepted only to issue a unique node certificate, after which the certificate authenticates the Raft-membership request. The enrollment response never includes the cluster CA private key. Node registration includes the node WireGuard public key, externally reachable base endpoint, local port-range base, and port-range size when namespace networking is available; heartbeats include discovered capabilities. The control plane combines the namespace's durable port slot with each node's advertised bases when building WireGuard peer plans. The scheduler derives requirements from workload runtime and networking fields; a pending allocation whose eligible nodes lack a required feature reports `missing_capability` and names the feature in its diagnostic message. Agent port 8127 accepts leader-driven allocation operations only from the node whose certificate matches the current Raft leader; it does not accept the bootstrap credential. These cluster-internal APIs are not a substitute for ordinary scoped operator access.

## Example

First-party clients treat a server address without an explicit scheme as HTTPS. In an API-enabled workload, use the injected namespace and CA rather than falling back to plaintext HTTP:

```sh
case "$TRELLIS_ADDR" in
  http://*|https://*) api_url=${TRELLIS_ADDR%/} ;;
  *) api_url="https://${TRELLIS_ADDR%/}" ;;
esac

printf '%s\n' "$TRELLIS_CA_CERT" > /tmp/trellis-ca.pem
curl -fsS --cacert /tmp/trellis-ca.pem \
  -H "Authorization: Bearer $TRELLIS_TOKEN" \
  -H "X-Trellis-Namespace: $TRELLIS_NAMESPACE" \
  "$api_url/v1/auth/whoami"
```

For deployments that deliberately use `http://`, omit `--cacert`. See [`examples/api-access/`](../../examples/api-access/) for an in-allocation namespace-scoped helper that handles both cases.
