# Job manifest reference

A **job manifest** is the first-party human-authored representation of one Trellis job. The CLI, dashboard, documentation, and examples use YAML because it is pleasant to edit, but the control-plane API does not process YAML. Consumers convert their representation into the canonical JSON `JobSpec` before calling Trellis.

> **Consumers own representation; Trellis owns meaning.** YAML, HCL, Python, forms, or another frontend may provide their own authoring conveniences. Consumers are responsible for converting those conveniences into canonical JSON. Trellis remains authoritative for validation, defaults, planning, version and revision semantics, and reconciliation.

The repository publishes two schemas:

- [`schemas/trellis-job.schema.json`](../../schemas/trellis-job.schema.json) describes the first-party YAML authoring representation and enables editor completion/diagnostics.
- [`schemas/trellis-job-api.schema.json`](../../schemas/trellis-job-api.schema.json) describes the canonical JSON API representation.

The schema improves editing but never replaces `trellisctl jobs apply --check` or server validation.

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/overfold/trellis/main/schemas/trellis-job.schema.json
name: web
namespace: default
task_groups:
  - name: frontend
    count: 2
    runtime: runc
    api_access:
      scope: namespace
      access: read
    labels:
      route: web
    constraints:
      - attribute: arch
        value: amd64
    restart:
      max_restarts: 3
      window: 5m
    update:
      strategy: rolling
      max_parallel: 1
    tasks:
      - name: app
        image: ghcr.io/overfold/trellis-tutorial:v2
        networking:
          mode: host
          ports:
            - port: 8080
        resources:
          cpu: 100
          memory: 64MiB
        health_check:
          type: http
          port: 8080
          path: /health
          interval: 5s
          timeout: 2s
          threshold: 2
```

Because the sample uses host networking and reserves port 8080, its two replicas must run on different nodes. A rolling replacement also needs another compatible node with port 8080 available while old and new allocations overlap.

Check locally with `trellisctl jobs apply --check --file trellis.yaml`, preview with `trellisctl jobs apply --dry-run --file trellis.yaml`, and apply with `trellisctl jobs apply --file trellis.yaml`. The dashboard's **Apply Manifest** editor accepts the same YAML, converts its human values to canonical JSON, and asks the control plane for the same semantic plan.

## Representation boundary

The YAML layer accepts human-readable quantities:

```yaml
resources:
  memory: 256MiB
health_check:
  interval: 10s
```

The canonical JSON representation uses machine values instead:

```json
{
  "resources": { "memory": 268435456 },
  "health_check": { "interval": 10000000000 }
}
```

Memory is bytes and durations are nanoseconds in the current API model. Parsing strings such as `256MiB`, `4GB`, or `10s` is therefore a responsibility of the authoring consumer, not the Trellis HTTP API. Omitted values and their effective defaults remain Trellis semantics; a consumer should not duplicate those rules.

### Defaults and the stored job

Before a job is planned or stored, Trellis resolves every omitted optional field to its effective value. The stored job, `trellisctl jobs get`, and plans therefore show exactly what Trellis runs, and later changes to cluster settings or Trellis defaults never change the behavior of a job that is already stored. Applying the same manifest again produces no change because its resolved form is identical.

| Omitted field | Resolved value |
| --- | --- |
| task group `runtime` | `runc` |
| task group `restart` | `max_restarts: 3`, `window: 10m` |
| task group `update` | `strategy: recreate`, `max_parallel: 1` (zero also means one) |
| task `networking` or `networking.mode` | `mode: isolated` |
| task `resources` | the cluster's `default_task_cpu` and `default_task_memory` job limits at apply time |
| `health_check.interval`, `timeout`, `threshold` | `10s`, `5s`, `3` (zero also selects the default) |
| HTTP `health_check.path` | `/` |
| file secret `mode` | `0400` (zero also selects the default) |

## Job fields

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | Yes | Job identifier, unique within its namespace. |
| `namespace` | Yes | Namespace containing the job and its runtime allocations. |
| `task_groups` | Yes | One or more placement and scaling units. |

There is no job-level networking block. Network attachment belongs to each task because different tasks in one group may require different isolation.

## Task-group fields

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | Yes | Group identifier, unique within the job. |
| `count` | Yes | Desired allocation count; must be at least one. |
| `tasks` | Yes | One or more containers placed in every allocation. |
| `runtime` | No | `runc` (the default) or `runsc`. |
| `labels` | No | Discovery and routing metadata. |
| `api_access` | No | Least-privilege API credential request. Omit it for no injected API credentials. |
| `constraints` | No | Exact matches against `os`, `arch`, or node labels. |
| `restart` | No | Retry policy for failed tasks. |
| `update` | No | Replacement strategy when execution-affecting desired state changes. |

### Labels and constraints

Label keys and constraint attributes must begin with a letter, may contain letters, digits, `.`, `_`, `/`, and `-`, and are limited to 63 characters. Label values are limited to 256 characters. Constraint values must not be blank, and a task group may constrain each attribute only once.

### API access

`api_access` has two independent dimensions:

```yaml
api_access:
  scope: namespace
  access: read
```

`scope` is `namespace` or `cluster`. `access` is `read` or `write`; write includes read capability.

- `namespace/read` is appropriate for discovery, observers, and namespace-local read-only controllers.
- `namespace/write` is appropriate for trusted controllers that deliberately mutate jobs in their own namespace.
- `cluster/read` can inspect cluster-scoped state and is the credential used by the read-only first-party dashboard.
- `cluster/write` is the normal high-privilege operator/controller credential for cluster-wide mutations.
- omitted means no API credential is injected.

A job may never delegate more authority than the credential submitting it. Namespace-scoped callers cannot request cluster-scoped workload credentials, and read-only callers cannot request write credentials. Planning enforces the same ceiling as apply so a preview cannot advertise a deployment the caller is not authorized to create.

The administrator signing key is intentionally separate. Trellis never injects it into workloads. Operations such as Raft administration, backup/restore, and minting ordinary operator credentials remain administrator operations rather than abilities granted by `cluster/write`. Node registration and heartbeats use certificate-bound node identity, while managed enrollment uses a separate enrollment credential.

Generated credentials carry an authoritative server-side kind (`operator` or `workload`) in addition to scope/access. `GET /v1/auth/whoami` reports the kind and effective authorization of the credential making the request; the administrator credential reports itself explicitly as `administrator`.

API-enabled allocations require the servers to be configured with the secrets encryption key (`secrets_key`, which the installer creates). Each allocation receives its own credential, bound to its job and task group and stable across start retries of the same allocation generation; a new generation receives a new credential and the previous one is revoked. Trellis revokes the credential when the allocation record is pruned, when the job or task group is deleted, or when the task group's `api_access` is removed or narrowed (from `cluster` to `namespace` scope, or from `write` to `read`). Narrowing takes effect immediately, including for allocations of the previous revision that are still running during a rollout. Widening `api_access` leaves existing credentials in place; replacement allocations receive the wider grant.

Enabled API access injects `TRELLIS_ADDR`, `TRELLIS_TOKEN`, and `TRELLIS_NAMESPACE`; when TLS is configured, `TRELLIS_CA_CERT` contains the cluster CA PEM. `TRELLIS_NAMESPACE` is initialized to the job namespace even for cluster-scoped credentials.

`TRELLIS_ADDR` is a Trellis-owned workload endpoint, currently exposed as the TLS name `trellis` on the control-plane port. Trellis maps that name to the node-local control plane for host-networked tasks and to the namespace gateway for namespace-networked tasks; the local listener proxies requests to the current leader. API-enabled tasks must therefore select either `networking.mode: host` or `networking.mode: namespace`. Omitted/isolated networking is rejected because it intentionally has no route to the control plane.

`api_access` is a task-group privilege boundary: every task in the group can read the credential. Do not colocate untrusted sidecars with an API-enabled controller. Request the narrowest scope and access level the workload needs.

When `restart` is omitted, Trellis stores a policy of three restarts per ten-minute window. An explicit `restart.max_restarts` is zero or greater and `restart.window` is a positive Go-style duration such as `5m`. The agent restarts a stopped task in place while the budget allows. Restarts are counted in fixed windows of `restart.window`, and the count resets when a window elapses. The count belongs to the allocation: control-plane start retries and agent restarts keep it and never grant a fresh budget, while a replacement allocation starts with its own. Once a task stops after the allowed restarts in the current window are used up, the allocation becomes `failed` with reason `restart_budget_exhausted` and is not restarted again, even after the window elapses. The failed allocation record and its event history remain for operator diagnosis; to keep the group at `count`, the control plane schedules a new allocation in its place.

Replacements of failed allocations are delayed by a per-task-group backoff so that a workload that always fails does not produce an unbounded series of allocations. With the default cluster settings, the first replacement waits 10s, and each further consecutive failure doubles the delay up to 5m. The count resets once an allocation placed after the latest failure has run for 10 minutes without being reported unhealthy, and applying a new job revision starts from zero. While the backoff is active, only the replacements of failed allocations wait: an allocation lost with its node, or a higher `count`, is placed immediately. `trellisctl jobs status` and the dashboard's job page show the backoff under **Replacement backoff**, with the failure count, the latest failed allocation and its reason, and the next replacement time. After fixing the cause without applying a new revision, `trellisctl jobs reset-backoff JOB GROUP` or the dashboard's **Reset backoff** action clears the backoff so the failed allocations are replaced at once. The control plane also keeps only the five newest (by default) stopped, failed, or lost allocation records per task group. If a node later reports a container for a pruned allocation, reconciliation treats it as an orphan and stops it. These values are replicated [cluster settings](operations.md#cluster-settings) rather than manifest fields.

When a node stops sending heartbeats for longer than the allocation loss timeout (45 seconds by default), its allocations become `lost` and are replaced like failed ones. Lost is terminal: if the node returns, its lost allocations are never adopted again. While a lost allocation record is retained, containers the returning node still runs for it keep running until the group has `count` running replacements, and are then stopped. If an older pruned allocation is later reported, Trellis stops it as an observed orphan. They are stopped earlier when they block a replacement, for example by holding a host port the replacement needs on the same node. The loss timeout is a replicated [cluster setting](operations.md#cluster-settings) (`allocation_loss_timeout`), not a manifest field. See [lost allocations](user-model.md#lost-allocations).

`update.strategy` is `recreate` (the default) or `rolling`. For rolling updates, `max_parallel` limits both the number of not-yet-healthy replacements in flight and temporary live capacity above `count`. A stop frees capacity only after it succeeds, so a failed stop cannot admit an excess replacement. Omission or zero resolves to one.

Task groups are the unit of placement, scaling, updates, restart behavior, and draining. Every task in a group is coupled to that lifecycle.

## Task fields

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | Yes | Task identifier, unique within the group. |
| `image` | Yes | Pullable OCI image reference. Pin a version or digest for reproducible deployment. |
| `env` | No | Literal environment-variable map. Do not place credentials here. |
| `networking` | No | Network mode and, for host mode, optional port reservations. |
| `resources` | No | CPU in millicores and memory as a byte count or readable size. |
| `volumes` | No | Namespace-scoped named volume mounts with explicit host and container paths. |
| `secrets` | No | References to namespace secrets delivered as environment variables or files. |
| `health_check` | No | HTTP, TCP, or script readiness/health observation. |

### Networking and ports

```yaml
networking:
  mode: host
  ports:
    - port: 8080
```

`networking.mode` is:

- `isolated` (the default when omitted): a private container network namespace with no external routes;
- `host`: join the node network namespace directly;
- `namespace`: join the private Trellis network belonging to the workload namespace.

`namespace` deliberately describes the networking semantics rather than the transport implementation. Trellis currently realizes this mode with WireGuard, so participating nodes require the corresponding WireGuard setup. Adding `runsc` (gVisor) is recommended for additional syscall-level sandboxing but is not required.

Port declarations are valid only with `mode: host`. Host networking has no Trellis NAT or port-forwarding layer, so there is no separate host/container port distinction in desired state. `port` is both the node port Trellis reserves and the port the process must listen on. It must be 1–65535 and unique across all tasks in a task group. A fixed port can be used only once per node, so replicas reserving the same port need distinct nodes.

### Resources

```yaml
resources:
  cpu: 250
  memory: 256MiB
```

CPU is expressed in millicores. The first-party YAML representation accepts a raw byte count or readable binary/decimal size such as `256MiB`, `1GiB`, or `500MB`; canonical JSON represents memory as integer bytes. The scheduler multiplies each task request by its group count when considering desired capacity. A task may omit `resources`; Trellis resolves it to the operator-configured default CPU and memory before persistence and scheduling. When supplied, both values must be positive; zero never requests the default.

The memory request is also the task's hard memory limit, including swap: Trellis sets the combined memory and swap limit to the same value, so a task cannot exceed its declared memory by swapping. On hosts whose memory cgroup lacks swap accounting (cgroup v1 without `memory.memsw.*`, or cgroup v2 without `memory.swap.max`, for example when booted with `swapaccount=0`), the kernel cannot enforce this and a node with active swap logs a warning at startup; enable swap accounting or disable swap on such hosts. These cgroup limits are applied when a node creates a task container, so containers created before a node gained them keep their earlier limits until the allocation is replaced. Each task container is also limited to the node's configured number of processes and threads (`task_pids_limit`, default 4096; see [Node configuration](operations.md#node-configuration)); that limit is operator policy and has no job field.

### Volumes

```yaml
volumes:
  - name: cache
    host_path: "@/cache"
    container_path: /var/cache/app
  - name: database
    host_path: /srv/postgres
    container_path: /var/lib/postgresql/data
    read_only: false
```

Every volume has three independent pieces of information. `name` is its stable logical identity within the job namespace and is used for locality-aware scheduling. `host_path` is the backing directory on the owning node. `container_path` is the absolute mount destination inside the container.

The first allocation that uses a previously unseen `(namespace, name)` establishes that volume's node registration. The node persists and advertises the registration, and later allocations using the same namespace and name are scheduled onto that node. Trellis does not silently create another copy on a different node if the owner is unavailable. A named volume therefore has a lifetime independent of any one allocation.

A `host_path` beginning with `@/` is resolved relative to Trellis's volume root for the current namespace. With the default data directory, `@/database` in namespace `acme` resolves below `/var/lib/trellis/data/volumes/namespaces/acme/database`; Trellis creates that directory when realizing the first allocation. `@/` is only a path prefix: the volume identity still comes from `name`.

An absolute `host_path` such as `/srv/postgres` is used verbatim and must already exist on the selected node. This is an intentional escape hatch and **does not provide filesystem-level namespace isolation**: two namespaces can point at the same absolute host directory if an operator configures them that way. Use node constraints when an absolute path only exists on particular nodes so first placement does not repeatedly choose an unsuitable node.

Trellis mounts every volume `nosuid` and `nodev`: setuid bits and device nodes in the backing directory have no effect inside any container that mounts it, and tasks cannot create device nodes. Files in volumes remain executable. The flags do not rewrite stored files, so host users with access to an absolute `host_path` should treat its contents as untrusted.

Namespace authorization does not prevent a manifest submitter from requesting an absolute path. A frontend serving untrusted tenants must reject this form; see [Multitenancy and trust boundaries](multitenancy.md).

Changing `host_path` does not change the volume identity or move it to another node. A later revision may point the same name at another path on its registered node, but Trellis does not copy or migrate the bytes; preparing the new backing data is the operator's responsibility.

Because locality is attached to `(namespace, name)`, multiple allocations that use the same name intentionally share one node registration. This is useful when several tasks need the same local data, but it also means a replicated stateful system that requires independent disks on separate nodes must use distinct volume names for those members. Trellis does not replicate, snapshot, back up, or migrate volume contents.

### Secrets

```yaml
secrets:
  - name: api-token
    target: env
    env: API_TOKEN
  - name: tls-key
    target: file
    path: /run/trellis-secrets/tls.key
    mode: 256 # decimal form of 0400
```

An environment target requires only a valid `env` name and may not collide with `env`. Its plaintext is not stored in containerd's OCI metadata, but it necessarily exists in the running process environment. A file target requires a clean path below `/run/trellis-secrets/`; mode may be `0400` or `0600` (or their YAML numeric values), and omission or zero resolves to `0400`. Trellis assigns the mounted file to the image-configured process UID/GID so owner-only files work for non-root images. Names, environment targets, and file paths must be unique within a task.

### Health checks

HTTP and TCP checks require a port:

```yaml
health_check:
  type: http
  port: 8080
  path: /health
  interval: 5s
  timeout: 2s
  threshold: 2
```

A script check instead requires a nonempty command:

```yaml
health_check:
  type: script
  command: ["/usr/local/bin/check-ready"]
```

`interval`, `timeout`, and `threshold` must be non-negative. Zero or omission resolves to a 10-second interval, 5-second timeout, and threshold of 3 when the job is applied. Positive values override those defaults. HTTP and TCP checks target the configured port on loopback inside the task's own network environment, regardless of networking mode or runtime. An HTTP check's `path` is the request path and optional query sent unchanged to that port. It must begin with `/` and be at most 1024 bytes, and may contain only letters, digits, `-._~!$&'()*+,;=:@/?`, and `%XX` percent-encodings (encode anything else; a `#fragment` is not allowed). An omitted path resolves to `/`. TCP and script checks ignore `path`. A response from 200 through 399 is healthy. The probe follows plain-HTTP redirects to `127.0.0.1` or `localhost` on the same port; more than 10 such redirects fails the check. A redirect anywhere else, including another loopback address or port, is not followed and its 3xx response is the result, so a service that only redirects elsewhere (for example to HTTPS) passes its check. A check never leaves task-local loopback. Script checks execute the supplied command in the task normally. `/run/trellis` is reserved for Trellis-managed task files and cannot be used as a volume destination. A running task without an explicit health check is treated as healthy, which is useful for the first tutorial but weaker than application-aware readiness for a service.

## Validation and editor tooling

Job, namespace, group, task, secret, and volume identifiers accept letters, digits, `_`, `.`, and `-`, must begin with a letter or digit, and are limited to 63 characters. Unknown YAML fields are rejected by the first-party parser.

The server also applies operator-configured admission limits after resolving default resources. Defaults are 500 replicas per task group, 64 task groups per job, 32 tasks per task group, 1,000 desired allocations per job, 10,000 desired allocations per namespace, 1,000,000 millicores per task, and 1 TiB memory per task. Operators may choose different values; see [Cluster settings](operations.md#cluster-settings). A manifest can therefore satisfy the structural schema yet exceed the target cluster's policy.

The YAML schema is intended for VS Code, Neovim, Zed, and other editors that support YAML language-server schemas. Checked-in beginner/intermediate examples use a stable raw-GitHub `yaml-language-server` schema URL, so completion and basic diagnostics continue to work when a manifest is copied out of the repository. Schema diagnostics are structural assistance only; `trellisctl jobs apply --check`, `/v1/jobs/plan`, and apply use Trellis's authoritative validator, which reports all independently actionable validation issues with paths and error codes.

Follow checked-in examples in learning order from the [examples index](../../examples/README.md), rather than copying an advanced architecture as a first workload.

[Documentation index](../README.md) · [Previous: Core concepts](core-concepts.md) · [Next: CLI workflows](cli.md)
