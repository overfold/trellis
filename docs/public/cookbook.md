# Cookbook

This cookbook starts with an operational outcome and explains the reusable Trellis pattern that achieves it. It is not an example index and does not define new resource types. Each recipe composes the primitives in the [job manifest reference](job-specification.md), explains why the composition works, and calls out the tradeoffs that should survive when you adapt it.

Use [Getting Started](getting-started.md) and the [learning path](learning-path.md) to learn Trellis in order. Use this page after you understand jobs, task groups, tasks, allocations, health, and namespaces.

## Put stable ingress in front of changing allocations

**Outcome:** expose one stable service endpoint while replicas scale, move between nodes, and roll to new revisions.

Give the serving task group a routing label and a meaningful health check, and leave it on the default namespace network:

```yaml
labels:
  route: web

tasks:
  - name: app
    image: registry.example.com/app:v1
    health_check:
      type: http
      port: 8080
      path: /ready
```

The replicas need no published node port: the proxy joins the same namespace network and reaches each replica's private address on port 8080 directly, so replicas can share nodes and roll without spare port capacity. Only the proxy publishes its listeners on the node:

```yaml
tasks:
  - name: proxy
    image: registry.example.com/proxy:v1
    networking:
      ports:
        - port: 8443
          host_port: 443
```

Run a fully trusted controller with `api_access` set to `scope: cluster` and the narrowest required `access` level. It should query allocations by an explicitly configured namespace and label, include only healthy endpoints, render or update the upstream set, and preserve its last known-good routing state through temporary control-plane failures. The credential itself is cluster-wide; namespace filtering is controller behavior, not authorization. The bundled `trellis-proxy-sync` implements this polling pattern. It writes each upstream as the allocation's address and the port the task listens on: the namespace address for namespace-networked backends, reachable from a proxy in the same namespace, or the node address for host-networked ones. Pass the application port with `-container-port`; it selects among an allocation's declared ports and is used directly for backends that declare none.

Give `trellis-proxy-sync` write access to the output config's parent directory, even when the config file already exists and is writable. It writes a temporary file there and renames it over the config so the proxy never reads a partial update. The output must be a regular file (or a symlink to one); missing files are created with mode `0644`, subject to the process umask and parent directory's default ACL. For an existing config, the synchronizer preserves its owner, group, mode, ACLs, and security labels; its process must be permitted to set that metadata.

Keep the public listener itself stable: place it deliberately or put an external load balancer in front of it. The routing controller is ordinary workload code, so make its retries, timeouts, reload behavior, and credential handling explicit. Trellis discovers endpoints; it does not make the proxy highly available for you.

## Connect services privately inside a namespace

**Outcome:** let services communicate across nodes without exposing their application ports on the node network.

Namespace networking is the default; stating `networking.mode: namespace` makes the choice explicit:

```yaml
tasks:
  - name: api
    image: registry.example.com/api:v1
    networking:
      mode: namespace
```

Optionally add `runtime: runsc` at the task-group level for additional syscall-level sandboxing.

Every node runs the WireGuard mesh behind namespace networking; across nodes, allow its UDP range between them as described in [Multi-node clusters](multi-node.md#prepare-the-network-and-configuration). Namespace peers reach each other's listening ports directly, without declaring or publishing them. Healthy allocation endpoints enter Trellis discovery, and DNS names follow the shape:

```text
group.job.namespace.trellis
```

Use DNS for locating healthy service instances in the caller's own namespace, not for leader election, distributed locking, or application consensus. A namespace-networked workload receives no records when it puts another namespace in the name. Namespace discovery is an availability mechanism; applications that require a single writer or elected primary still need their own coordination protocol.

Declare `ports` only for what must be reachable from outside the namespace network; each is published on the node. Use `host` networking when a task deliberately needs the node's network stack, and `mode: none` for work that needs no network at all. Networking is selected per task, so colocated tasks do not need to share the same exposure model.

## Choose task-group boundaries deliberately

**Outcome:** couple only the containers that must share placement, scaling, update, restart, and drain behavior.

Put tightly related processes in one task group when each replica should contain all of them—for example an application plus a local metrics exporter or proxy:

```yaml
task_groups:
  - name: web
    count: 3
    tasks:
      - name: app
        image: registry.example.com/app:v1
      - name: helper
        image: registry.example.com/helper:v1
```

`count: 3` creates three allocations and therefore three copies of both tasks. Give every task its own resources and health check when relevant.

Do not use a task group merely as a manifest-organizing device. If two components must scale independently, be updated independently, survive each other's failures, or run on different classes of nodes, separate them into different task groups or jobs. Task-group boundaries are lifecycle boundaries.

## Gate service readiness on application health

**Outcome:** keep a process out of routing and rollout decisions until it can actually serve useful work.

Use a health check that represents readiness rather than mere process existence:

```yaml
health_check:
  type: http
  port: 8080
  path: /ready
  interval: 5s
  timeout: 2s
  threshold: 2
```

HTTP and TCP checks are appropriate when readiness is externally observable. Script checks are useful when readiness depends on an in-container condition or when the task is not reachable through a host port.

A check should be strong enough to reject an unusable instance but not so broad that an unrelated downstream outage marks every replica unhealthy. This matters especially for rolling updates and health-filtered discovery. A task without an explicit check is treated as healthy once running, which is convenient for simple workloads but weaker than application-aware readiness.

## Restart transiently failing tasks without hiding persistent failure

**Outcome:** recover automatically from occasional process failures while eventually surfacing a broken workload for diagnosis.

Configure the restart policy on the task group:

```yaml
restart:
  max_restarts: 3
  window: 5m
```

The group-level policy applies to the tasks in each allocation. Use a small bounded retry budget for failures that are plausibly transient. Once the allowed failures in the window are exhausted, the allocation becomes `failed` permanently instead of entering an unlimited crash loop, and its record and event history remain for inspection. The control plane replaces it with a new allocation to keep the group at `count`, so a persistent defect shows up as a series of failed allocations rather than as one restarting container. Those replacements back off: 10s after the first failure, doubling with each consecutive failure up to 5m, and resetting once a replacement has run for 10 minutes without being reported unhealthy. Only the five newest terminal allocation records per task group are kept, so older failures eventually disappear from `trellisctl jobs status`; the **Replacement backoff** section still reports the failure count, the latest failed allocation and its reason, and when the next replacement is due.

Do not treat restart policy as a substitute for readiness checks or correct dependencies. Repeated startup failures usually indicate a bad revision, missing secret, unavailable volume, invalid configuration, or application defect; use `trellisctl jobs status NAME` after the retry budget is exhausted. Applying a corrected image or configuration creates a new job revision, which is placed immediately instead of waiting out the backoff. When the fix is outside the job, such as a restored secret, volume, or dependency, reset the backoff instead of waiting for it:

```sh
trellisctl jobs reset-backoff web api
```

The backoff only delays replacing failed allocations. Allocations lost with an unavailable node and capacity added by a higher `count` are placed without waiting.

## Choose between recreate and rolling replacement

**Outcome:** make update behavior match the workload's overlap and availability requirements.

Use the default `recreate` strategy when old and new instances must not overlap, temporary reduced capacity is acceptable, or the workload cannot safely run two revisions simultaneously:

```yaml
update:
  strategy: recreate
```

Use `rolling` when service availability matters and old and new revisions may coexist:

```yaml
count: 3
update:
  strategy: rolling
  max_parallel: 1
```

Rolling replacement marks old-revision allocations as draining, starts bounded replacement capacity, and removes old allocations as healthy replacements become available. `max_parallel` limits both not-yet-healthy replacements and temporary live capacity above `count`; a stop must succeed before its capacity can be reused. It is not a percentage.

Rolling updates require spare schedulable capacity and a useful health check. If the group publishes or reserves a fixed node port, spare capacity also means another node where that port is available. Recreate updates avoid overlap but can reduce or eliminate service capacity during replacement. In either case, preview with `trellisctl jobs apply --dry-run` before applying and treat rollback as another desired-state revision: restore the earlier image/configuration and apply it again.

## Switch complete releases with blue/green routing

**Outcome:** validate a full new release before moving production traffic, while keeping a fast route-only rollback.

Trellis has no blue/green resource type. Run the two releases as independently named jobs or otherwise independent routing targets. Give each release a distinct discovery label, validate the inactive release, then change the external routing controller from the old label to the new one.

Keep the old release alive for an observation window so rollback is a routing change rather than another deployment. Because both releases coexist, budget roughly double workload capacity during the overlap. If both releases reserve the same host port, they also require distinct nodes for every simultaneous allocation. Shared databases, queues, and message formats must remain compatible with every revision that can run at the same time.

Store and review the routing switch like application code. Trellis maintains the workloads; the proxy or external load balancer owns which release receives production traffic.

## Expose a canary to a bounded share of traffic

**Outcome:** send limited real traffic to a new release while the stable release continues serving most requests.

Run stable and canary as separate jobs or independently routable groups. Give them the same route label and distinct release metadata. With the bundled proxy synchronizer, `trellis/weight` can be passed through to a proxy template:

```yaml
labels:
  route: web
  track: canary
  trellis/weight: "5"
```

Start with little canary capacity and low effective routing weight. Compare errors, latency, saturation, and application-specific success metrics by release track before increasing exposure.

Weights are attached to discovered allocations. Replica count therefore changes aggregate weight: four stable allocations at weight 100 and one canary at weight 5 produce an aggregate 400:5 pool, not 100:5. Sticky sessions and long-lived connections can further change observed traffic share. Calculate the effective pool rather than treating one label value as a percentage.

## Isolate independent tenants or environments with namespaces

**Outcome:** keep workloads, discovery, secrets, and namespace networks separated even when they share one cluster.

Use different namespaces when two sets of workloads should not share resources, private networking, or discovery. A namespace is not just a prefix for job names, but it is not an API credential authorization boundary.

Keep related services that need private discovery in the same namespace. Put unrelated tenants, trust domains, or environments in different namespaces. Create CLI contexts that select the intended namespace so routine commands do not depend on remembering `--namespace` every time.

Do not emulate namespaces with job-name prefixes or labels. Labels are useful for selection and routing; they are not an authorization boundary.

Namespaces separate Trellis-owned resources but do not restrict cluster API credentials or decide which valid manifest capabilities a submitter may request. If submitters are untrusted tenants, put a trusted, policy-enforcing and tenant-authorizing frontend in front of the API and follow [Multitenancy and trust boundaries](multitenancy.md).

## Place workloads only on compatible nodes

**Outcome:** schedule a task group only where its runtime, architecture, hardware, locality, or operator-managed dependency is available.

Use task-group constraints for exact matches against `os`, `arch`, or node labels:

```yaml
constraints:
  - attribute: arch
    value: amd64
  - attribute: storage
    value: fast
```

Treat node labels as declared capabilities rather than arbitrary decoration. Apply a label only when every node carrying it really satisfies the implied contract.

Constraints are hard filters: if no healthy non-draining node satisfies them and has enough capacity, the workload remains pending. Do not over-constrain ordinary replicas when soft scheduler spreading is sufficient. Use constraints for requirements; let the scheduler choose among equivalent nodes.

## Rotate a credential without storing plaintext in the manifest

**Outcome:** deliver credentials to containers while keeping plaintext out of Git and job YAML.

Create or update the namespace secret separately from the workload:

```sh
printf %s "$NEW_VALUE" | trellisctl --namespace payments secrets set service-credential --stdin
```

Reference only the secret name from the task and choose environment or file delivery according to the application's interface. Prefer a file when the application supports it and a credential does not need to appear in the process environment.

For concurrent automation, read secret metadata and update with `--expected-version N`. A secret update affects allocations started afterward; Trellis does not mutate the environment or filesystem of an already-running container. Rotate consumers by replacing allocations in an order compatible with both the old and new credential.

Back up the secrets-encryption key separately from desired-state backups. Encrypted secret records are not recoverable without that key.

## Use local volumes without confusing identity and path

**Outcome:** keep node-local data stable across allocation replacement while making it explicit which node owns it and where its bytes live.

Every volume has three separate pieces: `name` is the namespace-scoped identity used by the scheduler, `host_path` is the node-side backing directory, and `container_path` is the mount destination. For Trellis-managed local storage, use the `@/` prefix:

```yaml
volumes:
  - name: cache
    host_path: "@/cache"
    container_path: /var/cache/app
```

`@/cache` resolves below Trellis's volume root for the job namespace and Trellis creates that directory on the node chosen for first placement. If you need an operator-prepared directory instead, use a clean absolute path:

```yaml
volumes:
  - name: database
    host_path: /srv/trellis/database
    container_path: /var/lib/app
```

An absolute path must already exist and is used verbatim, so filesystem-level namespace isolation is the operator's responsibility. In both forms, the first allocation using an unseen `(namespace, name)` durably binds that identity to one node. Later allocations using the same identity return to that node; loss of the owner leaves them unplaced rather than causing Trellis to create a second copy. Changing `host_path` changes the backing directory on the same owning node and never moves bytes.

A scaled task group repeats the same volume identities in every replica, so sharing a volume name also shares locality. Stateful replicas that need independent local disks should be modeled as independently named task groups/jobs with distinct volume names, plus application-level replication or another storage system. Back up volume data separately; Trellis desired-state backups preserve the ownership metadata, not the bytes.

## Give a trusted operator workload cluster API access

**Outcome:** let a fully trusted workload perform ordinary cluster-wide operations.

Request cluster access on the controller's task group:

```yaml
api_access:
  scope: cluster
  access: read
```

Trellis creates a cluster-scoped bearer token for each allocation. It injects:

- `TRELLIS_ADDR` — workload-reachable control-plane address;
- `TRELLIS_TOKEN` — cluster-scoped bearer token;
- `TRELLIS_NAMESPACE` — the job namespace, as routing convenience for `/v1/namespaces/{namespace}/...` request paths;
- `TRELLIS_CA_CERT` — cluster CA PEM when TLS is configured.

Use a namespace-aware client and verify TLS with the injected CA. Treat an address without an explicit scheme as HTTPS, matching first-party Trellis client behavior. Requests send Bearer authentication and address `/v1/namespaces/$TRELLIS_NAMESPACE/...`; never send the workload credential over plaintext HTTP.

Every task in the group receives the injected environment, so use only reviewed images in an API-enabled group. `TRELLIS_NAMESPACE` does not restrict the token: changing it or directly constructing another namespace path grants access there. Controllers should set request deadlines, retry transient failures with backoff, tolerate resources changing between reads, avoid leaking credentials into logs or metrics, and preserve useful last-known-good state through temporary API outages.

Cluster mode is appropriate only for an operator workload that genuinely needs ordinary API reads or writes. It does not grant credential minting, backup/restore, node enrollment, or Raft administration; those remain administrator, join-token, or node-identity operations. Treat compromise of any task in the group as compromise of the cluster credential. Pin and review images, avoid unrelated sidecars, and keep the token out of logs, metrics, and browser code.

## Apply database schema migrations safely across deployment strategies

**Outcome:** evolve a database schema without downtime or data loss, regardless of whether the workload uses rolling, blue/green, or canary deployment.

Database migrations are a deployment concern, not a container lifecycle concern. Rolling, blue/green, and canary releases run old and new application code simultaneously against the same database, so the schema must remain compatible with both versions during their overlap. The default `recreate` strategy stops old allocations first, but backwards-compatible migrations still make rollback and mixed external clients safer.

### Development and single-instance environments

Use application-level auto-migration: the application checks for pending migrations on startup, controlled by an environment variable so it can be disabled in production. Combined with a health check, the allocation stays unhealthy until migrations complete and the server starts listening:

```yaml
tasks:
  - name: app
    image: registry.example.com/app:v1
    env:
      AUTO_MIGRATE: "true"
    health_check:
      type: http
      port: 3000
      path: /health
```

This is safe for single-instance dev environments where concurrent migration attempts and backwards compatibility are not concerns.

### Production environments

Apply migrations as a controlled pre-deployment step, separate from the workload update. The operator or CI/CD pipeline runs the migration, verifies it succeeded, and only then applies the new job revision:

```sh
# 1. Apply the backwards-compatible migration
trellisctl exec ALLOC_ID -- your-migration-command
# or connect directly to the database from a CI runner

# 2. Verify the migration succeeded and existing workload still works

# 3. Deploy the new application code
trellisctl jobs apply --file app.yaml --wait
```

Write every production migration using the expand-and-contract pattern:

1. **Expand:** add the new column, table, or index. The currently running code continues to work against the new schema.
2. **Deploy** the new application code that uses both old and new schema structures.
3. **Contract** (in a later release): remove the old column or compatibility code once all instances run the new version.

This pattern is required because rolling, blue/green, and canary deployments all result in two application versions sharing one database. An `ALTER TABLE` that breaks the running version will cause failures regardless of deployment strategy.

For large-table schema changes that would lock reads or writes, use an online schema change tool (such as `pg_osc` or `pgroll` for PostgreSQL) rather than a blocking `ALTER TABLE`. These tools copy data incrementally and swap atomically without holding table locks.

Do not use orchestrator-level init containers or lifecycle hooks for production migrations. They couple the migration to the container lifecycle: in a rolling update, the migration runs once per replica (or per restart), they cannot gate the deployment across all replicas, and they cannot be observed or rolled back independently of the workload.

### Scheduled database maintenance

Run periodic maintenance (backups, `VACUUM`, integrity checks) inside a long-running sidecar or dedicated container with an internal scheduler such as `cron`. This is ordinary workload scheduling, not a Trellis-specific mechanism:

```yaml
task_groups:
  - name: db-maintenance
    count: 1
    constraints:
      - attribute: database
        value: "true"
    tasks:
      - name: maintenance
        image: registry.example.com/db-tools:v1
        volumes:
          - name: backups
            host_path: "@/db-backups"
            container_path: /backups
```

Trellis keeps the maintenance container running and restarts it on failure. The container's internal cron or loop handles the schedule.

## Run application-managed replicated state without confusing scheduling with consensus

**Outcome:** let Trellis place and operate replicated stateful members while the application remains responsible for data correctness and leadership.

Use Trellis for the container layer: replica count, placement constraints, network attachment, secret delivery, local-volume requirements, restart policy, health observation, and discovery. Use the stateful system's native mechanisms for replication, leader election or consensus, fencing, membership changes, backups, and recovery.

Do not infer a primary from Trellis scheduling order or health status. The scheduler places replicas of a task group on different eligible nodes when it can, which improves failure distribution, but it co-locates them when constraints, volumes, host ports, or capacity leave fewer eligible nodes than replicas, and it is not a consensus algorithm. Use constraints or host volumes when members must be on particular nodes. Trellis discovery tells members where healthy allocations are; it does not decide which member may accept writes.

Before treating such a deployment as highly available, test node loss, leader loss, stale members, replacement onto a node with different local data, restore from backup, and network partitions. If the storage layer is network-backed, verify that the application's own failover model safely controls which member mounts or writes the data.
