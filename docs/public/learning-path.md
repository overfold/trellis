# Learning path

Complete [Getting Started](getting-started.md) first. It establishes the only golden path: install → connect → deploy → inspect → update → view logs → remove. This page then introduces one layer of Trellis at a time instead of beginning with an application architecture.

## The sequence

| Stage | Learn | Run |
|---|---|---|
| 1. Minimal workload | Job → task group → task → allocation; revisions and logs | [`examples/hello`](../../examples/hello/) |
| 2. Healthy service | Host networking, one fixed port reservation, and HTTP health | [`examples/web-service`](../../examples/web-service/) |
| 3. Replicas and placement | Multiple replicas and the scheduling consequences of fixed host ports | [`examples/replicated-service`](../../examples/replicated-service/) |
| 4. Rolling updates | Healthy overlap, `max_parallel`, and temporary capacity requirements | [`examples/rolling-update`](../../examples/rolling-update/) |
| 5. Runtime configuration | Namespace-scoped environment/file secrets and rotation | [`examples/secrets`](../../examples/secrets/) |
| 6. Persistence | Namespace-scoped volume identities, `@/` paths, explicit host paths, locality, and backup responsibility | [`examples/volumes`](../../examples/volumes/) |
| 7. Colocated tasks | Sidecars and the consequences of shared placement/scaling/lifecycle | [`examples/sidecar`](../../examples/sidecar/) |
| 8. Namespace networking | Isolated, host, and namespace networking; service discovery | [`examples/namespace-networking`](../../examples/namespace-networking/) |
| 9. In-cluster automation | Namespace/cluster scope and read/write API access | [`examples/api-access`](../../examples/api-access/) |
| 10. Release architecture | Rolling, blue/green, and weighted canary composition | [`examples/deployment-strategies`](../../examples/deployment-strategies/) |
| 11. Stateful compositions | Coupled development stacks, local-volume caveats, application-native HA | [`examples/wordpress`](../../examples/wordpress/), then [`examples/patroni`](../../examples/patroni/) |

Do not skip directly to Patroni to learn basic Trellis. Patroni assumes you already understand every earlier layer and still requires a real DCS, replication, fencing, routing, and independent data backups.

## 1. Minimal workload

The `hello` example intentionally omits network exposure and application health settings. Learn the core loop first:

```sh
trellisctl jobs apply --check --file examples/hello/trellis.yaml
trellisctl jobs apply --dry-run --file examples/hello/trellis.yaml
trellisctl jobs apply --file examples/hello/trellis.yaml --wait
trellisctl jobs status hello
trellisctl jobs logs hello
trellisctl jobs delete hello --wait
```

At this stage, understand that the manifest is desired state and the allocation is runtime state. Drill into allocation details only when status or logs require it.

## 2. Health and service networking

The `web-service` example keeps `count: 1` and adds only the pieces needed to make the tutorial application a reachable, application-aware service:

- `networking.mode: host` opts the task into the node network;
- `networking.ports` reserves the exact `port` the process listens on;
- the HTTP health check decides when the running task is ready.

Host networking has no Trellis NAT or port translation. The reservation prevents another Trellis task from claiming the same node port, and the process must bind that port itself.

Apply the example and reach the service at the selected node's port 8080. If its health check blocks readiness, `jobs status web-service` includes the relevant allocation diagnostics automatically. Do not add replicas yet; first make the one-allocation service model concrete.

### Optional three-node Vagrant demo

Stages 1 and 2 work on the single node from Getting Started. The repository also includes [`orchestrator/Vagrantfile`](../../orchestrator/Vagrantfile) for the point where the learning path begins to need real multi-node placement. It provisions three Debian 12 VMs named `control`, `worker-1`, and `worker-2`, installs containerd and Trellis on them, joins them into one cluster, and deploys a couple of demo workloads.

The Vagrantfile contains no provider-specific VM configuration and requires no hostmanager plugin. It uses Vagrant's high-level private-network abstraction plus guest mDNS for peer names, so use whichever Vagrant VM provider is available on your host. Some providers still have their own normal setup requirements—for example, Hyper-V asks which virtual switch to use.

Start it from the orchestrator directory:

```sh
cd orchestrator
vagrant up
```

Or select a provider explicitly when your Vagrant installation has more than one:

```sh
vagrant up --provider=virtualbox
# or: --provider=libvirt / --provider=hyperv / another compatible provider
```

This demo is an optional local lab, not a supported production installation method and not a separate Trellis abstraction. You can use any three compatible machines instead. The important property for the next two lessons is simply having enough independently schedulable nodes to make placement and rollout overlap visible.

## 3. Replicas and placement

The `replicated-service` example changes the healthy service from one desired allocation to two. Both replicas reserve port 8080, so they cannot share a node and require at least two compatible nodes. The three-node Vagrant demo above is enough to run this lesson without provisioning separate hosts manually.

This stage is about scheduling rather than rollout policy. Inspect both allocations with:

```sh
trellisctl jobs status replicated-service
trellisctl nodes list
trellisctl nodes status NODE
```

If only one compatible node exists, `jobs status replicated-service` makes the placement failure visible. Understand why the second allocation cannot be placed before moving on to overlapping updates.

## 4. Rolling updates

The `rolling-update` example keeps the same two-replica service and adds:

```yaml
update:
  strategy: rolling
  max_parallel: 1
```

Change only the tutorial image from `v1` to `v2`, run `jobs apply --dry-run`, then apply again. Trellis starts healthy replacement capacity before completing removal of the old revision.

The fixed host port makes the temporary-capacity cost visible: two old replicas already occupy port 8080 on two nodes, so the first replacement needs another compatible node with that port free. `max_parallel: 1` limits how much replacement capacity can be in flight at once. The three-node Vagrant demo has exactly enough nodes to demonstrate this overlap. If placement or health blocks progress, use `jobs status` rather than treating the rollout as an opaque failed command.

## 5. Secrets

Create secret values separately, reference only their names in YAML, and decide whether each application needs an environment or file target. Trellis never reads plaintext values back. A rotated value reaches newly started allocations; it does not mutate a running process.

Follow [`examples/secrets`](../../examples/secrets/) before using secrets in a larger stack. Preserve and back up the node secrets-encryption key separately from Trellis desired-state backups.

## 6. Volumes

Learn the three separate pieces of a volume: `name` is the stable namespace-scoped identity used for locality-aware placement, `host_path` is the node-side backing directory, and `container_path` is where the directory appears in the container.

Start with `host_path: "@/scratch"`. The `@/` prefix resolves below Trellis's volume root for the workload namespace, and Trellis creates the backing directory when the first allocation is realized. Then compare it with an explicit absolute path such as `/srv/trellis/app-data`, which is used verbatim and therefore requires operator preparation and does not receive filesystem-level namespace isolation.

The first allocation using an unseen `(namespace, name)` establishes that volume's owning node. Later allocations using the same identity are scheduled there; node loss does not cause Trellis to silently create a second copy. Multiple allocations that intentionally use one volume name therefore share locality. Replicated stateful members that need independent local disks must use independently named task groups or jobs with distinct volume names, because scaling one task group repeats the same volume identity.

The [`volumes`](../../examples/volumes/) example demonstrates both path forms and uses a node constraint to steer first placement for the explicit absolute path. Complete its directory-ownership, namespace-isolation, backup, and recovery notes before adapting it to real data. `trellisctl nodes status NODE` shows the registered volume identities that affect later placement.

## 7. Sidecars and task groups

A task group is more than YAML nesting: every task in it is placed, scaled, updated, and drained together. The [`sidecar`](../../examples/sidecar/) example uses this coupling intentionally for nginx and its metrics exporter.

If two containers should scale or fail independently, use separate task groups or jobs. If a helper needs to observe many allocations rather than only its colocated application, continue to the API-access/controller stage instead.

## 8. Namespace networking and discovery

Networking is selected per task:

| `networking.mode` | Meaning | When to use it |
|---|---|---|
| omitted / `isolated` | Private container namespace without external routes | Jobs that need no network, or custom runtime setup |
| `host` | Join the node network; may reserve ports used directly by the process | Directly reachable services and simple local communication |
| `namespace` | Join the private Trellis network for the job namespace | Cross-node communication within the workload namespace |

`namespace` is the user-facing semantic mode. Its current implementation uses a WireGuard mesh and therefore requires the corresponding WireGuard node setup, but manifests do not depend on that implementation detail. Adding `runsc` (gVisor) provides additional syscall-level sandboxing and is recommended but not required.

Host port declarations are valid only with `mode: host`:

```yaml
networking:
  mode: host
  ports:
    - port: 8080
```

There is only one port because host networking has no Trellis NAT or translation layer. The reservation prevents another Trellis task from claiming the same node port; the process must bind it itself.

Namespace-networked tasks do not declare host ports. Healthy allocations enter Trellis DNS discovery using names shaped like:

```text
group.job.namespace.trellis
```

Run [`examples/namespace-networking`](../../examples/namespace-networking/) at this stage. Its `web` group publishes two healthy tutorial allocations while an `observer` group repeatedly requests:

```text
http://web.namespace-networking.default.trellis:8080/health
```

That makes both discovery and the private network visible in `trellisctl jobs logs` without introducing an application proxy or special service resource.

Configure the namespace-networking dependencies on every participating node and open the configured WireGuard UDP range between nodes (by default `51820-52075`). Each namespace is assigned one stable port from that range. The installer sets up WireGuard when namespace networking is enabled and optionally installs gVisor/runsc for additional sandboxing. Use `trellisctl jobs status` to see placement and current diagnostics, `jobs logs` to see application-level peer probes, and `jobs status NAME --history` when you need the recorded allocation lifecycle transitions that led to the current state.

Treat discovery as runtime endpoint information, not application consensus. Applications that require a single writer, leader election, or distributed locking still need their own coordination protocol.

## 9. In-cluster API access

API access has two dimensions: **scope** (`namespace` or `cluster`) and **access** (`read` or `write`). Prefer the narrowest pair that works.

A typical observer uses:

```yaml
api_access:
  scope: namespace
  access: read
```

Trellis gives every task in the group a bearer credential restricted to the job's own namespace, plus the API address, job namespace, and cluster CA when configured. The namespace cannot be redirected to another tenant.

Use `namespace/write` only for a namespace-local controller that actually mutates desired state. Use `cluster/read` for a trusted cluster-wide observer. Use `cluster/write` only for an operator workload that needs ordinary cluster mutations.

The administrator signing key is separate and more privileged. It is used for backup/restore, Raft administration, and minting scoped credentials, and Trellis never injects it into workloads. Node registration and heartbeats instead use certificate-bound node identity; managed enrollment has its own credential.

Every task in an API-enabled group can read the injected token, so do not add untrusted sidecars. The [`api-access`](../../examples/api-access/) example intentionally uses `namespace/read` and explains TLS verification, authenticated requests, last-known-good behavior, and token hygiene.

## 10. Release patterns

Trellis directly implements `recreate` and `rolling`. The dedicated [`rolling-update`](../../examples/rolling-update/) lesson covers the built-in rolling primitive before this stage. Blue/green and canary are compositions of separate jobs plus external routing state.

Read [`deployment-strategies`](../../examples/deployment-strategies/) only after completing the rolling lesson. When these patterns use a shared fixed host port, every simultaneously running allocation needs a node where that port is free; Trellis does not hide this capacity requirement behind a port-forwarding layer.

## 11. Stateful and HA patterns

The WordPress example is a development composition, not a production topology. The Patroni example is an architecture skeleton, not a database service. At this stage you should be able to identify which responsibilities Trellis supplies—placement, lifecycle, health observation, secret delivery, network attachment—and which remain application/operator responsibilities.

## Reference and operations

Use the learning path to acquire the model; use these pages afterward:

- [Job manifest reference](job-specification.md) for exact fields and validation.
- [CLI workflows](cli.md) for contexts, planning, diagnostics, lifecycle history, logging, and automation.
- [Operations](operations.md) for node maintenance, backups, TLS, and recovery.
- [Cookbook](cookbook.md) for architecture outcomes and tradeoffs.

[Documentation index](../README.md) · [Previous: Getting Started](getting-started.md) · [Next: User model](user-model.md)
