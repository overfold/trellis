# Multitenancy and trust boundaries

Trellis namespaces separate workload-facing resources, but Trellis does not include an admission-policy framework for deciding which valid manifest capabilities a tenant may use. This page defines that distinction and the trust model for operators building a multitenant service on Trellis.

## The trust model

A namespace is Trellis's tenant, authorization, discovery, and workload-isolation boundary. Jobs, allocations, namespace-scoped credentials, service discovery, namespace networks, secret references, and volume identities are all resolved within that boundary.

That boundary does not make every valid job manifest safe to accept from an untrusted tenant. The job API performs canonical validation and authorization, including preventing a caller from delegating API authority it does not have. It does not apply an operator-defined policy to fields such as networking mode or volume paths.

There are therefore two different access models:

| Access model | Trust assumption |
| --- | --- |
| Direct `trellisctl` or API access that can submit job manifests | The submitter is an operator trusted with every manifest capability their credential allows them to request. Namespace scope limits which Trellis resources they can address; it is not a manifest sandbox. |
| A product frontend for untrusted tenants | The frontend owns the tenant-facing schema and admission rules, emits only an approved subset of the canonical job model, and keeps its Trellis credentials on the trusted backend. Tenants do not receive a direct write credential. |

## What namespaces isolate

Within Trellis, a namespace provides these boundaries:

- **Authorization:** a namespace-scoped credential remains bound to its stored namespace regardless of request headers. It can read or write only the ordinary API resources allowed by its access level in that namespace.
- **Jobs and allocations:** names, desired state, runtime queries, logs, exec targets, and events are selected within the authorized namespace.
- **Discovery and networking:** nodes receive catalog entries only for namespaces with active allocations assigned to them. For `networking.mode: namespace`, the resolver derives the caller namespace from its network source address and returns only matching `group.job.namespace.trellis` records. Each namespace joins its own private bridge and WireGuard path with a durably assigned, non-overlapping subnet on every node. Its forwarding and host-bound isolation run before shared host `FORWARD` and `INPUT` rules, so host firewall accepts cannot bypass them.
- **Volume identity and managed paths:** volume registrations are keyed by `(namespace, name)`. A `host_path` beginning with `@/` resolves below the namespace's Trellis-managed volume root.
- **Secrets:** secret records and job references are namespace-scoped. A job can receive only secrets from its own namespace, and APIs return metadata rather than plaintext after a secret is stored. Secrets are not separately ACLed per job: a manifest submitter is trusted to reference any secret name in that namespace.

These controls are meaningful security boundaries for the resources Trellis owns. They do not imply separate physical nodes, kernels, container runtimes, disks, or control planes.

## Building a tenant-safe frontend

A multitenant frontend should expose its own constrained workload model instead of accepting arbitrary Trellis YAML or canonical JSON and merely replacing the `namespace` field. Validate the resulting canonical model on the trusted backend before calling plan or apply; do not rely only on controls in browser code.

At minimum, enforce the following rules:

- **Set the namespace server-side.** Derive it from the authenticated tenant and reject a tenant-supplied namespace. Keep tenant-to-namespace ownership in the frontend's own durable state; Trellis namespaces are named boundaries, not lifecycle-managed tenant records.
- **Reject absolute `host_path` values.** They mount an operator-prepared node path verbatim and deliberately bypass filesystem-level namespace separation. Permit only `@/` paths, optionally with a frontend-assigned prefix or a smaller storage abstraction.
- **Reject host networking.** `networking.mode: host` joins the node network namespace and exposes the node's network surface. Permit `namespace` where private service connectivity is needed, and `isolated` where no external route is needed.
- **Do not grant cluster API access.** Reject `api_access.scope: cluster`. For the safest general tenant profile, reject `api_access` entirely: a write credential, including `namespace/write`, lets its holder submit manifests directly and bypass the frontend's policy. If a product deliberately offers namespace API access, constrain it to the minimum access level, place it only in reviewed controller task groups, and treat every task in that group as holding the credential. Each API-enabled allocation receives its own credential, bound to its job and task group; deleting the job, or removing or narrowing `api_access`, revokes it.
- **Constrain images and runtime.** Apply the product's registry, digest, provenance, and update rules. Prefer `runtime: runsc` on nodes that support it for additional syscall isolation; it is defense in depth, not a replacement for manifest admission.
- **Enforce resources and scale.** Require CPU and memory values, bound replica counts and aggregate requests, and enforce per-tenant quotas and rate limits in the frontend. Trellis schedules declared resources but does not provide namespace quotas or protect against deliberate under-declaration.
- **Constrain the remaining model.** Allowlist fields rather than trying to denylist future capabilities. Authorize secret references against the tenant's own secret inventory, and bound environment and label data, health-check commands, volume counts and sizes through the storage layer, and any exec or log operations the product exposes.

Run canonical Trellis validation and planning after frontend admission, but before apply. Those steps catch schema and placement errors; they do not replace the frontend's security policy.

### Credentials and secrets

Keep Trellis operator credentials in the frontend backend. Use separate least-privilege credentials for distinct backend responsibilities where practical, and never place a cluster credential in tenant browser code or an untrusted workload.

Namespace-scoped operator credentials are useful for backend job, allocation, and secret operations because the control plane enforces their namespace: every namespaced request names its namespace in the path, and a namespace-scoped credential receives `403` for any namespace other than its own. A frontend that offers tenant secret management can therefore hold one `namespace/write` credential per tenant namespace instead of a cluster credential; `namespace/read` may list and describe secret metadata but not set or delete secrets. It should still accept secret plaintext only over a protected connection, avoid logging it, and return only Trellis's secret metadata after storage. Trellis never returns a stored secret value to any API caller.

A manifest may reference a secret name, and the allocation receives the value from the job's own namespace. Direct manifest access therefore grants the practical ability to consume any known secret in that namespace. A tenant frontend must authorize each reference and should keep different tenants in different namespaces. Prefer file delivery over environment delivery when the application supports it. Tenant separation does not protect a secret from other processes deliberately placed together in the same task group or from a compromised container that receives it.

## Shared surfaces and residual risk

Namespaces share the cluster's nodes, host kernels, container runtime, image cache and pull path, physical network, local disks, DNS forwarders, control plane, and operator-managed secret-encryption key. Consequences include:

- node or control-plane failure can affect several tenants;
- CPU, memory, disk, network, image pulls, and API capacity remain contention and denial-of-service surfaces unless the frontend constrains them. Each task is capped at its declared CPU and memory (memory including swap) and at the node's `task_pids_limit` processes and threads, but those caps bound a single task, not the aggregate a tenant can schedule; on hosts without memory-cgroup swap accounting the swap cap is not enforced;
- `@/` volumes provide path separation, not encryption, distributed storage, snapshots, or protection from node administrators;
- namespace networking separates tenant network paths, but workloads still share the host networking stack, DNS forwarders, and Trellis control-plane route;
- container isolation ultimately depends on the selected OCI runtime and host security. Trellis applies containerd's default seccomp profile (gVisor enforces it for `runsc` workloads only when started with `--oci-seccomp`, relying on its own sandbox otherwise), a default AppArmor profile on AppArmor-enabled hosts for non-`runsc` workloads, removes `CAP_MKNOD` and `CAP_NET_RAW`, and mounts volumes, secret files, and other Trellis-generated bind mounts `nosuid,nodev`, but tasks still run as the image's user (root by default) with containerd's remaining default capabilities on the shared host kernel; and
- cluster operators retain administrative access to the shared infrastructure.

Use separate clusters or dedicated nodes when tenants require stronger infrastructure, compliance, performance, or failure-domain separation than those shared surfaces permit. Trellis node constraints can select operator-labelled nodes, but the frontend must own and enforce any tenant-to-node-pool policy.

## Recommended safe tenant profile

Use this as a conservative starting point for untrusted tenants:

1. Give each tenant a frontend-owned namespace and no direct Trellis write credential.
2. Accept a small product-specific workload schema, then set namespace and defaults on the backend.
3. Permit only `networking.mode: namespace` or `isolated`; reject host networking and host ports.
4. Permit only `@/` volumes through a quota-aware storage interface; reject absolute host paths.
5. Omit `api_access` from tenant workloads.
6. Require reviewed or policy-compliant images, prefer `runsc`, and require bounded CPU, memory, replicas, and request rates.
7. Mediate secrets, logs, metrics, and any exec capability on the backend with tenant authorization on every request.
8. Revalidate the complete canonical job against the allowlist whenever it is created or updated, then use Trellis plan and apply.

This profile is an operator policy implemented outside the Trellis core. A frontend may deliberately broaden it for a trusted tenant or controller, but each exception should be treated as granting the corresponding host or API capability, not as ordinary namespace access.

[Documentation index](../README.md) · [User model](user-model.md) · [Job manifest reference](job-specification.md) · [Operations](operations.md)
