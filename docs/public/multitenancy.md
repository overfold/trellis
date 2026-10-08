# Multitenancy and trust boundaries

Trellis namespaces separate workload-facing resources, but Trellis does not include an admission-policy framework for deciding which valid manifest capabilities a tenant may use. This page defines that distinction and the trust model for operators building a multitenant service on Trellis.

## The trust model

A namespace separates Trellis resources, service discovery, namespace networks, secret references, and volume identities. It is not an API authorization boundary: operator and workload API credentials are cluster-scoped and may address every namespace.

That boundary does not make every valid job manifest safe to accept from an untrusted tenant. The job API performs canonical validation and authorization, including preventing a caller from delegating API authority it does not have. It does not apply an operator-defined policy to fields such as networking mode or volume paths.

There are therefore two different access models:

| Access model | Trust assumption |
| --- | --- |
| Direct `trellisctl` or API access that can submit job manifests | The submitter is an operator trusted with cluster-wide API access and every manifest capability their access level allows them to request. |
| A product frontend for untrusted tenants | The trusted backend owns tenant authorization, the tenant-facing schema, and admission rules, emits only an approved subset of the canonical job model, and keeps cluster credentials out of browsers and tenant workloads. |

## What namespaces isolate

Within Trellis, a namespace provides these boundaries:

- **Jobs and allocations:** names, desired state, runtime queries, logs, exec targets, and events are selected by namespace paths. Cluster credentials can select any namespace.
- **Discovery and networking:** nodes receive catalog entries only for namespaces with active allocations assigned to them. For `networking.mode: namespace`, the resolver derives the caller namespace from its network source address and returns only matching `group.job.namespace.trellis` records. Each namespace joins its own private bridge and WireGuard path with a durably assigned, non-overlapping subnet on every node. Its forwarding and host-bound isolation run before shared host `FORWARD` and `INPUT` rules, so host firewall accepts cannot bypass them.
- **Volume identity and managed paths:** volume registrations are keyed by `(namespace, name)`. A `host_path` beginning with `@/` resolves below the namespace's Trellis-managed volume root.
- **Secrets:** secret records and job references are namespace-scoped. A job can receive only secrets from its own namespace, and APIs return metadata rather than plaintext after a secret is stored. Secrets are not separately ACLed per job: a manifest submitter is trusted to reference any secret name in that namespace.

These controls are meaningful security boundaries for the resources Trellis owns. They do not imply separate physical nodes, kernels, container runtimes, disks, or control planes.

## Networking

The three networking modes grant very different reach:

- **`none`** has loopback only. It cannot reach or be reached by anything.
- **`namespace`** is separated from other namespaces: the node's Trellis firewall rules drop forwarding from a namespace bridge to any other namespace's bridge or WireGuard interface, admit into the bridge only the namespace's own WireGuard peers, replies, and published ports, and limit traffic from the bridge to the node itself to workload DNS, the API proxy, and replies. Everything else leaving the namespace network is masqueraded to the node's address, so a namespace task can reach whatever the node can: the internet, the node's local network, other nodes' addresses and services, and link-local endpoints such as cloud instance metadata. Published ports accept connections from anywhere that can reach the node, including other namespaces' tasks through a node address.
- **`host`** is node-level network access. The task runs in the node's network namespace, where every namespace bridge and WireGuard interface lives and every namespace route is installed. Trellis's isolation rules filter traffic forwarded through the node and traffic arriving from a namespace bridge or decrypted WireGuard ingress, not connections the node itself opens, so a host-networked task can connect to allocations of **every** namespace present on the node, including through WireGuard to those namespaces' allocations on other nodes. It can also bind any free node port, reach every service listening on the node, and use the node's source address toward the node's network. Trellis removes `CAP_NET_RAW` from every task, so it cannot sniff or inject raw packets on those interfaces, but ordinary connections are not restricted.

Namespace links are IPv4-only: their host-side IPv6 stack is disabled so automatic link-local addresses cannot bypass host-input isolation. Remote WireGuard peers may reply to connections opened by the node but cannot initiate host-service connections through the tunnel. This does not restrict host-networked tasks or revoke the intentional cross-namespace published-port reachability above.

Host networking is available to any credential that can write jobs; Trellis applies no admission policy to it. Treat it as granting access to every namespace network on the nodes where it can run.


## Building a tenant-safe frontend

A multitenant frontend should expose its own constrained workload model instead of accepting arbitrary Trellis YAML or canonical JSON and merely replacing the `namespace` field. Validate the resulting canonical model on the trusted backend before calling plan or apply; do not rely only on controls in browser code.

At minimum, enforce the following rules:

- **Set the namespace server-side.** Derive it from the authenticated tenant and reject a tenant-supplied namespace. Keep tenant-to-namespace ownership in the frontend's own durable state; Trellis namespaces are named boundaries, not lifecycle-managed tenant records.
- **Reject absolute `host_path` values.** They mount an operator-prepared node directory and deliberately bypass filesystem-level namespace separation. Symlink rejection and pinning the resolved directory prevent path redirection, not access to an operator-selected host directory. Permit only `@/` paths, optionally with a frontend-assigned prefix or a smaller storage abstraction.
- **Reject host networking.** `networking.mode: host` joins the node network namespace and exposes the node's network surface, including every other tenant's namespace network on that node; see [Networking](#networking). Permit `namespace` where connectivity is needed, and `none` where no network is needed. Constrain published node ports (`host_port`) to a range or assignment the frontend owns, since they are a shared node resource.
- **Decide on egress.** Namespace-networked tasks can reach anything the node can, including cloud instance metadata endpoints and the node's private network. Block what tenants must not reach with host or network firewalling outside Trellis, or select `none` for workloads that need no network.
- **Do not grant workload API access.** Reject `api_access` for untrusted tenants because every injected credential has cluster scope. A write credential lets its holder submit manifests directly and bypass the frontend's policy; even read access exposes resources across tenants. If a product deliberately runs an API-enabled controller, use only reviewed images in a trusted task group and treat every task in that group as holding cluster authority. Each API-enabled allocation receives its own credential, bound to its job and task group; deleting the job or removing or narrowing `api_access` revokes it.
- **Constrain images and runtime.** Apply the product's registry, digest, provenance, and update rules. Prefer `runtime: runsc` on nodes that support it for additional syscall isolation; it is defense in depth, not a replacement for manifest admission.
- **Enforce resources and scale.** Require CPU and memory values, bound replica counts and aggregate requests, and enforce per-tenant quotas and rate limits in the frontend. Trellis schedules declared resources but does not provide namespace quotas or protect against deliberate under-declaration.
- **Constrain the remaining model.** Allowlist fields rather than trying to denylist future capabilities. Authorize secret references against the tenant's own secret inventory, and bound environment and label data, health-check commands, volume counts and sizes through the storage layer, and any exec or log operations the product exposes.

Run canonical Trellis validation and planning after frontend admission, but before apply. Those steps catch schema and placement errors; they do not replace the frontend's security policy.

### Credentials and secrets

Keep Trellis operator credentials in a trusted frontend backend. Use separate read and write credentials for distinct backend responsibilities where practical, and never place one in tenant browser code or an untrusted workload.

Workload history does not retain authority. Terminal allocations and allocations on removed nodes lose API access independently of log/history retention. A partitioned workload is subject to the leader's bounded silence window, not immediate container termination or a permanent wall-clock credential lease across leadership changes; see the [workload credential lifecycle](job-specification.md#api-access). Revocation cannot erase a workload's copied secrets or cluster-wide data it already read. In-flight admitted requests can complete.

All operator credentials are cluster-scoped. A frontend must authenticate the tenant, derive the allowed namespace from its own durable ownership data, reject tenant-supplied namespace selection, and enforce that decision before every Trellis request. Browser controls are not sufficient. A frontend that offers tenant secret management should accept plaintext only over a protected connection, avoid logging it, and return only Trellis's secret metadata after storage. Trellis never returns a stored secret value to any API caller, but its cluster credential does not prevent the backend from addressing another tenant's namespace.

A manifest may reference a secret name, and the allocation receives the value from the job's own namespace. Direct manifest access therefore grants the practical ability to consume any known secret in that namespace. A tenant frontend must authorize each reference and should keep different tenants in different namespaces. Prefer file delivery over environment delivery when the application supports it. Tenant separation does not protect a secret from other processes deliberately placed together in the same task group or from a compromised container that receives it.

## Shared surfaces and residual risk

Worker-only nodes (`control_plane: false`) hold no Raft state, cluster secrets-encryption key, or CA private key, and relay API TLS without decrypting it. A compromised worker can expose secrets and workload API tokens delivered to its allocations, disrupt traffic, and attack shared infrastructure, but does not thereby acquire every namespace's secrets or authority to impersonate the API. A compromised control-plane node remains a full cluster compromise. Use `runs_workloads: false` to keep workloads off control-plane nodes when stronger separation is needed.

Namespaces share the cluster's nodes, host kernels, container runtime, image cache and pull path, physical network, local disks, DNS forwarders, control plane, and operator-managed secret-encryption key. Consequences include:

- node or control-plane failure can affect several tenants;
- CPU, memory, disk, network, image pulls, and API capacity remain contention and denial-of-service surfaces unless the frontend constrains them. Each task is capped at its declared CPU and memory (memory including swap) and at the node's `task_pids_limit` processes and threads, but those caps bound a single task, not the aggregate a tenant can schedule; on hosts without memory-cgroup swap accounting the swap cap is not enforced;
- `@/` volumes provide path separation, not encryption, distributed storage, snapshots, or protection from node administrators;
- namespace networking separates tenant network paths, but workloads still share the host networking stack, node ports, egress address, DNS forwarders, and Trellis control-plane route;
- container isolation ultimately depends on the selected OCI runtime and host security. Trellis hardens container capabilities, syscall access, and generated mounts, but tasks still run as the image's user (root by default). `runc` shares the host kernel; `runsc` adds a syscall sandbox with different confinement behavior. Neither replaces admission policy or host hardening. Exact seccomp, AppArmor, capability, and mount settings are documented in [runtime internals](../developer/node-internals.md#runtime-abstraction); and
- cluster operators retain administrative access to the shared infrastructure.

Use separate clusters or dedicated nodes when tenants require stronger infrastructure, compliance, performance, or failure-domain separation than those shared surfaces permit. Trellis node constraints can select operator-labelled nodes, but the frontend must own and enforce any tenant-to-node-pool policy.

## Recommended safe tenant profile

Use this as a conservative starting point for untrusted tenants:

1. Give each tenant a frontend-owned namespace and no direct Trellis write credential.
2. Accept a small product-specific workload schema, then set namespace and defaults on the backend.
3. Permit only `networking.mode: namespace` or `none`; reject host networking, and assign any published `host_port` on the backend.
4. Permit only `@/` volumes through a quota-aware storage interface; reject absolute host paths.
5. Omit `api_access` from tenant workloads.
6. Require reviewed or policy-compliant images, prefer `runsc`, and require bounded CPU, memory, replicas, and request rates.
7. Mediate secrets, logs, metrics, and any exec capability on the backend with tenant authorization on every request.
8. Revalidate the complete canonical job against the allowlist whenever it is created or updated, then use Trellis plan and apply.

This profile is an operator policy implemented outside the Trellis core. A frontend may deliberately broaden it for a trusted tenant or controller, but API access always grants cluster-wide read or write capability, not namespace access.

[Documentation index](../README.md) · [User model](user-model.md) · [Job manifest reference](job-specification.md) · [Operations](operations.md)
