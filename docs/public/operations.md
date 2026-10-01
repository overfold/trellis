# Operations

Operational commands use the vocabulary in the [Trellis user model](user-model.md): apply/delete jobs, inspect allocations, and drain/undrain nodes. Cluster membership, quorum, and leadership are covered separately in [Multi-node clusters](multi-node.md) because they are not part of the normal workload model. The [CLI workflows guide](cli.md) covers contexts, planning, rollout watching, diagnosis, logging, and structured output in detail.

## Routine workflow

```sh
trellisctl context current
trellisctl jobs apply --check --file trellis.yaml
trellisctl jobs apply --dry-run --file trellis.yaml
trellisctl jobs apply --file trellis.yaml --wait
trellisctl jobs status NAME
trellisctl jobs status NAME --history
trellisctl jobs logs NAME --tail 200
trellisctl jobs delete NAME
```

Commands with a coherent structured result expose a local `--output json` flag; streaming and action commands do not. See [CLI workflows](cli.md#structured-output-and-automation) for the exact contract.

## Node configuration

Installer-managed nodes keep their durable daemon configuration at `/etc/trellis/trellis.yaml`. The file is root-readable and contains operator-managed settings such as advertise addresses, labels, secret-encryption key path, and WireGuard transport settings. The first node also contains only the Ed25519 administrator public key used to initialize replicated cluster state; the private key remains operator-side and is never retained by a daemon. Volume placement is not configured here; namespace-scoped volume ownership is established by first placement and stored in the control plane.

A minimal installed node resembles:

```yaml
cluster: default
administrator_public_key: MCowBQYDK2VwAyEA...
node_signing_mode: managed
data_dir: /var/lib/trellis/data
agent_advertise: node-a:8127
server_advertise: node-a:8128
raft_advertise: node-a:8129
job_limits:
  max_replicas_per_task_group: 500
  max_task_groups_per_job: 64
  max_tasks_per_task_group: 32
  max_desired_allocations: 1000
  max_desired_allocations_per_namespace: 10000
  default_task_cpu: 100
  default_task_memory: 128MiB
  max_task_cpu: 1000000
  max_task_memory: 1TiB
```

A node joining a managed-mode cluster also sets `join` and `ca_cert`, plus `join_token` until it has enrolled; see [Multi-node clusters](multi-node.md#add-a-node). The join token is used once, at first start, and the installer removes it afterwards.

`job_limits` is operator-only admission policy. Jobs cannot override it. The
defaults shown above are used when the section is omitted. Every task without a
`resources` block receives the default CPU and memory requests current when the
job is applied; they are stored in the job, so later changes to the defaults
do not affect jobs that are already applied. Explicit zero or negative resource
values are invalid. `job_limits`, `wireguard_pool`, and `wireguard_port_count`
are [cluster settings](#cluster-settings): they only initialize a new cluster,
and afterwards every node uses the replicated values. Editing them here and
restarting has no effect on an existing cluster.

Every task container a node creates is limited to `resources.task_pids_limit`
processes and threads (default `4096`, maximum `4194304`; flag
`--task-pids-limit`), so a fork bomb in one task cannot exhaust the host's PIDs
and take down containerd, the agent, or other workloads:

```yaml
resources:
  task_pids_limit: 8192
```

Nodes require the `pids` cgroup controller, which systemd-based
distributions enable by default. The node warns at startup when it is missing,
and task creation then fails rather than running tasks unbounded.

This is node hardening policy, not part of a job: it is applied when the node
creates a container and is not part of the execution hash, so changing it does
not restart running allocations or their local restarts. The new value applies
to containers created afterward, such as when a job update or reschedule
replaces an allocation. The limit covers everything in the container's cgroup,
including `trellisctl exec` sessions and script health checks, so a task at its
limit also cannot start those. Raise it for workloads that legitimately run
many threads or processes. Keep it consistent across nodes unless you
deliberately want different per-node bounds.

Each node admits at most 256 concurrent UDP DNS queries and 128 active TCP DNS
connections. UDP queries above the limit receive `SERVFAIL`; excess TCP
connections are closed. Admission is released as soon as a query or connection
finishes, and DNS forwarding is canceled during node shutdown. These fixed
limits bound work and open connections when workloads flood the node-local
resolver or an upstream resolver is slow, while preserving the source-network
namespace check for every admitted discovery query.

Each control-plane process also admits at most 256 simultaneous event
streams (`GET /v1/events` and `GET /v1/namespaces/{namespace}/events`). A request above that limit receives `503 Service
Unavailable` with `Retry-After: 1`; clients should reconnect with backoff. A
disconnected or canceled stream releases its slot immediately. The limit is per
process, so clients reconnecting after a leader change are admitted against the
new leader's independent limit.

Edit this file when changing persistent node configuration, then restart the service:

```sh
sudo systemctl restart trellis
```

`trellis --config PATH` loads the same strict YAML format. Explicit daemon flags override values from the file and are useful for one-off runs; the installed systemd unit intentionally contains only `trellis --config /etc/trellis/trellis.yaml` so there is one durable configuration source.

The `cluster` value (or `--cluster`) must be a 1–63 character safe identifier: it must start with an ASCII letter or digit, and the remaining characters may contain only ASCII letters, digits, dots, underscores, or hyphens. Configure the same cluster name on every node.

Run `trellis` in the host mount namespace. The installed unit deliberately omits systemd sandboxing options that give the service a private mount namespace, such as `PrivateMounts=`, `PrivateTmp=`, `ProtectSystem=`, `ProtectHome=`, `ReadOnlyPaths=`, and `InaccessiblePaths=`. Adding them in a drop-in, or running the daemon inside a container or under `unshare -m`, is unsupported: Trellis bind-mounts managed volumes at staging paths that containerd reads whenever it creates a task, and mounts made in a private namespace are invisible to containerd and disappear when the daemon exits, breaking managed-volume allocations on their next start or restart.

Installer-created nodes also keep `/var/lib/trellis/install-state`. It records only lifecycle facts the installer can prove, such as which optional features are enabled and which host packages/repositories Trellis itself introduced. It is not cluster desired state and is not used by the scheduler.

The root-run containerd runtime stores task logs and generated DNS/hosts mount files in `/var/lib/trellis/runtime`, independently of `data_dir` and `TMPDIR`. It creates this directory with mode `0750`. The directory and its ancestors must be root-owned, must not be symlinks, and must not be group- or world-writable; the runtime directory must also deny access to other users. Unsafe existing paths cause allocation creation/start to fail rather than being repaired automatically. Do not remove these files while allocations still use them. Allocation removal cleans up their files.

Logs from allocations already running at the former `$TMPDIR/trellis-logs` location (`/tmp/trellis-logs` by default) remain readable only when that directory is owned by the runtime user, is not group- or world-writable, and denies access to other users. Legacy symlinks and non-regular log files are rejected. New task starts use the protected location; Trellis does not migrate existing mount files.

## Cluster settings

Settings that every leader must apply identically are replicated with the
cluster state instead of living in node configuration, so they never change
when leadership moves. Inspect them with `trellisctl cluster settings`. Job
limits change with `trellisctl cluster set-job-limits` and reconciliation
settings with `trellisctl cluster set-reconciliation`; both require the
administrator key, and only the flags given change. The namespace WireGuard
pool and port count are fixed when the cluster is created. See
[the CLI reference](cli.md#inspect-and-change-cluster-settings).

The reconciliation settings are:

| Setting | Default | Bounds | Meaning |
| --- | --- | --- | --- |
| `allocation_loss_timeout` | `45s` | `30s`–`24h` | How long a node may go without a heartbeat before the leader marks its allocations `lost` and replaces them. |
| `replacement_backoff_base` | `10s` | `1s`–`24h` | Delay before replacing a task group's allocation after its first consecutive failure; each further failure doubles it. |
| `replacement_backoff_max` | `5m` | base–`24h` | Cap on the doubling replacement delay. |
| `replacement_stable_after` | `10m` | `10s`–`24h` | How long a replacement must run without being reported unhealthy before the failure count resets. |
| `terminal_allocation_retention` | `5` | `0`–`100` | Stopped, failed, or lost allocation records kept per task group for diagnosis. |

Lost is terminal, so `allocation_loss_timeout` is the point at which Trellis
gives up on a node's allocations. While a lost allocation record is retained,
its old containers keep running until enough replacements are running when
the node returns, and are then stopped. They are stopped sooner if they block
a replacement. If an older pruned allocation is later reported, Trellis stops
it as an observed orphan. See [lost allocations](user-model.md#lost-allocations).
Raise the timeout when nodes can be briefly unreachable, for example during
reboots or on unreliable networks, and replacing their work would cost more
than waiting:

```sh
trellisctl --administrator-key ./trellis-administrator.pem cluster set-reconciliation --allocation-loss-timeout 2m
```

This matters most for groups bound to one node by a volume, because their
replacement can only run on that node anyway. Lower values replace work
faster after a real failure. The leader still waits 30 seconds after it is
elected before marking anything lost, and a newly elected leader counts a
node's silence from the start of its leadership, so each failover restarts
the timeout for nodes that are already down.

## Add a node

To grow the cluster beyond one node, see [Multi-node clusters](multi-node.md). It covers cluster sizing, networking between nodes, settings that must match, and both the managed and external-signing join workflows.

## Manage operator credentials

The installer creates one normal `cluster/write` credential for the installing user, but operators often need read-only or shorter-lived credentials for another human, an observer, or automation. All operator API credentials are cluster-scoped. `trellisctl credentials create`, `list`, and `revoke` are the explicit administrative workflow for that.

Credential minting requires the **administrator private key** held by the operator. Supply a PKCS#8 PEM file, or place unpadded base64 PKCS#8 DER in `TRELLIS_ADMINISTRATOR_KEY`; Trellis nodes do not store it:

```sh
# Read-only cluster observer
trellisctl --administrator-key ./trellis-administrator.pem credentials create --scope cluster --access read

# Cluster writer expiring after 30 days
trellisctl --administrator-key ./trellis-administrator.pem credentials create \
  --scope cluster \
  --access write \
  --ttl 720h
```

Without `--ttl` a credential does not expire. Once it expires, requests using it are rejected as unauthenticated.

The default output is the newly minted bearer token so it can be handed directly to a password manager or context setup. Use `--output json` when automation needs the response object instead:

```sh
trellisctl --administrator-key ./trellis-administrator.pem credentials create --scope cluster --access read --output json
```

To save a generated credential as an ordinary user context without leaving it in command history:

```sh
TOKEN="$(trellisctl --administrator-key ./trellis-administrator.pem credentials create --scope cluster --access write)"
trellisctl --token "$TOKEN" --namespace staging context save staging --use
unset TOKEN
```

The cluster stores only a hash of each token, so a lost token cannot be recovered, only replaced. To see and withdraw credentials:

```sh
trellisctl --administrator-key ./trellis-administrator.pem credentials list
trellisctl --administrator-key ./trellis-administrator.pem credentials revoke 3f9c2a7d41b0e865
```

`credentials create` retains `--scope cluster` and no longer accepts `--namespace-scope`. `credentials list` shows each operator credential's ID, scope, access, creation time, and expiry, including expired credentials, but never a token; there is no Namespace column. Existing namespace-scoped tokens are rejected rather than promoted. `credentials revoke ID` rejects the credential on its next use. Workload credentials injected through `api_access` are managed with their allocations and are neither listed nor revocable here.

`trellisctl` fetches the signing challenge and signs the request automatically. Join tokens and ordinary `cluster/write` bearer credentials cannot manage credentials, change Raft membership, or perform backup/restore.

## Drain and maintenance

`trellisctl nodes drain NODE` prevents new placement and migrates allocations to other nodes. `NODE` may be the host/address displayed by `nodes list`, a unique UUID prefix, or a complete UUID. Wait until workloads have healthy replacements before maintenance. `trellisctl nodes undrain NODE` re-enables scheduling. `nodes remove NODE` permanently removes a node from the cluster, requires the administrator key, and is different from draining; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster) before removing a member.

## Upgrade a node

The upgrade entrypoint performs the node-maintenance sequence instead of asking the operator to remember it:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/upgrade.sh | sudo bash
```

It downloads and verifies the new release before touching the running daemon, then swaps the binaries, refreshes the installer-owned systemd unit, starts the daemon, and verifies both the service and control-plane API. If the new daemon does not become healthy, the previous binaries and unit are restored.

A service that was already stopped remains stopped. On a multi-node cluster the script also evacuates the node first; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster).

## Agent recovery refused

When the daemon starts, its allocation agent restores the node's allocations from durable records below `data_dir` (`agent/control-epoch` and `agent/allocations/`) and compares them with the containers containerd reports for the cluster. If containerd cannot list containers, the agent starts anyway: it keeps its recorded allocations and their ports, reports them with unknown health, and retries until a listing succeeds. Restore containerd; nothing else is needed.

The agent refuses to start, and the daemon exits, when that state is broken:

- `agent/control-epoch` or an allocation record is unreadable or malformed, or a record's file name does not match its allocation ID;
- the control epoch is missing while allocation records or managed containers exist, or while containerd cannot be listed to confirm an empty first boot;
- a Trellis container of this cluster has no allocation record.

The last check also runs when a listing succeeds after containerd was unavailable at startup; the daemon then exits with the same error, and restarting refuses at startup. The agent does not adopt such a container from its labels: labels carry no control epoch, drain state, or restart budget, so adopting it could keep running or restart a task the control plane has already replaced. While the daemon is down its allocations stop heartbeating and are replaced on other nodes.

The error names the file or container and ends with `see "Agent recovery refused"`. To recover:

1. Read the error and stop the restart loop while you work:

   ```sh
   sudo journalctl -u trellis -n 50
   sudo systemctl stop trellis
   ```

2. **Missing epoch while containerd is unavailable.** Fix containerd (`sudo systemctl status containerd`) and start Trellis again.

3. **Container without a record.** Inspect the named container, then remove it. Do not write an allocation record by hand. If its workload is still desired, the control plane starts it again.

   ```sh
   sudo ctr -n trellis containers info CONTAINER_ID
   sudo ctr -n trellis tasks kill -s SIGKILL CONTAINER_ID
   sudo ctr -n trellis tasks delete CONTAINER_ID
   sudo ctr -n trellis containers rm CONTAINER_ID
   sudo systemctl start trellis
   ```

4. **Damaged agent state**: an unreadable, malformed, or mis-keyed file, or a missing epoch while records or containers exist. Restore the named file from a backup of that node if you have one. Otherwise reset the node's allocation state: remove every Trellis container of this cluster as in step 3 (list them with `sudo ctr -n trellis containers ls 'labels."trellis.cluster"==CLUSTER'`), move the agent's allocation state aside, and start again. With no records and no containers left, the agent starts as an empty node and the control plane reschedules its work.

   ```sh
   cd /var/lib/trellis/data   # data_dir
   sudo mkdir -p agent-broken
   sudo mv agent/allocations agent/control-epoch agent-broken/
   sudo systemctl start trellis
   ```

   Keep `agent/secret-root`: startup uses it to remove secret files that no allocation owns. Staging mounts and network attachments left by the removed containers are cleaned up at startup as well.

## Uninstall a node

The default uninstall is a reversible machine-removal operation:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/uninstall.sh | sudo bash
```

It removes only dependencies/repositories recorded as introduced by Trellis; older installations without ownership records are handled conservatively and shared host packages are left alone. The user's `trellisctl` contexts are also kept because they describe cluster connections, not ownership of this machine. On a live multi-node cluster the script first hands the node's work and membership to the rest of the cluster; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster).

Installing a new cluster on this machine replaces the invoking user's `local` context with a freshly minted operator token and a live CA-file reference to `/run/trellis/ca.crt`; unrelated contexts are preserved. Resuming an existing installation or joining a cluster keeps an existing `local` context. File-backed local trust follows the running daemon, but old bearer tokens and administrator keys do not gain access to a replacement cluster. Embedded remote contexts remain pinned to their saved CA; see [CA sources](cli.md#named-cluster-contexts).

Instead of throwing away the encryption key while retaining encrypted state, normal uninstall archives the complete recoverable set—node data, `/etc/trellis` configuration and the configured secrets key, plus installer state—under a timestamped `/var/lib/trellis/recovery/` directory.

For deliberate permanent destruction, use:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/uninstall.sh | \
  sudo bash -s -- --purge
```

`--purge` deletes active node state and any previous recovery archives. Its confirmation therefore defaults to **no**. Both modes expose `--yes` for controlled non-interactive automation.

## Backups

```sh
trellisctl --administrator-key ./trellis-administrator.pem backup create trellis-backup.json
trellisctl --administrator-key ./trellis-administrator.pem backup restore trellis-backup.json
```

Backups contain the replicated [cluster settings](#cluster-settings), desired jobs and each live job's retained version history (at most the 10 newest versions), encrypted secret records, volume-registration locality metadata, and durable namespace WireGuard port assignments. Jobs are stored in their resolved form, with every default explicit, so a restored job behaves exactly as it did when the backup was taken. They do **not** contain allocations, container images, local volume bytes, deleted-job history, TLS private keys, or the secret encryption key. Restoring the locality metadata deliberately prevents Trellis from silently treating a previously bound volume as new; recovering a volume-backed workload therefore also requires the owning node identity and its data, or an intentional manifest change to a new volume name. Secure and separately back up the 32-byte secrets key referenced by `secrets_key` in the node config; encrypted records are unusable without it.

Restore into a freshly created cluster that has no jobs, secrets, volume or network port registrations, or allocations. Create that cluster with the same `wireguard_pool` and `wireguard_port_count` as the backed-up cluster; the restore is refused otherwise, because namespace subnets and WireGuard ports are derived from them. The restore replaces the new cluster's job limits and reconciliation settings with the backed-up values, and refuses a backup whose jobs those limits would not admit.

Each backup records its `format_version` and the `trellis_version` that created it. Trellis restores only backups in its own format and does not convert older formats. If a restore reports a different format version, restore the backup with a Trellis release that uses that format, such as the release named in the error. Take a fresh backup after upgrading so you always hold one your current release can restore.

## Secrets

```sh
printf %s 'value' | trellisctl --namespace default secrets set db-password --stdin
trellisctl --namespace default secrets describe db-password
trellisctl --namespace default secrets delete db-password
```

Use `--expected-version N` for compare-and-swap (`0` means create only). Values are capped at 65,536 bytes. Secrets are namespace-scoped resources, but cluster-scoped credentials may address any namespace: `write` may set and delete secrets, while `read` may list and describe their metadata. No credential can read a stored value back. Rotation affects newly started allocations, so apply a workload revision or replace the consuming allocations afterward.

Allocation secret files are held on a verified tmpfs rather than a durable node filesystem. Linux can swap tmpfs pages, so disable swap or configure encrypted swap when secrets must also be protected from offline swap inspection. Environment delivery does not persist plaintext in containerd's OCI metadata, but the running application necessarily receives the value in its process environment; use file delivery when the application supports it. Trellis sets mounted secret ownership to the numeric UID/GID resolved from the image configuration, preserving owner-only access for non-root images.

## Observability

The control plane exposes Prometheus metrics at `/metrics` on its API port. Metrics name namespaces and jobs across the cluster, so a scrape needs a cluster-scoped credential; `read` access is enough. Mint one for Prometheus and scrape any control-plane node, which forwards the request to the leader:

```sh
trellisctl credentials create --scope cluster --access read
```

```yaml
scrape_configs:
  - job_name: trellis
    scheme: https
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/trellis-token
    tls_config:
      ca_file: /etc/prometheus/trellis-ca.pem
    static_configs:
      - targets: ["control.example:8128"]
```

A scrape without a credential receives `401`. `GET /v1/auth/whoami` reports the kind, scope, and access of the bearer credential making the request; it has no top-level namespace. Job status and allocation events explain lifecycle transitions; logs proxy per-task allocation logs. Monitor leader availability, unhealthy/draining nodes, desired-versus-running/healthy counts, reconciliation latency, retries, task groups in replacement backoff (`trellis_replacement_backoff_failures`), and disk capacity for Raft, containerd, and volumes.

For normal workload diagnosis, start and usually finish with `jobs status`. `ready`, `converging`, and `degraded` summarize desired-versus-observed state without collapsing allocation lifecycle and health, and non-ready status output includes the allocations that need attention with reason/message, retry timing, and attempt count. Use `jobs status NAME --history` when you need the recorded lifecycle transitions, and `jobs logs NAME` for task output.

## Networking and TLS

Every node runs namespace networking, the default task network, and requires WireGuard tools, iproute2, and iptables; the installer sets them up and the daemon refuses to start without them. Trellis enables IPv4 forwarding and keeps its rules in its own iptables chains, jumped to first from `FORWARD`, `INPUT`, and the nat table's `PREROUTING`, `OUTPUT`, and `POSTROUTING`. Namespace-networked tasks reach beyond their namespace network through the node, masqueraded to its address, so they can reach whatever the node can, including its private network and cloud metadata endpoints; block destinations tasks must not reach with firewalling outside Trellis. Published task ports are forwarded to their task before your host `FORWARD` rules run, so a host firewall does not restrict who can reach them; restrict access to published ports at the network edge instead. See [Networking and ports](job-specification.md#networking-and-ports) and [Multitenancy](multitenancy.md#networking).

Workloads use Trellis's node-local DNS resolver on the reserved internal address `198.18.0.53:53`; it is not intended to be exposed on external interfaces. Node and Raft transports require mutually authenticated TLS. Possession of the CA key is not API or leader authorization: requests still need the durably bound certificate for one immutable node ID, and leader work is executed only by the current Raft leader with control-epoch, generation, revision, and execution-hash fencing where applicable. Followers preserve that certificate identity by redirecting node-authenticated control-plane requests, and Raft streams verify the peer's joined advertised address. Inbound Raft streams additionally require the peer's certificate to be the one bound to a current Raft member's UUID, so a certificate that merely chains to the cluster CA cannot replicate or vote. Removing a node tombstones its UUID, which every node-authenticated path rejects. Administrator credentials and join tokens are separate from node identity; managed enrollment receives the CA key only after certificate-bound Raft admission, and in managed mode every member therefore holds it (see [Multi-node clusters](multi-node.md#managed-signing-default)).

Ports and WireGuard settings needed between nodes are described in [Multi-node clusters](multi-node.md#prepare-the-network-and-configuration).

[Documentation index](../README.md) · [Previous: CLI workflows](cli.md) · [Next: Multi-node clusters](multi-node.md)
