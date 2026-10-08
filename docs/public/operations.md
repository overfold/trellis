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

New installer config and secrets-key files are staged with mode 0600 from creation and atomically renamed into place, including custom paths in traversable directories. New files belong to the installing user (root); replacing a key preserves its existing owner/group. Resuming an existing config keeps its contents and ownership and tightens config/key modes to 0600; upgrading does the same without regenerating keys.

`job_limits` is operator-only admission policy. Jobs cannot override it. The
defaults shown above are used when the section is omitted. Every task without a
`resources` block receives the default CPU and memory requests current when the
job is applied; they are stored in the job, so later changes to the defaults
do not affect jobs that are already applied. Explicit zero or negative resource
values are invalid. `job_limits`, `wireguard_pool`, and `wireguard_port_count`
are [cluster settings](#cluster-settings): they only initialize a new cluster,
and afterwards every node uses the replicated values. Editing them here and
restarting has no effect on an existing cluster.

### Host resource reserve

Scheduling uses the node's **allocatable** CPU and memory, not its whole-host
capacity or current utilization. Trellis automatically reserves 5% for the host:
CPU is bounded to 100–1000 millicores and memory to 256 MiB–2 GiB, each capped
at half the host's capacity on small machines. Override either dimension in the
node configuration when the OS, containerd, or other host services need more:

```yaml
resources:
  reserved:
    cpu: 500
    memory: 512MiB
```

CPU is millicores and memory accepts the same byte-size notation as job YAML.
Omitting a dimension keeps its automatic reserve; explicit zero disables that
reserve. Negative values or a reserve greater than host capacity are rejected.
This is per-node policy, not replicated job limits, and is applied after a
daemon restart. Inspect capacity, allocatable resources, and live usage with
`trellisctl nodes status NODE`; a low utilization reading alone does not make
room for a job whose declared requests exceed allocatable capacity. A reserve
is scheduler accounting, not a dedicated cgroup protecting host services.

### Task process limit

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

### Task log limit

Each task log on a node keeps about `resources.task_log_limit` of its newest
output (default `64MiB`, minimum `1MiB`; flag `--task-log-limit`), so one chatty
task cannot fill the node's disk:

```yaml
resources:
  task_log_limit: 256MiB
```

The value accepts the same byte-size notation as job YAML. The agent checks
the logs in `/var/lib/trellis/runtime` every second, for running tasks and for
terminal allocations whose logs are retained. When a task's active log grows
past half the limit, the agent copies its newest half into `<container>.log.1`,
replacing the previous copy, and truncates the active log in place; the task
keeps writing without interruption. Retained output therefore varies from half
the limit to the limit. `--tail` reads across both files, and `--follow`
continues across a rotation. The limit is approximate:

- a task can exceed it by what it writes in about one second before the next
  check, and by more while the agent is stopped, since output keeps flowing
  through the containerd shim; the limit is enforced again when the agent
  restarts;
- output the task writes while a rotation finishes copying is lost, and very
  fast output can make a rotation cut a line;
- a follower that falls more than one rotation behind skips the output
  rotation discarded in between;
- when the filesystem is already full and the newest output cannot be copied
  aside, the agent discards that log's retained output to free space and logs
  a warning.

This is node policy, not part of a job. It applies to existing logs as well as
new ones, including tasks that were already running when the node gained the
limit. Logs of allocations still read from the legacy `$TMPDIR/trellis-logs`
location are not limited. Size the limit and the number of retained terminal
allocations (`terminal_allocation_retention`) together: a node can hold up to
the limit for every running task and every retained task log on it.

### Fixed service limits

These limits are not configurable cluster settings. Reduce concurrent clients
or reconnect with backoff when they are reached:

| Service | Limit | Overload behavior |
|---|---|---|
| Node DNS | 256 concurrent UDP queries; 128 active TCP connections per node | UDP receives `SERVFAIL`; excess TCP connections close |
| API events | 256 event streams per control-plane process | `503 Service Unavailable`, `Retry-After: 1` |
| Exec | 256 relays per leader; 64 sessions per node; 8 per allocation | `429 Too Many Requests` before opening a stream |

Exec closes after 30 minutes without input, output, or resize, or after eight
hours even while active. A client that stops consuming output can also be
disconnected after 30 minutes. Leadership changes end existing exec sessions;
run the command again after a leader is available. See [CLI exec](cli.md#run-commands-and-open-an-allocation-terminal)
and the [API stream contract](api.md#exec-streams).

Internal discovery publishes IPv4 A records. Queries for AAAA or other record
types on an existing name return `NOERROR` with no answers, so dual-stack
lookups can use the A record. Missing or namespace-inaccessible names return
`NXDOMAIN`.

### Apply node configuration

Edit this file when changing persistent node configuration, then restart the service:

```sh
sudo systemctl restart trellis
```

`trellis --config PATH` loads the same strict YAML format. Explicit daemon flags override values from the file and are useful for one-off runs; the installed systemd unit intentionally contains only `trellis --config /etc/trellis/trellis.yaml` so there is one durable configuration source.

The `cluster` value (or `--cluster`) must be a 1–63 character safe identifier: it must start with an ASCII letter or digit, and the remaining characters may contain only ASCII letters, digits, dots, underscores, or hyphens. Configure the same cluster name on every node.

Run `trellis` in the host mount namespace. The installed unit deliberately omits systemd sandboxing options that give the service a private mount namespace, such as `PrivateMounts=`, `PrivateTmp=`, `ProtectSystem=`, `ProtectHome=`, `ReadOnlyPaths=`, and `InaccessiblePaths=`. Adding them in a drop-in, or running the daemon inside a container or under `unshare -m`, is unsupported: Trellis bind-mounts managed volumes at staging paths that containerd reads whenever it creates a task, and mounts made in a private namespace are invisible to containerd and disappear when the daemon exits, breaking managed-volume allocations on their next start or restart.

Installer-created nodes also keep `/var/lib/trellis/install-state`. It records only lifecycle facts the installer can prove, such as which optional features are enabled and which host packages/repositories Trellis itself introduced. It is not cluster desired state and is not used by the scheduler.

The root-run containerd runtime stores task logs and generated DNS/hosts mount files in `/var/lib/trellis/runtime`, independently of `data_dir` and `TMPDIR`. It creates this directory with mode `0750`. The directory and its ancestors must be root-owned, must not be symlinks, and must not be group- or world-writable; the runtime directory must also deny access to other users. Unsafe existing paths cause allocation creation/start to fail rather than being repaired automatically. Do not remove these files while allocations still use them. Container cleanup removes generated mount files but preserves task logs while the control plane retains the terminal allocation record. Minimal node-local log lookup metadata survives agent restarts; logs remain readable through the ordinary allocation logs API after the container is gone.

`terminal_allocation_retention` governs terminal history and its retained task logs. Once the control plane prunes an allocation, the leader asks its node to delete the logs. The agent reports retained log inventory in every heartbeat, so deletion is retried and catches up when an unavailable node returns. Logs are node-local, not replicated: they are unavailable while the node is unreachable and cannot be recovered if its disk is lost. Each task log is bounded by the node's [task log limit](#task-log-limit), but many running tasks and retained logs together can still fill a small disk. Each node reports its task log usage in heartbeats: `/metrics` exposes `trellis_node_task_log_bytes`, `trellis_node_task_log_filesystem_available_bytes`, and `trellis_node_task_log_filesystem_capacity_bytes` per `node_id`, and `GET /v1/nodes` returns the same values. Alert on the available bytes before the filesystem fills. The values come from the node's latest heartbeat to the current leader, so pair the alert with `trellis_node_heartbeat_age_seconds`; the series are absent when the agent cannot measure its log directory. Health-check error messages include namespace, job, allocation, task, and container identifiers to distinguish failures in colocated workloads.

The runtime's retained-log lookup and cleanup mechanics, including older temporary-directory paths, are described in [runtime internals](../developer/node-internals.md#runtime-abstraction).

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

It downloads and verifies the new release before touching the running daemon, then swaps the binaries, refreshes the installer-owned systemd unit, and starts the daemon. Verification checks that systemd reports the service active and the local API can serve authentication requests; it does not verify worker registration or workload readiness. If this check fails, the previous binaries and unit are restored. Afterward, inspect `trellisctl nodes status NODE` and affected jobs for workload health.

A service that was already stopped remains stopped. On a multi-node cluster the script also evacuates the node first; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster).

Upgrade and graceful uninstall require `jq` for structural parsing of the CLI's node-list JSON (on Debian/Ubuntu, `sudo apt-get install jq`). Membership must be a single nonempty JSON array of node objects with nonempty IDs; malformed, empty, or unexpected output stops maintenance before draining, replacing binaries, or deleting local state. Labels and pretty/compact formatting do not affect the count. Only a validated one-node list selects the single-node flow. Uninstall's explicit `--force` bypasses cluster inspection, not local resource cleanup.

Node checks, drain, and undrain use the invoking user's saved `local` context from `~/.config/trellis/config.yaml` (the user identified by `SUDO_USER` when run through `sudo`, or root's home when run directly as root). This context needs a valid cluster/write operator credential. Maintenance connects to the node's local API and uses `/run/trellis/ca.crt`, regardless of the currently selected context. On a worker this local API is relayed to the control plane; the upgrade does not require local Raft state or keys. A missing context or rejected credential stops the upgrade before binaries are changed and reports the underlying CLI error.

On a joining node without a saved `local` context, configure one with a cluster/write credential first. For a configuration stored elsewhere, download the script and pass its path explicitly:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/upgrade.sh -o /tmp/trellis-upgrade.sh
sudo env TRELLIS_CONFIG="$HOME/.config/trellis/config.yaml" bash /tmp/trellis-upgrade.sh
```

### Discovery identifier upgrades

Older versions accepted dots in namespace, job, and task-group names even though their discovery names did not resolve. These names are now rejected; there is no automatic dot-to-hyphen conversion, escaping, or identity-boundary reinterpretation. Task, secret, and volume names are unchanged.

Before upgrading, save a [desired-state backup](#backups) and a consistent recovery copy of stopped nodes' data directories, configuration, and separately protected secrets keys. Inspect manifests, live jobs, and retained job versions for dotted namespace/job/group names, and inspect namespace-bearing secret, volume, and network registrations in the backup. Merely applying a renamed group is insufficient: its old retained versions still contain the dotted identity.

While still running the old version, create replacement jobs with explicitly chosen single-component names, update application DNS references, and delete the old jobs (which also removes their version history). Verify workload and attachment cleanup, including unreachable/lost allocations, before proceeding. A namespace rename is resource recreation, not a metadata edit: recreate secrets from their original secure source and plan volume-data transfer/locality explicitly. Do not rewrite encrypted secret namespaces or volume registrations in a backup; their identity and node-side paths matter.

If residual dotted namespaces remain in registrations, or safe cleanup cannot be confirmed, migrate workloads and their data to a fresh cluster with valid names rather than editing Bolt/Raft state. Keep the old cluster and recovery copies until the replacement is verified. Do not restore a legacy dotted backup into the new version: restore rejects it atomically, including dotted historical jobs and namespace-bearing registrations.

Loading an invalid persisted job or reading an invalid retained revision returns a repair diagnostic; control-plane startup/leadership reload refuses invalid live jobs rather than scheduling or renaming them. Legacy allocation discovery entries are excluded from the catalog and DNS cache. Already-running containers are not automatically migrated or stopped by this validation change. If an upgrade was attempted prematurely, stop the upgraded nodes and return to the prior binaries with intact state (or the saved recovery copy) to perform cleanup; do not expect the new API to delete invalid dotted paths.

## Leadership activation failures

Leadership activation failures (Raft barrier, state reload, or epoch acquisition) shut the node down fail-closed with a nonzero exit status. The first-party `Restart=on-failure` systemd unit restarts it; the daemon does not retry activation in place. Inspect `journalctl -u trellis` if faults persist. Requested SIGTERM/SIGINT shutdown remains successful.

## Agent recovery refused

When the daemon starts, its allocation agent recovers from records below `data_dir` (`agent/control-epoch` and `agent/allocations/`). With intact records, an unavailable containerd listing is retried while health is unknown; restore containerd rather than deleting agent state.

The agent refuses to start, and the daemon exits, when that state is broken:

- `agent/control-epoch` or an allocation record is unreadable or malformed, or a record's file name does not match its allocation ID;
- the control epoch is missing while allocation records or managed containers exist, or while containerd cannot be listed to confirm an empty first boot;
- a Trellis container of this cluster has no allocation record.

The container-without-record check also runs when containerd becomes available after startup. While the daemon is down its allocations stop heartbeating and can be replaced on other nodes. Do not fabricate records or delete fencing state to bypass the refusal; [agent convergence](../developer/node-internals.md#agent-convergence) explains the recovery invariants.

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

It removes only dependencies/repositories recorded as introduced by Trellis; older installations without ownership records are handled conservatively and shared host packages are left alone. The user's `trellisctl` contexts are also kept because they describe cluster connections, not ownership of this machine. On a live cluster with other registered nodes, the script first drains the node and removes its registration; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster). This path applies to workers as well as control-plane members: workers have no Raft membership to remove, and `nodes remove` records their tombstone so the old identity cannot register again.

Graceful inspection and drain use the invoking user's saved `local` cluster/write context (selected using `SUDO_USER`, or an explicit `TRELLIS_CONFIG`), pinned to the local API and `/run/trellis/ca.crt`; ambient `TRELLIS_TOKEN` does not override it. Joining nodes need an operator-provisioned local context. Multi-node leadership transfer and removal additionally require explicit, transient `TRELLIS_ADMINISTRATOR_KEY`, as a private-key file path or base64 PKCS#8 key. For example, download the script, inspect it, and run `sudo env TRELLIS_CONFIG=/home/operator/.config/trellis/config.yaml TRELLIS_ADMINISTRATOR_KEY=/secure/trellis-administrator.pem bash uninstall.sh`. The script does not copy the administrator key into daemon state. Missing authority aborts before drain; rejected authority or unavailable quorum prevents deletion and leaves the node drained, so check its state and undrain it if abandoning removal. Single-node uninstall does not require administrator authority.

Installing a new cluster on this machine replaces the invoking user's `local` context with a freshly minted operator token and a live CA-file reference to `/run/trellis/ca.crt`; unrelated contexts are preserved. Resuming an existing installation or joining a cluster keeps an existing `local` context. File-backed local trust follows the running daemon, but old bearer tokens and administrator keys do not gain access to a replacement cluster. Embedded remote contexts remain pinned to their saved CA; see [CA sources](cli.md#named-cluster-contexts).

Instead of throwing away the encryption key while retaining encrypted state, normal uninstall archives the complete recoverable set—node data, `/etc/trellis` configuration and the configured secrets key, plus installer state—under a timestamped `/var/lib/trellis/recovery/` directory.

After stopping the daemon, uninstall force-deletes containerd tasks, waiting for their processes to exit before removing containers. Failures show the underlying containerd error and identify the task or container that could not be removed; cleanup stops before changing network resources or node data.

Once all containers are removed, uninstall uses the installed Trellis binary to remove every local network resource recorded in its attachment journals and detach leftover volume-staging bind mounts. This applies equally to single-node, `--force`, and `--purge` removal. Staging cleanup does not delete backing volume contents and runs before either archiving or purging data; uninstall never guesses at or deletes unjournaled host interfaces or firewall rules.

The service, binaries, and dependencies remain installed until data handling succeeds. If local resource cleanup or data handling fails, fix the reported error and rerun the same uninstall command. A failed purge may already have deleted some data; retryability does not make purge reversible.

If a broken installation cannot inspect or update cluster membership, use `--force` to skip cluster operations and remove the local installation:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/uninstall.sh | \
  sudo bash -s -- --force
```

This stops local workloads without draining or waiting for healthy replacements, and leaves cluster membership unchanged. Removing a voting node can affect quorum. If other members remain, remove the node from a healthy operator context afterward; the script prints its node ID when available. `--force` still archives recoverable state and does not skip confirmation. Combine it with `--purge` only when permanent data deletion is intended.

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

Restore into a freshly created cluster that has no jobs, job revisions, secrets, volume or network port/subnet registrations, allocations, or replacement backoffs. Backoffs are runtime scheduling state and are not backed up or carried into a restore. Deleting all jobs does not immediately make a used cluster fresh: retained backoffs must also have been cleaned up. A rejected restore leaves existing state unchanged. Create that cluster with the same `wireguard_pool` and `wireguard_port_count` as the backed-up cluster; the restore is refused otherwise, because namespace subnets and WireGuard ports are derived from them. The restore replaces the new cluster's job limits and reconciliation settings with the backed-up values, and refuses a backup whose jobs those limits would not admit.

Before restoring a backup containing secrets, configure the original 32-byte `secrets_key` and the original `secrets_key_id` if explicitly set. Every potential leader must use the same key and ID. Restore authenticates every encrypted secret with the receiving leader's configured secrets store before committing state: an absent store, unavailable key ID, wrong key bytes (even with the same ID), or damaged ciphertext refuses the restore with an actionable error. Correct the key configuration before retrying; Trellis does not include keys in backups, rekey restored records, or search a keyring. A backup without secrets does not require a secrets store.

A nonfresh target or unusable secret returns HTTP `409` without installing the backup. Actual state-read, Raft barrier/commit, or reload failures return `503`; a commit or reload failure can occur after state was committed, so inspect the target before retrying rather than assuming it is still fresh. Successful restore validates secret decryption, not recovery of external images, volume bytes, or all workload dependencies.

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
trellisctl --administrator-key ./trellis-administrator.pem \
  credentials create --scope cluster --access read
```

Credential creation requires the administrator key (alternatively set `TRELLIS_ADMINISTRATOR_KEY`). Save the returned token securely in the scrape's `credentials_file`; it is shown only once.

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

A scrape without a credential receives `401`. `GET /v1/auth/whoami` reports the kind, scope, and access of the bearer credential making the request; it has no top-level namespace. Job status and allocation events explain lifecycle transitions; logs proxy per-task allocation logs. Monitor leader availability, unhealthy/draining nodes, desired-versus-running/healthy counts, reconciliation latency, retries, task groups in replacement backoff (`trellis_replacement_backoff_failures`), free space on the task log filesystem (`trellis_node_task_log_filesystem_available_bytes`), and disk capacity for Raft, containerd, and volumes.

For normal workload diagnosis, start and usually finish with `jobs status`. `ready`, `converging`, and `degraded` summarize desired-versus-observed state without collapsing allocation lifecycle and health, and non-ready status output includes the allocations that need attention with reason/message, retry timing, and attempt count. Use `jobs status NAME --history` when you need the recorded lifecycle transitions, and `jobs logs NAME` for task output.

## Networking and TLS

Every node runs namespace networking, the default task network, and requires WireGuard tools, iproute2, and iptables; the installer sets them up and the daemon refuses to start without them. Trellis enables IPv4 forwarding and keeps its rules in its own iptables chains, jumped to first from `FORWARD`, `INPUT`, and the nat table's `PREROUTING`, `OUTPUT`, and `POSTROUTING`. Namespace-networked tasks reach beyond their namespace network through the node, masqueraded to its address, so they can reach whatever the node can, including its private network and cloud metadata endpoints; block destinations tasks must not reach with firewalling outside Trellis. Published task ports are forwarded to their task before your host `FORWARD` rules run, so a host firewall does not restrict who can reach them; restrict access to published ports at the network edge instead. See [Networking and ports](job-specification.md#networking-and-ports) and [Multitenancy](multitenancy.md#networking).

Workloads use Trellis's node-local DNS resolver on the reserved internal address `198.18.0.53:53`; it is not intended to be exposed externally. Node and Raft transports require mutually authenticated TLS and certificate-bound node identities. Every node identity uses `<uuid>.node.trellis`; only control-plane nodes receive the separate `trellis` API certificate (valid for 24 hours). Worker API listeners relay TCP to the control plane without terminating TLS, so bearer credentials remain encrypted end to end. Workers hold no Raft state, secrets-encryption key, or CA private key. Workers can be promoted by an administrator before reconfiguration and restart. In managed mode control-plane nodes do hold the CA key and remain fully trusted; removing or reconfiguring one does not erase a copied key. In-place demotion is unsupported; removal and re-enrollment do not secure a cluster against a compromised former control-plane node without replacing the cluster keys.

Ports and WireGuard settings needed between nodes are described in [Multi-node clusters](multi-node.md#prepare-the-network-and-configuration).

[Documentation index](../README.md) · [Previous: CLI workflows](cli.md) · [Next: Multi-node clusters](multi-node.md)
