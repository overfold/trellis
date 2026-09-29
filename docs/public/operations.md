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

Installer-managed nodes keep their durable daemon configuration at `/etc/trellis/trellis.yaml`. The file is root-readable and contains the managed-enrollment credential and operator-managed settings such as advertise addresses, labels, secret-encryption key path, and WireGuard transport settings. The first node also contains only the Ed25519 administrator public key used to initialize replicated cluster state; the private key remains operator-side and is never retained by a daemon. Volume placement is not configured here; namespace-scoped volume ownership is established by first placement and stored in the control plane.

A minimal installed node resembles:

```yaml
cluster: default
administrator_public_key: MCowBQYDK2VwAyEA...
enrollment_token: trls_enroll_...
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

`job_limits` is operator-only admission policy. Jobs cannot override it. The
defaults shown above are used when the section is omitted. Every task without a
`resources` block receives the configured CPU and memory requests before it is
stored, scheduled, and sent to containerd. Explicit zero or negative resource
values are invalid. In a [multi-node cluster](multi-node.md#prepare-the-network-and-configuration),
keep these values identical on every node.

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

`allocation_loss_timeout` (flag `--allocation-loss-timeout`) is how long a
node may go without a heartbeat before the leader marks its allocations
`lost` and replaces them. It is a Go-style duration between `30s` and `24h`
and defaults to `45s`:

```yaml
allocation_loss_timeout: 2m
```

Lost is terminal, so this is the point at which Trellis gives up on the
node's allocations. While a lost allocation record is retained, its old
containers keep running until enough replacements are running when the node
returns, and are then stopped. They are stopped sooner if they block a
replacement. If an older pruned allocation is later reported, Trellis stops
it as an observed orphan. See [lost allocations](user-model.md#lost-allocations).
Raise the timeout when nodes can be briefly unreachable, for example during
reboots or on unreliable networks, and replacing their work would cost more
than waiting. This matters most for groups bound to one node by a volume,
because their replacement can only run on that node anyway. Lower values
replace work faster after a real failure. The leader still waits 30 seconds
after it is elected before marking anything lost. The timeout applies on
whichever node is leader, so keep it the same on every node.

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

## Add a node

To grow the cluster beyond one node, see [Multi-node clusters](multi-node.md). It covers cluster sizing, networking between nodes, settings that must match, and both the managed and external-signing join workflows.

## Mint operator credentials

The installer creates one normal `cluster/write` credential for the installing user, but operators often need narrower credentials for another human, a read-only dashboard, or automation. `trellisctl credentials create` is the explicit administrative workflow for that.

Credential minting requires the **administrator private key** held by the operator. Supply a PKCS#8 PEM file, or place unpadded base64 PKCS#8 DER in `TRELLIS_ADMINISTRATOR_KEY`; Trellis nodes do not store it:

```sh
# Read-only cluster observer
trellisctl --administrator-key ./trellis-administrator.pem credentials create --scope cluster --access read

# Writer restricted to one namespace
trellisctl --administrator-key ./trellis-administrator.pem credentials create \
  --scope namespace \
  --namespace-scope staging \
  --access write
```

The default output is the newly minted bearer token so it can be handed directly to a password manager or context setup. Use `--output json` when automation needs the response object instead:

```sh
trellisctl --administrator-key ./trellis-administrator.pem credentials create --scope cluster --access read --output json
```

To save a generated credential as an ordinary user context without leaving it in command history:

```sh
TOKEN="$(trellisctl --administrator-key ./trellis-administrator.pem credentials create --scope namespace --namespace-scope staging --access write)"
trellisctl --token "$TOKEN" --namespace staging context save staging --use
unset TOKEN
```

`trellisctl` fetches the signing challenge and signs the request automatically. Enrollment credentials and ordinary `cluster/write` bearer credentials cannot mint credentials, change Raft membership, or perform backup/restore.

## Drain and maintenance

`trellisctl nodes drain NODE` prevents new placement and migrates allocations to other nodes. `NODE` may be the host/address displayed by `nodes list`, a unique UUID prefix, or a complete UUID. Wait until workloads have healthy replacements before maintenance. `trellisctl nodes undrain NODE` re-enables scheduling. `nodes remove NODE` permanently removes a node from the cluster, requires the administrator key, and is different from draining; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster) before removing a member.

## Upgrade a node

The upgrade entrypoint performs the node-maintenance sequence instead of asking the operator to remember it:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/upgrade.sh | sudo bash
```

It downloads and verifies the new release before touching the running daemon, then swaps the binaries, refreshes the installer-owned systemd unit, starts the daemon, and verifies both the service and control-plane API. If the new daemon does not become healthy, the previous binaries and unit are restored.

After a successful core upgrade, the script refreshes a dashboard that was installed and recorded by the setup lifecycle state. A service that was already stopped remains stopped. On a multi-node cluster the script also evacuates the node first; see [Multi-node clusters](multi-node.md#maintain-a-multi-node-cluster).

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

Backups contain desired jobs and their revision history, encrypted secret records, volume-registration locality metadata, and durable namespace WireGuard port assignments. They do **not** contain allocations, container images, local volume bytes, TLS private keys, or the secret encryption key. Restoring the locality metadata deliberately prevents Trellis from silently treating a previously bound volume as new; recovering a volume-backed workload therefore also requires the owning node identity and its data, or an intentional manifest change to a new volume name. Secure and separately back up the 32-byte secrets key referenced by `secrets_key` in the node config; encrypted records are unusable without it.

## Secrets

```sh
printf %s 'value' | trellisctl --namespace default secrets set db-password --stdin
trellisctl --namespace default secrets describe db-password
trellisctl --namespace default secrets delete db-password
```

Use `--expected-version N` for compare-and-swap (`0` means create only). Values are capped at 65,536 bytes. Rotation affects newly started allocations, so apply a workload revision or replace the consuming allocations afterward.

Allocation secret files are held on a verified tmpfs rather than a durable node filesystem. Linux can swap tmpfs pages, so disable swap or configure encrypted swap when secrets must also be protected from offline swap inspection. Environment delivery does not persist plaintext in containerd's OCI metadata, but the running application necessarily receives the value in its process environment; use file delivery when the application supports it. Trellis sets mounted secret ownership to the numeric UID/GID resolved from the image configuration, preserving owner-only access for non-root images.

## Observability

The control plane exposes Prometheus metrics at `/metrics`. `GET /v1/auth/whoami` reports the kind, scope, and access of the bearer credential making the request. Job status and allocation events explain lifecycle transitions; logs proxy per-task allocation logs. Monitor leader availability, unhealthy/draining nodes, desired-versus-running/healthy counts, reconciliation latency, retries, task groups in replacement backoff (`trellis_replacement_backoff_failures`), and disk capacity for Raft, containerd, and volumes.

For normal workload diagnosis, start and usually finish with `jobs status`. `ready`, `converging`, and `degraded` summarize desired-versus-observed state without collapsing allocation lifecycle and health, and non-ready status output includes the allocations that need attention with reason/message, retry timing, and attempt count. Use `jobs status NAME --history` when you need the recorded lifecycle transitions, and `jobs logs NAME` for task output.

## Networking and TLS

Workloads use Trellis's node-local DNS resolver on the reserved internal address `198.18.0.53:53`; it is not intended to be exposed on external interfaces. Node and Raft transports require mutually authenticated TLS. Possession of the CA key is not API or leader authorization: requests still need the durably bound certificate for one immutable node ID, and leader work is executed only by the current Raft leader with control-epoch, generation, revision, and execution-hash fencing where applicable. Followers preserve that certificate identity by redirecting node-authenticated control-plane requests, and Raft streams verify the peer's joined advertised address. Administrator and enrollment bearer credentials are separate from node identity; managed enrollment receives the CA key only after certificate-bound Raft admission.

Ports and WireGuard settings needed between nodes are described in [Multi-node clusters](multi-node.md#prepare-the-network-and-configuration).

[Documentation index](../README.md) · [Previous: CLI workflows](cli.md) · [Next: Multi-node clusters](multi-node.md)
