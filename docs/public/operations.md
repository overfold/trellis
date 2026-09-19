# Operations

Operational commands use the vocabulary in the [Trellis user model](user-model.md): apply/delete jobs, inspect allocations, and drain/undrain nodes. Raft and leadership controls are advanced control-plane operations rather than part of the normal workload model. The [CLI workflows guide](cli.md) covers contexts, planning, rollout watching, diagnosis, logging, and structured output in detail.

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

Installer-managed nodes keep their durable daemon configuration at `/etc/trellis/trellis.yaml`. The file is root-readable and contains the node's bootstrap credential together with operator-managed settings such as advertise addresses, labels, secret-encryption key path, and WireGuard transport settings. Volume placement is not configured here; namespace-scoped volume ownership is established by first placement and stored in the control plane.

A minimal installed node resembles:

```yaml
cluster: default
bootstrap_token: trls_boot_...
data_dir: /var/lib/trellis/data
agent_advertise: node-a:8127
server_advertise: node-a:8128
raft_advertise: node-a:8129
```

Edit this file when changing persistent node configuration, then restart the service:

```sh
sudo systemctl restart trellis
```

`trellis --config PATH` loads the same strict YAML format. Explicit daemon flags override values from the file and are useful for one-off runs; the installed systemd unit intentionally contains only `trellis --config /etc/trellis/trellis.yaml` so there is one durable configuration source.

Installer-created nodes also keep `/var/lib/trellis/install-state`. It records only lifecycle facts the installer can prove, such as which optional features are enabled and which host packages/repositories Trellis itself introduced. It is not cluster desired state and is not used by the scheduler.

## Add a node

Adding a server is explicit rather than another branch in the first-install questionnaire. The joining server needs three pieces of information from an existing member:

- an existing control-plane address such as `node-a:8128`;
- the root bootstrap credential;
- the **same secrets-encryption key used by the existing servers**.

The last requirement is important: encrypted secret records are replicated cluster state, so every server that may lead the cluster must be able to decrypt them with the same key/key ID. A joining server must not generate its own key.

On an existing node, make temporary root-readable copies for secure transfer:

```sh
sudo awk -F': ' '$1 == "bootstrap_token" { print $2; exit }' \
  /etc/trellis/trellis.yaml | \
  sudo tee /root/trellis-bootstrap-token >/dev/null
sudo chmod 600 /root/trellis-bootstrap-token
sudo install -m 600 /etc/trellis/secrets.key /root/trellis-secrets.key
```

Transfer those two files to the new machine over a secure channel, then run:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/setup.sh | \
  sudo bash -s -- \
    --join node-a:8128 \
    --bootstrap-token-file /root/trellis-bootstrap-token \
    --secrets-key-file /root/trellis-secrets.key
```

Normal installer-created clusters derive the secrets key ID from the shared key, so no additional argument is needed. If the existing cluster explicitly sets `secrets_key_id` in its node configuration, pass that same value with `--secrets-key-id ID` (or `TRELLIS_SECRETS_KEY_ID`) on the joining node.

The installer shows the complete plan before making changes; choose **Customize** to change it interactively. `--advertise HOST` overrides address auto-detection when peers cannot reach the detected private address. Namespace networking and gVisor/runsc are installed by default on fresh nodes; `--without-networking` and `--without-gvisor` are the automation opt-outs. The dashboard remains opt-in through **Customize**, `--with-dashboard`, or `--dashboard-write`. Delete the temporary transferred copies after setup succeeds.

After the daemon starts, verify membership from any operator context:

```sh
trellisctl nodes list
```

A joining node must use the bootstrap credential; minting an ordinary operator/workload token does not create or join a cluster.

## Mint operator credentials

The installer creates one normal `cluster/write` credential for the installing user, but operators often need narrower credentials for another human, a read-only dashboard, or automation. `trellisctl credentials create` is the explicit administrative workflow for that.

Credential minting requires the **bootstrap** credential. On an installed Trellis node, running the command as root automatically uses the root-readable local node connection, so the bootstrap value does not need to be copied into shell history:

```sh
# Read-only cluster observer
sudo trellisctl credentials create --scope cluster --access read

# Writer restricted to one namespace
sudo trellisctl credentials create \
  --scope namespace \
  --namespace-scope staging \
  --access write
```

The default output is the newly minted bearer token so it can be handed directly to a password manager or context setup. Use `--output json` when automation needs the response object instead:

```sh
sudo trellisctl credentials create --scope cluster --access read --output json
```

To save a generated credential as an ordinary user context without leaving it in command history:

```sh
TOKEN="$(sudo trellisctl credentials create --scope namespace --namespace-scope staging --access write)"
trellisctl --token "$TOKEN" --namespace staging context save staging --use
unset TOKEN
```

A remote bootstrap administrator may instead supply the bootstrap bearer credential through `TRELLIS_TOKEN` or `--token`, but it should be handled as a root secret. Ordinary `cluster/write` credentials cannot mint more credentials, join nodes, change Raft membership, or perform backup/restore.

## Drain and maintenance

`trellisctl nodes drain NODE` prevents new placement and migrates allocations. `NODE` may be the host/address displayed by `nodes list`, a unique UUID prefix, or a complete UUID. Wait until workloads have healthy replacements before maintenance. `trellisctl nodes undrain NODE` re-enables scheduling. `nodes remove NODE` permanently removes a node from the cluster and is different from draining.

## Upgrade a node

The upgrade entrypoint performs the node-maintenance sequence instead of asking the operator to remember it:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/upgrade.sh | sudo bash
```

It downloads and verifies the new release before touching the running daemon. In a multi-node cluster it drains the local node and waits for its local allocations to stop; Trellis only stops draining allocations after healthy replacement capacity exists. The script then swaps the binaries, refreshes the installer-owned systemd unit, starts the daemon, and verifies both the service and control-plane API. If the new daemon does not become healthy, the previous binaries and unit are restored and the node is undrained.

After a successful core upgrade, the script refreshes a dashboard that was installed and recorded by the setup lifecycle state, then undrains the node. A service that was already stopped remains stopped. Single-node clusters skip evacuation because there is nowhere to move their allocations.

## Uninstall a node

The default uninstall is a reversible machine-removal operation:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/uninstall.sh | sudo bash
```

On a live multi-node cluster it drains the node, waits for healthy replacements, transfers leadership away when necessary, and removes the local Raft member before deleting local software. It removes only dependencies/repositories recorded as introduced by Trellis; older installations without ownership records are handled conservatively and shared host packages are left alone. The user's `trellisctl` contexts are also kept because they describe cluster connections, not ownership of this machine.

Instead of throwing away the encryption key while retaining encrypted state, normal uninstall archives the complete recoverable set—node data, `/etc/trellis` configuration and the configured secrets key, plus installer state—under a timestamped `/var/lib/trellis/recovery/` directory.

For deliberate permanent destruction, use:

```sh
curl -fsSL https://raw.githubusercontent.com/clofour/trellis/main/scripts/uninstall.sh | \
  sudo bash -s -- --purge
```

`--purge` deletes active node state and any previous recovery archives. Its confirmation therefore defaults to **no**. Both modes expose `--yes` for controlled non-interactive automation.

## Advanced control-plane maintenance

Trellis uses Raft internally. If an operator deliberately needs to move control-plane leadership before maintenance, the advanced command `trellisctl nodes transfer-leadership` requests a transfer to another voter. It is intentionally hidden from normal CLI help because workload operations should not require understanding Raft leadership.

Preserve quorum: operate an odd number of Raft voters and avoid removing several members together.

## Backups

```sh
trellisctl backup create --file trellis-backup.json
trellisctl backup restore trellis-backup.json
```

Backups contain desired jobs, encrypted secret records, volume-registration locality metadata, and durable namespace WireGuard port assignments. They do **not** contain allocations, container images, local volume bytes, TLS private keys, or the secret encryption key. Restoring the locality metadata deliberately prevents Trellis from silently treating a previously bound volume as new; recovering a volume-backed workload therefore also requires the owning node identity and its data, or an intentional manifest change to a new volume name. Secure and separately back up the 32-byte secrets key referenced by `secrets_key` in the node config; encrypted records are unusable without it.

## Secrets

```sh
printf %s 'value' | trellisctl --namespace default secrets set db-password --stdin
trellisctl --namespace default secrets describe db-password
trellisctl --namespace default secrets delete db-password
```

Use `--expected-version N` for compare-and-swap (`0` means create only). Values are capped at 65,536 bytes. Rotation affects newly started allocations, so apply a workload revision or replace the consuming allocations afterward.

## Observability

The control plane exposes Prometheus metrics at `/metrics`. `GET /v1/auth/whoami` reports the kind, scope, and access of the bearer credential making the request. Job status and allocation events explain lifecycle transitions; logs proxy per-task allocation logs. Monitor leader availability, unhealthy/draining nodes, desired-versus-running/healthy counts, reconciliation latency, retries, and disk capacity for Raft, containerd, and volumes.

For normal workload diagnosis, start and usually finish with `jobs status`. `ready`, `converging`, and `degraded` summarize desired-versus-observed state without collapsing allocation lifecycle and health, and non-ready status output includes the allocations that need attention with reason/message, retry timing, and attempt count. Use `jobs status NAME --history` when you need the recorded lifecycle transitions, and `jobs logs NAME` for task output.

## Networking and TLS

Ports `8127`, `8128`, and `8129` must be reachable between appropriate cluster members. Namespace networking gives each namespace its own WireGuard interface and UDP port. Allow the configured WireGuard port range between participating nodes; by default `wireguard_port: 51820` with `wireguard_port_count: 256` uses UDP `51820-52075`. `wireguard_port_count` must match on every cluster node so a namespace slot means the same offset everywhere; the base port may differ per node. `wireguard_endpoint` is the externally reachable host or base `host:port`; Trellis applies the namespace's stable port offset to that base when building peer endpoints. Workloads use Trellis's node-local DNS resolver on the reserved internal address `198.18.0.53:53`; it is not intended to be exposed on external interfaces. Never expose the unauthenticated transport surface to an untrusted network. Configure a CA and node certificates on every node and pass the CA/client certificate flags to the CLI. Advertised addresses must be routable from peers, not wildcard bind addresses. Automatic setup chooses a non-loopback IPv4 address instead of falling back to the machine hostname; use `--advertise` when the detected address is not the one other nodes should use.

## Failure recovery

A missed-heartbeat node becomes unhealthy; allocations may become lost after leader recovery grace and an availability timeout. Reconciliation replaces missing desired capacity when placement remains valid. A namespace-scoped volume registration stays bound to its original node even while that node is absent, so Trellis leaves a dependent workload unplaced rather than creating an unrelated second copy elsewhere. If the data is intentionally abandoned, use a new volume name; changing only `host_path` does not change the owning node.

[Documentation index](../README.md) · [Previous: CLI workflows](cli.md) · [Next: Cookbook](cookbook.md)
