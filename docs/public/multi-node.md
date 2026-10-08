# Multi-node clusters

[Getting Started](getting-started.md) installs a single-node cluster, and most of the documentation can be followed on that one node. This page collects everything that changes when a cluster has more than one node: how members relate to each other, how many to run, how to add them, what the network between them needs, and how maintenance and failure behave.

Read it when you are ready to grow the first node into a cluster, or when the [learning path](learning-path.md) reaches replicas and rolling updates, which need several schedulable nodes.

## How a cluster is formed

Every machine runs the same `trellis` daemon, with two independent settings:

- `control_plane` (default `true`) determines whether it replicates state through Raft and may lead;
- `runs_workloads` (default `true`) determines whether it receives allocations;
- worker-only nodes (`control_plane: false`) register and heartbeat but hold no Raft state, secrets-encryption key, or CA private key;
- the steady-state target is up to five control-plane **voters**; other control-plane nodes are **non-voters** that can be promoted;
- one elected voter, the **leader**, serves the control-plane API, schedules, and reconciles jobs.

Trellis chooses the voters itself. A node always joins as a non-voter, and the leader promotes healthy nodes that have caught up with the replicated state until the cluster has the right number of voters. `trellisctl nodes list` shows each node's role in the **Control plane** column.

Any node accepts control-plane connections. A worker is a TCP relay: the client-to-control-plane TLS session remains encrypted end to end, so the worker cannot read bearer tokens or requests. Control-plane followers proxy to the leader. `trellisctl` also retries administrator-signed requests automatically if leadership changes mid-request.

## Choose a cluster size

A majority of voters (a **quorum**) must be reachable for Trellis to elect a leader and accept changes. Trellis targets an odd number of voters in steady state, because an even number tolerates no more failures than one fewer:

| Control-plane nodes | Voters | Voter failures tolerated |
|---|---|---|
| 1–2 | 1 | 0 |
| 3–4 | 3 | 1 |
| 5 or more | 5 | 2 |

Three control-plane nodes is the smallest cluster that survives a control-plane failure, and five survive two. Worker count does not affect quorum. Set `runs_workloads: false` to dedicate control-plane nodes, or keep its default to use their capacity.

Voters are replaced automatically when that is safe:

- when a voter is removed with `nodes remove` (or by the uninstall script), Trellis first promotes a healthy non-voter, if one exists, so the number of reachable voters never drops;
- when a voter's node has been silent for 5 minutes, Trellis demotes it, first promoting a healthy non-voter in its place when one exists. If the node returns, it stays a non-voter until a voter is needed again.

Odd voter counts are steady-state targets, not a guarantee for every intermediate configuration. Membership changes happen one at a time: growth passes through 1→2→3 or 3→4→5 voters, and replacement promotes before removing or demoting the old voter. These temporary even sets require their own majority (for example, three of four voters); a replacement becoming unavailable during promotion can interrupt progress even while the old quorum remains reachable. After a voter is removed and no healthy non-voter can take its place yet, the cluster can also briefly run with an even number of voters; the next eligible node is promoted toward the odd target.

When quorum is lost, allocations already running on reachable nodes keep running, but nothing can change: jobs cannot be applied or deleted, failed or lost allocations are not replaced, nodes cannot be drained, and no voter can be replaced. The cluster resumes once a majority of voters is reachable again.

## Prepare the network and configuration

Before joining nodes, make sure they can reach each other:

| Port | Protocol | Used for |
|---|---|---|
| `8127` | TCP | Leader-to-agent operations |
| `8128` | TCP | Control-plane API and cluster join |
| `8129` | TCP | Raft replication |
| `51820-52075` (default) | UDP | Namespace networking between nodes |

Node and Raft transports use mutually authenticated TLS. Each node's `agent_advertise`, `server_advertise`, and `raft_advertise` addresses must be routable from the other nodes; wildcard bind addresses are not valid advertised addresses. The installer auto-detects a private address and accepts `--advertise HOST` when peers cannot reach the detected one.

Daemon listeners default to all interfaces; advertised addresses do not restrict their exposure. The installer does not configure an external host firewall. Restrict the API to trusted operator/cluster networks and agent, Raft, and WireGuard ports to cluster peers. Published workload ports need separate network-edge access controls; see [Networking and TLS](operations.md#networking-and-tls).

Namespace networking gives each namespace one stable UDP port from the cluster's WireGuard range: `wireguard_port` (default `51820`) plus the cluster's `wireguard_port_count` (default `256`). Allow that range between every node. Because namespace networking is the default task network, every namespace with desired or running namespace-networked tasks holds one of these ports, so `wireguard_port_count` bounds how many such namespaces a cluster can run at once; when every port is held, new placements for another namespace wait until one is released. `wireguard_pool` (default `10.64.0.0/10`) supplies a `/24` for each namespace on each node; the default pool addresses 16384 namespace-node pairs, and when it is full new placements for another namespace wait instead of reusing an address. `wireguard_endpoint` sets the externally reachable host or base `host:port` other nodes use; Trellis applies each namespace's port offset to that base.

**Cluster settings** are replicated with the rest of the cluster state, so any node can become leader without changing them. The first node's `job_limits`, `wireguard_pool`, and `wireguard_port_count` (or the matching flags) initialize them when it creates the cluster; after that, node configuration no longer changes them, on the first node or any other. Reconciliation settings, such as the allocation loss timeout, start at their defaults and have no node configuration. Inspect them with `trellisctl cluster settings`, change job limits with `trellisctl cluster set-job-limits`, and change reconciliation settings with `trellisctl cluster set-reconciliation` ([CLI](cli.md#inspect-and-change-cluster-settings)). The pool and port count are fixed for the life of the cluster. A node started with different values behaves as follows:

- a different `job_limits` or `wireguard_pool` is ignored, and the node logs a warning at startup;
- a node that leaves `wireguard_port_count` unset uses the cluster's count; one that sets a different count refuses to start and names the cluster's value, because the leader would reject its registration.

If network slots or subnets are exhausted, free registrations by removing unneeded namespace-networked jobs and waiting for their allocations to stop. Editing the pool or port count on existing nodes cannot expand the cluster. A larger range or pool requires a new cluster; a backup restore still requires matching network settings and is not a resizing mechanism.

Lost allocations still reserve network slots and subnets: an unreachable node may still run their workloads. Removing the desired job or pruning terminal history does not free those reservations. They can be reclaimed after the node returns and acknowledges cleanup, but remain held if cleanup cannot be proven. See [network lifecycle internals](../developer/node-internals.md#networking) for the reservation and attachment rules.

Some settings must match on control-plane nodes:

- the **secrets-encryption key** (and `secrets_key_id`, if set explicitly), so every potential leader can decrypt replicated secret records;
- the node signing mode and trusted node CA. Workers need the public CA certificate, but not its private key.

## Add a node

### Managed signing (default)

Adding a node is explicit. Every joining node needs an existing address, a role-bound join token, and the public CA certificate. A control-plane node additionally needs the shared secrets-encryption key; a worker must not receive it.

- an existing control-plane address such as `node-a:8128`;
- a **join token** minted for this purpose by the administrator;
- a pinned copy of the trusted node CA certificate;
- for a control-plane join only, the **same secrets-encryption key used by the existing control-plane nodes**.

A joining node must not generate its own secrets key; see the matching settings above.

Mint a join token from an operator context that holds the administrator key. Control-plane tokens are the default, are always single-use, and expire within one hour:

```sh
trellisctl --administrator-key ./trellis-administrator.pem \
  nodes join-token create --role control-plane --ttl 30m > trellis-join-token
```

The token is printed once; the cluster stores only its hash. `trellisctl nodes join-token list` shows unexpired tokens with their use counts, and `trellisctl nodes join-token revoke ID` withdraws one before it expires. Revoking a token does not affect nodes that already enrolled with it. Each enrollment attempt that the leader accepts uses the token once, even if its response is lost before the node durably stores its identity; if a node then reports that its token is exhausted, mint another. Rerun the installer with `--join-token-file` or `TRELLIS_JOIN_TOKEN`: an incomplete managed join without a stored certificate or pending enrollment journal atomically replaces the saved token while preserving the existing node configuration, identity, pinned CA, and secrets key. An already-enrolled node or completed installation does not replace its token. After publishing the enrollment journal, interrupted certificate/key, role, or node-ID writes resume from that same identity without another token use. The journal is private-key material; do not remove it to retry installation.

For a worker, create a worker token and omit the secrets-key transfer:

```sh
trellisctl --administrator-key ./trellis-administrator.pem \
  nodes join-token create --role worker --ttl 24h --max-uses 10 > trellis-worker-token
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/install.sh | \
  sudo bash -s -- --worker --join node-a:8128 \
    --join-token-file trellis-worker-token --ca-cert-file trellis-node-ca.crt
```

The installer writes `control_plane: false` and the selected `runs_workloads` value, and neither requests nor writes a secrets key, CA key, or Raft configuration for a worker. `--control-plane true|false` and `--runs-workloads true|false` are available for automation and both default to `true`.

On an existing node, make temporary root-readable copies of the CA certificate and secrets key for secure transfer:

```sh
sudo install -m 644 /var/lib/trellis/data/node-ca.crt /root/trellis-node-ca.crt
sudo install -m 600 /etc/trellis/secrets.key /root/trellis-secrets.key
```

Transfer those files and the join token to the new machine over a secure channel, then run:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/install.sh | \
  sudo bash -s -- \
    --join node-a:8128 \
    --join-token-file /root/trellis-join-token \
    --ca-cert-file /root/trellis-node-ca.crt \
    --secrets-key-file /root/trellis-secrets.key
```

`TRELLIS_JOIN_TOKEN` may carry the token instead of `--join-token-file`. The installer removes the token from the node configuration once the node has enrolled; the node never presents it again.

Normal installer-created clusters derive the secrets key ID from the shared key, so no additional argument is needed. If the existing cluster explicitly sets `secrets_key_id` in its node configuration, pass that same value with `--secrets-key-id ID` (or `TRELLIS_SECRETS_KEY_ID`) on the joining node.

The installer shows the complete plan before making changes; choose **Customize** to change it interactively. Namespace networking, which every node requires, and gVisor/runsc are installed on joining nodes, as on the first node; `--without-gvisor` is the automation opt-out. Delete the temporary transferred copies after setup succeeds.

After the daemon starts, verify membership from any operator context:

```sh
trellisctl nodes list
```

A join token is accepted only by the managed enrollment endpoint and is never administrator API authority. Enrollment sends it only over TLS authenticated by the pinned CA, and each enrollment consumes one use in the same replicated transaction that records the new identity, so a use limit holds even when enrollments race. The node generates its private key locally and submits a signing request; the leader ignores requested names, assigns the new UUID, and returns only the certificate. Node identity certificates carry `<uuid>.node.trellis`, never the API name `trellis`. Control-plane nodes serve that name with a separate 24-hour API certificate, renewed hourly by the leader while the node remains admitted. The managed CA signing key is delivered only after a control-plane identity proves its certificate and joins Raft. Administrator requests are checked by the current leader against the replicated public key, so followers do not need or retain the administrator private key.

Join addresses may be `HOST:8128` (HTTPS implied) or `https://HOST:8128`.
Explicit HTTP, user information, paths, queries, and fragments are rejected.
Enrollment and Raft join redirects must remain HTTPS; legitimate leader
redirects still use the pinned CA. There is no plaintext enrollment fallback.

**In managed mode every control-plane node holds the cluster CA private key; workers do not.** Compromise of a control-plane node is therefore cluster compromise. To promote an enrolled worker, run `trellisctl --administrator-key ./trellis-administrator.pem nodes promote NODE`, then set `control_plane: true`, configure the matching secrets key if the cluster uses one, and restart its daemon. Changing local configuration alone cannot promote a worker.

There is no in-place demotion operation: remove and freshly enroll the machine with a worker token. A former control-plane node may retain the CA and secrets keys; removal, certificate expiry, or re-enrollment does **not** undo that exposure. If it is untrusted, replace the exposed cluster keys and reissue certificates before treating the cluster as secure. Short-lived API certificates stop normal renewal after removal, but cannot prevent a holder of the CA key from signing its own certificates.

To grow a single node into a fault-tolerant cluster, repeat this for two more machines.

### External signing

Set `node_signing_mode: external` when the operator owns the node CA. This is the mode in which no Trellis node holds the CA private key. Every node configuration must provide `ca_cert`, `cert`, and `key`; omit `ca_key` and `join_token`, since join tokens only authorize managed enrollment. Trellis verifies the key pair, trust chain, client-auth usage, and immutable node ID at startup, stores the trusted CA certificate and node key pair, and does not require or persist the CA private key.

Before first start, choose a UUID, write it to `<data_dir>/node-id` with mode `0600`, and have the external signer issue an identity certificate containing URI SAN `trellis-node:UUID` and DNS SAN `<uuid>.node.trellis`. Never issue the API name `trellis` to a worker. Control-plane API certificates are separate and short-lived. A minimal first-node configuration is:

```yaml
node_signing_mode: external
administrator_public_key: MCowBQYDK2VwAyEA...
ca_cert: /etc/trellis/node-ca.crt
cert: /etc/trellis/node.crt
key: /etc/trellis/node.key
api_cert: /etc/trellis/api.crt
api_key: /etc/trellis/api.key
```

Generate the administrator key on the operator workstation, keep the private key in a password manager, and put only its unpadded base64 PKIX public key in node configuration:

```sh
openssl genpkey -algorithm ED25519 -out trellis-administrator.pem
openssl pkey -in trellis-administrator.pem -pubout -outform DER | base64 | tr -d '=\n'
```

For another pre-issued node, omit `administrator_public_key`, add `join: node-a:8128`, and set its role. Before starting an external worker, enroll its public identity explicitly with `trellisctl --administrator-key ./trellis-administrator.pem nodes enroll --cert node.crt --role worker`; set `control_plane: false` and omit `api_cert` and `api_key`. Control-plane nodes require the separate API key pair and its renewal is the external operator's responsibility. Only administrator-approved control-plane identities may join Raft. Loss of the external signer prevents issuing certificates for new nodes and renewing API certificates. A certificate from any other CA, one whose node ID differs from `<data_dir>/node-id`, or a worker identity containing `trellis` is rejected.

## Try it locally with Vagrant

The repository includes [`orchestrator/Vagrantfile`](../../orchestrator/Vagrantfile) for a disposable three-node lab. It provisions three Debian 12 VMs named `control`, `worker-1`, and `worker-2`, installs containerd and Trellis on them, joins them into one cluster, and deploys a couple of demo workloads.

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

This lab is not a supported production installation method. Any three compatible machines work equally well; three nodes is enough to run every lesson in the learning path.

## What changes for workloads

Workload semantics are the same on one node or many, but some constraints only become visible with several nodes. Each is documented with its feature:

- **Node ports**, whether published from namespace networking or reserved by host networking, can be used once per node, so replicas using the same node port need distinct nodes, and rolling updates need a spare node while old and new allocations overlap. See [networking and ports](job-specification.md#networking-and-ports).
- **Named volumes** are bound to the node where they were first placed. Later allocations return to that node, and Trellis does not create a second copy elsewhere if it is lost. See [volumes](job-specification.md#volumes).
- **Namespace networking** connects tasks across nodes; every node runs it and needs the WireGuard UDP range open as described above. See [namespace networking](learning-path.md#8-namespace-networking-and-discovery).
- **Placement** considers each node's labels, capabilities, capacity, and volume registrations. `trellisctl nodes list` and `nodes status NODE` show what the scheduler sees. See [scheduling](core-concepts.md#scheduling).

## Maintain a multi-node cluster

`trellisctl nodes drain NODE` moves allocations to other nodes before maintenance; see [Drain and maintenance](operations.md#drain-and-maintenance) for the node commands.

The [upgrade script](operations.md#upgrade-a-node) is safe to run one node at a time on a live cluster. It drains the local node and waits for its allocations to stop—Trellis only stops draining allocations after healthy replacement capacity exists elsewhere—then upgrades, verifies, and undrains the node. Upgrade nodes one after another, not in parallel, so the cluster keeps quorum and replacement capacity.

The [uninstall script](operations.md#uninstall-a-node) on a live multi-node cluster drains the node, waits for healthy replacements, transfers leadership away when necessary, and removes the local Raft member before deleting local software.

Both scripts wait up to five minutes for local containerd tasks to disappear. If evacuation times out, they abort before replacing binaries or deleting data and attempt to undrain the node. Check its actual drain state afterward if the cluster is unavailable. Volume locality, insufficient capacity, node-port conflicts, or unhealthy replacements can block evacuation; resolve those conditions before retrying.

To remove a node that can no longer be uninstalled cleanly—for example, a machine that has permanently failed or may be compromised—run `trellisctl --administrator-key ./trellis-administrator.pem nodes remove NODE` from any operator context. It removes the node's Raft membership, promoting a healthy non-voter first when the node was a voter. Trellis refuses a removal that would leave the remaining voters without a reachable majority; bring back or remove the unreachable voters first. It also refuses to remove the current leader; run `nodes transfer-leadership` first. Remove nodes one at a time.

Removal is permanent and revokes the node's identity. Before changing Raft membership, Trellis records a replicated tombstone for the node's UUID; from then on its certificate is rejected by the control-plane API, the agent API, the Raft transport, and Raft join, even if the machine still has its data directory and restarts. A removed machine returns to the cluster only as a new node: wipe its data directory (the uninstall script archives it) and add it again with a new join token. In managed mode a removed node may still hold a copy of the CA private key; removal stops it from acting as its old identity or joining as a member, but if the machine is untrusted, treat the key as exposed.

### Recover an interrupted removal

The quorum preflight checks the final configuration using recent heartbeats; it cannot guarantee availability throughout promotion. For example, with A/B/C voters, C offline, and caught-up non-voter D, removing B first promotes D. Promotion needs three of A/B/C/D. If B or D stops before acknowledging that entry, leadership can be lost; losing D can interrupt progress even though A/B still form the original quorum. Once promotion commits, B's removal needs two of A/C/D, **not** three of the intermediate four voters. This is an availability window, not evidence of corrupted state.

Prefer removal while all remaining voters and the replacement are reachable. Keep those nodes running until removal succeeds; do not combine removal with another node's maintenance. If removal reports that the node is **tombstoned but removal is incomplete**, or the response is lost:

1. Treat the target identity as permanently revoked; a failed request is not a rollback. Stop the target daemon and retain its data for inspection. Do not delete tombstones, edit Raft files, force a new bootstrap, or wipe healthy replicas to bypass quorum.
2. Restore network/process availability of non-revoked voters and the replacement. In the example, restore C and D so A/C/D can elect and commit without relying on B. A displayed voter role may reflect an uncommitted configuration and is not proof that promotion finished.
3. Once the API is writable, retry `nodes remove` for the **same UUID**. Retries do not readmit it. Verify `nodes list` has no control-plane role for that UUID and the remaining voter set converges to the odd target before further maintenance or local uninstall. If the API is still unavailable, repeated removal requests cannot substitute for quorum recovery.

Revocation is checked against each receiver's locally applied state on authentication; it does not erase copied keys or retroactively close already-authenticated streams. Do not rely on a tombstone as an immediate process kill or instantaneous cluster-wide disconnection.

To move control-plane leadership deliberately before maintenance, the advanced command `trellisctl --administrator-key ./trellis-administrator.pem nodes transfer-leadership` requests a transfer to another voter; non-voters never receive leadership. It is hidden from normal CLI help because workload operations should not require understanding Raft leadership.

## Node failure

A node that misses heartbeats for 30 seconds becomes unhealthy and receives no new allocations. Once it has been silent for the allocation loss timeout (the `allocation_loss_timeout` cluster setting, default 45 seconds), and the current leader has been leader for at least 30 seconds, its allocations become lost. A newly elected leader counts that silence from the start of its leadership. Reconciliation then replaces the missing capacity on other nodes when placement remains valid.

A lost allocation is not re-adopted. While its record is retained, if its node returns with the old containers still running, Trellis keeps them running until enough replacements are `running`, then stops them. If an older pruned allocation is later reported, Trellis stops it as an observed orphan. It stops them sooner if they block a replacement, such as one that needs the same host port on that node. Allocations that depend on a volume bound to the failed node stay unplaced rather than starting with an empty copy elsewhere. If that data is intentionally abandoned, use a new volume name; changing only `host_path` does not change the owning node.

If a failure takes the cluster below quorum, see [Choose a cluster size](#choose-a-cluster-size): restore enough voters to regain a majority before expecting any of this reconciliation to happen. A voter that stays down for 5 minutes is demoted, and replaced by a healthy non-voter when one exists, as long as the remaining voters still hold quorum.

[Documentation index](../README.md) · [Previous: Operations](operations.md) · [Next: Cookbook](cookbook.md)
