# Multi-node clusters

[Getting Started](getting-started.md) installs a single-node cluster, and most of the documentation can be followed on that one node. This page collects everything that changes when a cluster has more than one node: how members relate to each other, how many to run, how to add them, what the network between them needs, and how maintenance and failure behave.

Read it when you are ready to grow the first node into a cluster, or when the [learning path](learning-path.md) reaches replicas and rolling updates, which need several schedulable nodes.

## How a cluster is formed

Every machine runs the same `trellis` daemon. There are no separate server and worker roles:

- every node runs allocations;
- every node replicates desired state through Raft; up to five of them are **voters** that take part in leader election and must acknowledge changes, and the others are **non-voters** that can be promoted when needed;
- one elected voter, the **leader**, serves the control-plane API, schedules, and reconciles jobs.

Trellis chooses the voters itself. A node always joins as a non-voter, and the leader promotes healthy nodes that have caught up with the replicated state until the cluster has the right number of voters. `trellisctl nodes list` shows each node's role in the **Control plane** column.

Any node accepts control-plane requests. Followers proxy ordinary operator and administrator requests to the current leader, so `trellisctl` contexts, the dashboard, and in-cluster `TRELLIS_ADDR` clients can point at any reachable node and do not need reconfiguring when leadership moves. Certificate-authenticated node requests are redirected instead, preserving the caller's node certificate end to end. `trellisctl` also retries administrator-signed requests automatically if leadership changes mid-request.

## Choose a cluster size

A majority of voters (a **quorum**) must be reachable for Trellis to elect a leader and accept changes. Trellis keeps an odd number of voters, because an even number tolerates no more failures than one fewer:

| Nodes | Voters | Voter failures tolerated |
|---|---|---|
| 1–2 | 1 | 0 |
| 3–4 | 3 | 1 |
| 5 or more | 5 | 2 |

Three nodes is the smallest cluster that survives a node failure, and five survive two. Beyond five, extra nodes add workload capacity without enlarging the quorum, so the node count can follow workload needs rather than consensus arithmetic. A second node adds capacity and a standby copy of the state, but the control plane still depends on the first node until a third joins.

Voters are replaced automatically when that is safe:

- when a voter is removed with `nodes remove` (or by the uninstall script), Trellis first promotes a healthy non-voter, if one exists, so the number of reachable voters never drops;
- when a voter's node has been silent for 5 minutes and a healthy non-voter exists, Trellis promotes the non-voter and demotes the silent voter. If the node returns, it stays a non-voter until a voter is needed again.

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

Namespace networking gives each namespace one stable UDP port from the configured WireGuard range: `wireguard_port` (default `51820`) plus `wireguard_port_count` (default `256`). Allow that range between every node that may run namespace-networked tasks. `wireguard_endpoint` sets the externally reachable host or base `host:port` other nodes use; Trellis applies each namespace's port offset to that base.

Some settings must match on every node, because any node may become leader or take part in the same namespace network:

- the **secrets-encryption key** (and `secrets_key_id`, if set explicitly), so every potential leader can decrypt replicated secret records;
- `job_limits`, so admission policy does not change with leadership;
- `allocation_loss_timeout`, so how long a silent node is tolerated does not change with leadership;
- `wireguard_port_count`, so a namespace's port offset means the same thing everywhere (the base `wireguard_port` may differ per node);
- the node signing mode and trusted node CA.

## Add a node

### Managed signing (default)

Adding a node is explicit rather than another branch in the first-install questionnaire. The joining node needs four pieces of information from an existing member:

- an existing control-plane address such as `node-a:8128`;
- the dedicated node-enrollment credential;
- a pinned copy of the trusted node CA certificate;
- the **same secrets-encryption key used by the existing nodes**.

A joining node must not generate its own secrets key; see the matching settings above.

On an existing node, make temporary root-readable copies for secure transfer:

```sh
sudo awk -F': ' '$1 == "enrollment_token" { print $2; exit }' \
  /etc/trellis/trellis.yaml | \
  sudo tee /root/trellis-enrollment-token >/dev/null
sudo chmod 600 /root/trellis-enrollment-token
sudo install -m 644 /var/lib/trellis/data/node-ca.crt /root/trellis-node-ca.crt
sudo install -m 600 /etc/trellis/secrets.key /root/trellis-secrets.key
```

Transfer those files to the new machine over a secure channel, then run:

```sh
curl -fsSL https://raw.githubusercontent.com/overfold/trellis/main/scripts/setup.sh | \
  sudo bash -s -- \
    --join node-a:8128 \
    --enrollment-token-file /root/trellis-enrollment-token \
    --ca-cert-file /root/trellis-node-ca.crt \
    --secrets-key-file /root/trellis-secrets.key
```

Normal installer-created clusters derive the secrets key ID from the shared key, so no additional argument is needed. If the existing cluster explicitly sets `secrets_key_id` in its node configuration, pass that same value with `--secrets-key-id ID` (or `TRELLIS_SECRETS_KEY_ID`) on the joining node.

The installer shows the complete plan before making changes; choose **Customize** to change it interactively. Namespace networking and gVisor/runsc are installed by default on joining nodes, as on the first node; `--without-networking` and `--without-gvisor` are the automation opt-outs. The dashboard remains opt-in through **Customize**, `--with-dashboard`, or `--dashboard-write`. Delete the temporary transferred copies after setup succeeds.

After the daemon starts, verify membership from any operator context:

```sh
trellisctl nodes list
```

The enrollment credential is accepted only by the managed enrollment endpoint and is never administrator API authority. Enrollment sends it only over TLS authenticated by the pinned CA. The leader assigns the new UUID rather than accepting a caller-selected identity and initially returns only that node's certificate and private key. The managed CA signing key is delivered only after the node proves that certificate and is admitted under the assigned UUID as a Raft member. This keeps managed signing available after failover without allowing the enrollment credential alone to duplicate an existing node identity. After enrollment, node registration, heartbeats, Raft joins, and node-to-agent traffic use the node's unique certificate-bound UUID instead of a shared bearer token. Administrator requests are checked by the current leader against the replicated public key, so followers do not need or retain the administrator private key. Managed mode deliberately trusts every admitted Trellis node and makes the CA signing key available to every leader-capable member so failover does not disable enrollment. Treat compromise of any admitted node in managed mode as compromise of the cluster.

To grow a single node into a fault-tolerant cluster, repeat this for two more machines.

### External signing

Set `node_signing_mode: external` when the operator owns the node CA. Every node configuration must provide `ca_cert`, `cert`, and `key`; omit `ca_key` and `enrollment_token`. Trellis verifies the key pair, trust chain, client-auth usage, and immutable node ID at startup, stores the trusted CA certificate and node key pair, and does not require or persist the CA private key.

Before first start, choose a UUID, write it to `<data_dir>/node-id` with mode `0600`, and have the external signer issue a certificate containing that UUID as URI SAN `trellis-node:UUID`. The certificate must allow TLS client and server authentication and include `trellis` plus the node's agent, control-plane, and Raft advertised DNS names or IP addresses as SANs. A minimal first-node configuration is:

```yaml
node_signing_mode: external
administrator_public_key: MCowBQYDK2VwAyEA...
ca_cert: /etc/trellis/node-ca.crt
cert: /etc/trellis/node.crt
key: /etc/trellis/node.key
```

Generate the administrator key on the operator workstation, keep the private key in a password manager, and put only its unpadded base64 PKIX public key in node configuration:

```sh
openssl genpkey -algorithm ED25519 -out trellis-administrator.pem
openssl pkey -in trellis-administrator.pem -pubout -outform DER | base64 | tr -d '=\n'
```

For another pre-issued node, omit `administrator_public_key` and add `join: node-a:8128`; its authenticated node certificate authorizes only that certificate's UUID as the Raft member ID. Its advertised control-plane and Raft hosts must match certificate SANs. Loss of the external signer prevents issuing certificates for new nodes but does not affect operation or leader failover among nodes that already have certificates. A certificate from any other CA, or one whose node ID differs from `<data_dir>/node-id`, is rejected.

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

- **Fixed host ports** can be used once per node, so replicas reserving the same port need distinct nodes, and rolling updates need a spare node while old and new allocations overlap. See [host networking](job-specification.md#networking-and-ports).
- **Named volumes** are bound to the node where they were first placed. Later allocations return to that node, and Trellis does not create a second copy elsewhere if it is lost. See [volumes](job-specification.md#volumes).
- **Namespace networking** connects tasks across nodes; each participating node needs the WireGuard setup described above. See [namespace networking](learning-path.md#8-namespace-networking-and-discovery).
- **Placement** considers each node's labels, capabilities, capacity, and volume registrations. `trellisctl nodes list` and `nodes status NODE` show what the scheduler sees. See [scheduling](core-concepts.md#scheduling).

## Maintain a multi-node cluster

`trellisctl nodes drain NODE` moves allocations to other nodes before maintenance; see [Drain and maintenance](operations.md#drain-and-maintenance) for the node commands.

The [upgrade script](operations.md#upgrade-a-node) is safe to run one node at a time on a live cluster. It drains the local node and waits for its allocations to stop—Trellis only stops draining allocations after healthy replacement capacity exists elsewhere—then upgrades, verifies, and undrains the node. Upgrade nodes one after another, not in parallel, so the cluster keeps quorum and replacement capacity.

The [uninstall script](operations.md#uninstall-a-node) on a live multi-node cluster drains the node, waits for healthy replacements, transfers leadership away when necessary, and removes the local Raft member before deleting local software.

To remove a node that can no longer be uninstalled cleanly—for example, a machine that has permanently failed—run `trellisctl --administrator-key ./trellis-administrator.pem nodes remove NODE` from any operator context. It removes the node's Raft membership, promoting a healthy non-voter first when the node was a voter. Trellis refuses a removal that would leave the remaining voters without a reachable majority; bring back or remove the unreachable voters first. Remove nodes one at a time.

To move control-plane leadership deliberately before maintenance, the advanced command `trellisctl --administrator-key ./trellis-administrator.pem nodes transfer-leadership` requests a transfer to another voter; non-voters never receive leadership. It is hidden from normal CLI help because workload operations should not require understanding Raft leadership.

## Node failure

A node that misses heartbeats for 30 seconds becomes unhealthy and receives no new allocations. Once it has been silent for the allocation loss timeout (`allocation_loss_timeout`, default 45 seconds), and the current leader has been leader for at least 30 seconds, its allocations become lost. Reconciliation then replaces the missing capacity on other nodes when placement remains valid.

A lost allocation is not re-adopted. While its record is retained, if its node returns with the old containers still running, Trellis keeps them running until enough replacements are `running`, then stops them. If an older pruned allocation is later reported, Trellis stops it as an observed orphan. It stops them sooner if they block a replacement, such as one that needs the same host port on that node. Allocations that depend on a volume bound to the failed node stay unplaced rather than starting with an empty copy elsewhere. If that data is intentionally abandoned, use a new volume name; changing only `host_path` does not change the owning node.

If a failure takes the cluster below quorum, see [Choose a cluster size](#choose-a-cluster-size): restore enough voters to regain a majority before expecting any of this reconciliation to happen. A voter that stays down is replaced by a healthy non-voter after 5 minutes when one exists, as long as the remaining voters still hold quorum.

[Documentation index](../README.md) · [Previous: Operations](operations.md) · [Next: Cookbook](cookbook.md)
