# Patroni architecture skeleton

**Level:** Advanced architecture · **Prerequisites:** complete the earlier learning path, prepare at least three failure-separated nodes, and design an external Patroni DCS and backup system

This directory demonstrates how Trellis can place and monitor three Patroni/PostgreSQL containers. It is intentionally **not a turnkey HA database**. Trellis supplies container scheduling, health observations, namespace networking, secret delivery, and local-volume placement; Patroni and a supported distributed configuration store (DCS) must supply database membership, leader election, replication, and promotion safety.

## What the manifest provides

- Three independently named, single-allocation PostgreSQL task groups.
- Explicit `patroni-member=1`, `2`, and `3` constraints so the three database members are tied to three deliberately prepared failure domains.
- A distinct namespace-scoped volume identity for every PostgreSQL member (`postgres-data-1`, `postgres-data-2`, and `postgres-data-3`).
- Task-level namespace WireGuard networking with the `runsc` runtime for additional syscall-level sandboxing.
- PostgreSQL and Patroni REST listeners inside each namespace-networked task, plus a script `/health` probe.
- Namespace-scoped Trellis API access for optional endpoint discovery.
- Environment-delivered superuser and replication credentials.

The separate task groups and volume names are deliberate. A Trellis volume name is a namespace-scoped locality identity: every allocation using the same `(namespace, name)` is scheduled to the node that owns that registration. Reusing one volume name for all three Patroni members would therefore colocate them instead of giving them independent local disks.

## Required work before applying

### 1. Prepare three nodes

Label one intended database node for each member:

```sh
# Node 1
sudo trellis --bootstrap-token "$TRELLIS_TOKEN" --label patroni-member=1

# Node 2
sudo trellis --bootstrap-token "$TRELLIS_TOKEN" --label patroni-member=2

# Node 3
sudo trellis --bootstrap-token "$TRELLIS_TOKEN" --label patroni-member=3
```

The manifest uses `@/patroni/member-N`, so each node creates its PostgreSQL directory below Trellis's volume root for the `database` namespace when that member is first realized. Each logical volume then remains registered to that node. Back up the three data directories independently; the matching `@/` paths do not imply replication between nodes.

If you instead use explicit absolute `host_path` values, prepare those directories yourself and keep the member constraints. Absolute host paths are used verbatim and do not gain filesystem-level namespace isolation from Trellis.

### 2. Supply a real DCS and Patroni configuration

The manifest gives each member a distinct static `PATRONI_NAME`, but it still omits the real DCS configuration required by Patroni. Configure Patroni for etcd, Consul, or another Patroni-supported DCS with quorum and TLS appropriate to your environment.

`discover-members.sh` queries Trellis allocations labeled `service:patroni`; it can help a controller find endpoints, but the Trellis catalog is eventually reconciled service discovery—not Patroni's consensus DCS. Never use the catalog alone to decide which PostgreSQL member may accept writes.

### 3. Create credentials

```sh
openssl rand -base64 32 | \
  trellisctl --namespace database secrets set postgres-password --stdin
openssl rand -base64 32 | \
  trellisctl --namespace database secrets set replication-password --stdin
```

Confirm the selected Patroni image actually consumes `PGPASSWORD_SUPERUSER` and `PGPASSWORD_STANDBY`, or adapt the environment to that image's documented configuration contract. Pin the image by digest after qualification.

### 4. Design networking and client routing

Open the configured WireGuard UDP range between nodes and ensure advertised endpoints are routable; each Trellis namespace uses one stable port from that range. PostgreSQL clients should not pick an arbitrary catalog member for writes. Route through a Patroni-aware proxy or controller that checks the leader/read-only REST endpoints and distinguishes primary from replica traffic.

## Apply and inspect

Only after replacing the placeholders and configuring the DCS:

```sh
trellisctl jobs apply --file examples/patroni/trellis.yaml
trellisctl --namespace database jobs status patroni
trellisctl nodes list
```

Check that all three allocations are on their intended nodes, Patroni reports one leader, replicas stream successfully, and write/read routing follows the desired roles. Inspect allocation events when a health check fails; Trellis health alone does not prove replication is current or promotion is safe.

## Failure and upgrade tests

Before storing production data, demonstrate all of the following in a disposable environment:

1. loss and return of a replica;
2. loss of the PostgreSQL leader and exactly-one safe promotion;
3. loss of DCS quorum without split-brain writes;
4. node loss without Trellis silently recreating that member's registered volume on another node;
5. WAL archiving, base backup, point-in-time restore, and credential recovery;
6. `pg_rewind` or reinitialization of the former primary;
7. PostgreSQL/Patroni upgrades with version-skew compatibility;
8. restoration when Trellis desired state and database data are recovered separately.

Trellis backups contain the job, encrypted secret records, and volume-registration locality metadata, but not PostgreSQL volume data or the separate secrets encryption key. Database backup and fencing remain application/operator responsibilities.

[Examples index](../README.md) · [Learning path](../../docs/public/learning-path.md)
